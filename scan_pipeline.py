#!/usr/bin/env python3
"""
Scan Pipeline für Paperless-ngx (mit OCR)
----------------------------------------
Überwacht einen Ordner auf neue PDFs, führt OCR durch (ocrmypdf),
löscht leere Seiten (Text-basiert nach OCR), trennt jede Seite in ein
einzelnes PDF und schickt die verarbeiteten Dateien an Dropbox und
Paperless-ngx via SFTP (Paramiko).

Installation:
    pip3 install watchdog pypdf paramiko
System:
    ocrmypdf + tesseract müssen installiert sein (brew install ocrmypdf tesseract)

Konfiguration: siehe KONFIGURATION unten
"""

import os
import time
import shutil
import logging
import subprocess
import threading
from pathlib import Path
from datetime import datetime

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pypdf import PdfReader, PdfWriter
import paramiko

# ─────────────────────────────────────────────
# KONFIGURATION
# ─────────────────────────────────────────────

WATCH_FOLDER        = "/Users/family/Documents"           # Eingangsordner (Scanner)
DROPBOX_FOLDER      = "/Users/family/Dropbox/Ablage"      # Dropbox Backup
TEMP_FOLDER         = "/Users/family/Documents/.tmp_scan" # Temporärer Arbeitsordner

LINUX_HOST          = "100.83.215.34"
LINUX_USER          = "tom"
LINUX_CONSUME_PATH  = "/home/tom/docker/paperless/consume"
SSH_KEY_PATH        = os.path.expanduser("~/.ssh/id_ed25519")

# Seite gilt als leer, wenn weniger als X Zeichen Text vorhanden (nach OCR)
EMPTY_PAGE_THRESHOLD = 10

# Warteschlangen-Einstellungen
QUEUE_CHECK_INTERVAL = 5       # Sekunden zwischen jedem Queue-Check
FILE_STABLE_SECONDS  = 10      # Datei muss X Sekunden stabil (gleiche Grösse) sein
FILE_ZERO_TIMEOUT    = 60      # Nach X Sekunden mit 0kb → überspringen

# OCR Einstellungen
# Falls PATH-Probleme: setze hier z.B. OCRMY_PDF_BIN = "/opt/homebrew/bin/ocrmypdf"
OCRMY_PDF_BIN = "ocrmypdf"

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/Users/family/Documents/scan_pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# WARTESCHLANGE
# ─────────────────────────────────────────────

# { filepath: { "detected_at": float, "last_size": int, "stable_since": float|None } }
_queue: dict = {}
_queue_lock = threading.Lock()


def queue_worker():
    """
    Läuft als Hintergrund-Thread.
    Prüft alle QUEUE_CHECK_INTERVAL Sekunden jede Datei in der Warteschlange:
      - 0kb seit > FILE_ZERO_TIMEOUT  → überspringen (Scanner-Fehler)
      - Grösse stabil seit FILE_STABLE_SECONDS → verarbeiten
    """
    while True:
        time.sleep(QUEUE_CHECK_INTERVAL)

        with _queue_lock:
            to_process = []
            to_remove = []

            for path, state in _queue.items():
                # Datei existiert noch?
                if not os.path.exists(path):
                    log.warning(f"  Queue: Datei verschwunden – entfernt: {path}")
                    to_remove.append(path)
                    continue

                size = os.path.getsize(path)
                now = time.time()

                # Noch 0kb?
                if size == 0:
                    elapsed = now - state["detected_at"]
                    if elapsed > FILE_ZERO_TIMEOUT:
                        log.warning(f"  Queue: Timeout (immer noch 0kb) – übersprungen: {path}")
                        to_remove.append(path)
                    else:
                        log.info(f"  Queue: Warte auf Inhalt ({elapsed:.0f}s) – {Path(path).name}")
                    continue

                # Grösse hat sich geändert → stable_since zurücksetzen
                if size != state["last_size"]:
                    state["last_size"] = size
                    state["stable_since"] = now
                    log.info(f"  Queue: Grösse ändert sich ({size} bytes) – {Path(path).name}")
                    continue

                # Grösse gleich aber stable_since noch nicht gesetzt
                if state["stable_since"] is None:
                    state["stable_since"] = now
                    continue

                # Stabil seit X Sekunden?
                stable_duration = now - state["stable_since"]
                if stable_duration >= FILE_STABLE_SECONDS:
                    log.info(f"  Queue: Datei stabil ({size} bytes, {stable_duration:.0f}s) → verarbeiten: {Path(path).name}")
                    to_process.append(path)
                    to_remove.append(path)
                else:
                    log.info(f"  Queue: Fast stabil ({stable_duration:.0f}/{FILE_STABLE_SECONDS}s) – {Path(path).name}")

            for path in to_remove:
                del _queue[path]

        # Verarbeitung ausserhalb des Locks (damit Queue nicht blockiert)
        for path in to_process:
            process_pdf(path)


def add_to_queue(path: str):
    """Fügt eine neue Datei der Warteschlange hinzu."""
    with _queue_lock:
        if path in _queue:
            return  # bereits in der Queue
        _queue[path] = {
            "detected_at": time.time(),
            "last_size": -1,
            "stable_since": None,
        }
    log.info(f"  Queue: Datei erkannt, warte auf Stabilität – {Path(path).name}")


# ─────────────────────────────────────────────
# OCR
# ─────────────────────────────────────────────

def ocr_pdf(input_path: str) -> str:
    """
    Führt OCR auf input_path aus und gibt Pfad zur OCR-Version zurück.
    Output liegt im TEMP_FOLDER.
    """
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    base_name = Path(input_path).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(TEMP_FOLDER, f"{timestamp}_{base_name}_ocr.pdf")

    cmd = [
        OCRMY_PDF_BIN,
        "--skip-text",          # vorhandenen Text nicht erneut ocr'en
        "--deskew",             # Schieflage korrigieren
        "--rotate-pages",       # Seiten automatisch drehen
        "--language", "deu+eng",
        "--output-type", "pdf",
        input_path,
        output_path,
    ]

    log.info("  OCR läuft…")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log.error("  ✗ OCR fehlgeschlagen")
        if result.stdout:
            log.error(f"  OCR stdout: {result.stdout.strip()}")
        if result.stderr:
            log.error(f"  OCR stderr: {result.stderr.strip()}")
        raise RuntimeError("OCR failed")

    log.info(f"  ✓ OCR-PDF erstellt: {output_path}")
    return output_path

# ─────────────────────────────────────────────
# PDF VERARBEITUNG
# ─────────────────────────────────────────────

def is_empty_page(page) -> bool:
    """Gibt True zurück wenn die Seite als leer gilt (Text-basiert)."""
    try:
        text = page.extract_text() or ""
        return len(text.strip()) < EMPTY_PAGE_THRESHOLD
    except Exception:
        return False


def split_pdf(input_path: str) -> list[str]:
    """
    Liest ein PDF, überspringt leere Seiten und speichert
    jede verbleibende Seite als einzelnes PDF im TEMP_FOLDER.
    Gibt Liste der erzeugten Dateipfade zurück.
    """
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    reader = PdfReader(input_path)
    base_name = Path(input_path).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_files: list[str] = []
    page_counter = 1

    for i, page in enumerate(reader.pages):
        if is_empty_page(page):
            log.info(f"  Seite {i+1} ist leer – wird übersprungen")
            continue

        writer = PdfWriter()
        writer.add_page(page)

        filename = f"{timestamp}_{base_name}_s{page_counter:03d}.pdf"
        output_path = os.path.join(TEMP_FOLDER, filename)

        with open(output_path, "wb") as f:
            writer.write(f)

        log.info(f"  Seite {i+1} gespeichert als: {filename}")
        output_files.append(output_path)
        page_counter += 1

    return output_files

# ─────────────────────────────────────────────
# DATEIEN VERSENDEN
# ─────────────────────────────────────────────

def copy_to_dropbox(file_path: str):
    """Kopiert eine Datei in den Dropbox-Ordner."""
    os.makedirs(DROPBOX_FOLDER, exist_ok=True)
    dest = os.path.join(DROPBOX_FOLDER, Path(file_path).name)
    shutil.copy2(file_path, dest)
    log.info(f"  → Dropbox: {dest}")


def copy_to_paperless(file_path: str):
    """Kopiert eine Datei via SFTP auf den Linux-Server."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=LINUX_HOST,
        username=LINUX_USER,
        key_filename=SSH_KEY_PATH
    )
    with ssh.open_sftp() as sftp:
        remote_path = f"{LINUX_CONSUME_PATH}/{Path(file_path).name}"
        sftp.put(file_path, remote_path)
        log.info(f"  → Paperless (Linux): {remote_path}")
    ssh.close()

# ─────────────────────────────────────────────
# HAUPTVERARBEITUNG
# ─────────────────────────────────────────────

def process_pdf(file_path: str):
    """Verarbeitet ein einzelnes PDF komplett."""
    log.info(f"Verarbeite: {file_path}")

    ocr_path = None
    output_files: list[str] = []

    try:
        # 1) OCR
        ocr_path = ocr_pdf(file_path)

        # 2) PDF splitten und leere Seiten entfernen
        output_files = split_pdf(ocr_path)

        if not output_files:
            log.warning("  Keine Seiten übrig nach Filterung – Original wird behalten")
            return

        log.info(f"  {len(output_files)} Seite(n) nach Verarbeitung")

        # 3) Jede Seite an beide Endpoints schicken
        # Erst alle Transfers prüfen bevor wir löschen
        dropbox_ok = True
        paperless_ok = True

        for f in output_files:
            try:
                copy_to_dropbox(f)
            except Exception as e:
                log.error(f"  ✗ Dropbox Fehler: {e}")
                dropbox_ok = False

            try:
                copy_to_paperless(f)
            except Exception as e:
                log.error(f"  ✗ SFTP Fehler: {e}")
                paperless_ok = False

        # 4) Nur löschen wenn beide Endpoints erfolgreich waren
        if dropbox_ok and paperless_ok:
            for f in output_files:
                try:
                    os.remove(f)
                except Exception:
                    pass
            if ocr_path:
                try:
                    os.remove(ocr_path)
                except Exception:
                    pass
            os.remove(file_path)
            log.info(f"  ✓ Verarbeitung abgeschlossen, Original gelöscht: {file_path}")
        else:
            log.warning("  ⚠ Nicht alle Endpoints erfolgreich – Original wird NICHT gelöscht")

    except Exception as e:
        log.error(f"  ✗ Fehler bei Verarbeitung: {e}")

# ─────────────────────────────────────────────
# FOLDER WATCHER
# ─────────────────────────────────────────────

class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        if not event.src_path.lower().endswith(".pdf"):
            return

        filename = Path(event.src_path).name

        # Versteckte/temporäre Dateien ignorieren
        if filename.startswith(".") or filename.startswith("~"):
            return

        # Temp-Ordner ignorieren
        if TEMP_FOLDER in event.src_path:
            return

        # Nicht direkt verarbeiten – in Warteschlange legen
        add_to_queue(event.src_path)

# ─────────────────────────────────────────────
# START
# ─────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("Scan Pipeline gestartet (mit OCR)")
    log.info(f"Überwache:    {WATCH_FOLDER}")
    log.info(f"Dropbox:      {DROPBOX_FOLDER}")
    log.info(f"Paperless:    {LINUX_USER}@{LINUX_HOST}:{LINUX_CONSUME_PATH}")
    log.info(f"OCR Bin:      {OCRMY_PDF_BIN}")
    log.info(f"Queue-Check:  alle {QUEUE_CHECK_INTERVAL}s")
    log.info(f"Stabil nach:  {FILE_STABLE_SECONDS}s")
    log.info("=" * 50)

    # Hintergrund-Thread für Warteschlange starten
    worker = threading.Thread(target=queue_worker, daemon=True)
    worker.start()

    # Folder Watcher starten
    observer = Observer()
    observer.schedule(PDFHandler(), WATCH_FOLDER, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
        log.info("Pipeline gestoppt.")

    observer.join()
