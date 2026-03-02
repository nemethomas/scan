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

# Seite gilt als leer, wenn weniger als X Zeichen Text vorhanden
# (nach OCR)
EMPTY_PAGE_THRESHOLD = 10

# Warte-Logik: Scanner schreibt Datei ggf. noch
FILE_STABLE_SECONDS = 1.0     # wie lange muss die Größe stabil bleiben
FILE_WAIT_TIMEOUT   = 60.0    # max. warten (Sek.)

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
# HILFSFUNKTIONEN
# ─────────────────────────────────────────────

def wait_until_file_is_stable(path: str) -> bool:
    """
    Wartet, bis sich die Dateigröße für FILE_STABLE_SECONDS nicht ändert,
    oder bis FILE_WAIT_TIMEOUT erreicht ist.
    """
    start = time.time()
    last_size = -1
    stable_since = None

    while True:
        try:
            size = os.path.getsize(path)
        except FileNotFoundError:
            time.sleep(0.2)
            if time.time() - start > FILE_WAIT_TIMEOUT:
                return False
            continue

        if size == last_size and size > 0:
            if stable_since is None:
                stable_since = time.time()
            if time.time() - stable_since >= FILE_STABLE_SECONDS:
                return True
        else:
            stable_since = None
            last_size = size

        if time.time() - start > FILE_WAIT_TIMEOUT:
            return False

        time.sleep(0.2)

# ─────────────────────────────────────────────
# OCR
# ─────────────────────────────────────────────

def ocr_pdf(input_path: str) -> str:
    """
    Führt OCR auf input_path aus und gibt Pfad zur OCR-Version zurück.
    output liegt im TEMP_FOLDER.
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
        # im Zweifel NICHT als leer markieren (lieber behalten)
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
    try:
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
    except Exception as e:
        log.error(f"  ✗ SFTP Fehler: {e}")

# ─────────────────────────────────────────────
# HAUPTVERARBEITUNG
# ─────────────────────────────────────────────

def process_pdf(file_path: str):
    """Verarbeitet ein einzelnes PDF komplett."""
    log.info(f"Verarbeite: {file_path}")

    # Warten bis der Scanner wirklich fertig geschrieben hat
    if not wait_until_file_is_stable(file_path):
        log.error("  ✗ Datei wurde nicht stabil / Timeout – übersprungen")
        return

    ocr_path = None
    output_files: list[str] = []

    try:
        # 0) OCR
        ocr_path = ocr_pdf(file_path)

        # 1) PDF splitten und leere Seiten entfernen (nach OCR)
        output_files = split_pdf(ocr_path)

        if not output_files:
            log.warning("  Keine Seiten übrig nach Filterung – Original wird behalten")
            return

        log.info(f"  {len(output_files)} Seite(n) nach Verarbeitung")

        # 2) Jede Seite an beide Endpoints schicken
        for f in output_files:
            copy_to_dropbox(f)
            copy_to_paperless(f)

        # 3) Temporäre Seiten löschen
        for f in output_files:
            try:
                os.remove(f)
            except Exception:
                pass

        # 4) OCR-Zwischendatei löschen
        if ocr_path:
            try:
                os.remove(ocr_path)
            except Exception:
                pass

        # 5) Original löschen
        os.remove(file_path)
        log.info(f"  ✓ Original gelöscht: {file_path}")

    except Exception as e:
        log.error(f"  ✗ Fehler bei Verarbeitung: {e}")
        # Bei Fehler: keine Dateien löschen, außer ggf. die erzeugten Splits (optional)
        # (Hier lassen wir absichtlich alles liegen, damit du debuggen kannst.)

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

        process_pdf(event.src_path)

# ─────────────────────────────────────────────
# START
# ─────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("Scan Pipeline gestartet (mit OCR)")
    log.info(f"Überwache: {WATCH_FOLDER}")
    log.info(f"Dropbox:   {DROPBOX_FOLDER}")
    log.info(f"Paperless: {LINUX_USER}@{LINUX_HOST}:{LINUX_CONSUME_PATH}")
    log.info(f"OCR Bin:   {OCRMY_PDF_BIN}")
    log.info("=" * 50)

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
