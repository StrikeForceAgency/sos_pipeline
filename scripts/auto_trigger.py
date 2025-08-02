"""
File watcher that automatically kicks off enrichment when new CSVs arrive.

This helper script monitors the ``raw/`` directory for the appearance of new
CSV files.  When a new file is detected it waits briefly (to avoid partial
writes) and then calls the enrichment module.  It is intended to be run as a
long‑running background process or managed by a process supervisor.

Requires the ``watchdog`` package.  Install it via pip:

    pip install watchdog

Usage:

    python3 -m scripts.auto_trigger
"""

from __future__ import annotations

import time
from pathlib import Path

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError as exc:
    raise SystemExit(
        "The watchdog package is required to use the auto‑trigger. "
        "Install it with `pip install watchdog` and try again."
    ) from exc

from scripts.enrichment import enrich_and_export


class CSVCreationHandler(FileSystemEventHandler):
    """Handler that invokes enrichment when a new CSV file is created."""

    def __init__(self, debounce_seconds: float = 3.0) -> None:
        super().__init__()
        self._debounce_seconds = debounce_seconds
        self._last_run = 0.0

    def on_created(self, event) -> None:
        if event.is_directory or not event.src_path.lower().endswith(".csv"):
            return
        now = time.time()
        if now - self._last_run < self._debounce_seconds:
            return
        self._last_run = now
        print(f"📂 Detected new CSV: {event.src_path}")
        time.sleep(1.0)  # wait for file to finish writing
        try:
            enrich_and_export()
        except Exception as e:
            print(f"❌ Error during auto‑trigger enrichment: {e}")


def main() -> None:
    raw_dir = Path("raw")
    if not raw_dir.exists():
        print(f"🚫 Raw directory {raw_dir.resolve()} does not exist; exiting auto‑trigger.")
        return
    event_handler = CSVCreationHandler()
    observer = Observer()
    observer.schedule(event_handler, str(raw_dir), recursive=False)
    observer.start()
    print(f"👂 Listening for new CSV files in {raw_dir.resolve()} ...")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("🛑 Stopping auto‑trigger watcher...")
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    main()
