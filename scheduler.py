import logging
import threading
import time

from extract import run_etl

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

RUN_INTERVAL_SECONDS = 5 * 60  # 5 minutes


class ETLScheduler:
    """Lightweight scheduler for running the crypto ETL on a fixed interval."""

    def __init__(self, interval_seconds=RUN_INTERVAL_SECONDS):
        self.interval_seconds = interval_seconds
        self.stop_event = threading.Event()
        self.run_lock = threading.Lock()
        self.current_worker = None

    def _run_cycle(self):
        # Prevent overlapping runs
        if not self.run_lock.acquire(blocking=False):
            logger.warning("Previous ETL cycle is still running. Skipping this cycle.")
            return

        started_at = time.monotonic()
        logger.info("ETL cycle started.")

        try:
            run_etl()
        except Exception as error:
            logger.exception("ETL cycle failed: %s", error)
        finally:
            elapsed_seconds = time.monotonic() - started_at
            logger.info("ETL cycle finished in %.2f seconds.", elapsed_seconds)
            self.run_lock.release()

    def _start_cycle(self):
        self.current_worker = threading.Thread(
            target=self._run_cycle,
            name="crypto-etl-worker",
            daemon=True  # ✅ Added for safe shutdown
        )
        self.current_worker.start()

    def start(self):
        logger.info(
            "Scheduler started. Running ETL every %s seconds.",
            self.interval_seconds,
        )

        next_run_at = time.monotonic()  # immediate first run

        try:
            while not self.stop_event.is_set():
                wait_seconds = max(0, next_run_at - time.monotonic())

                if self.stop_event.wait(wait_seconds):
                    break

                self._start_cycle()
                next_run_at += self.interval_seconds

        except KeyboardInterrupt:
            logger.info("Shutdown requested by user.")
            self.stop()

        finally:
            self._wait_for_current_cycle()
            logger.info("Scheduler stopped.")

    def stop(self):
        self.stop_event.set()

    def _wait_for_current_cycle(self):
        if self.current_worker and self.current_worker.is_alive():
            logger.info("Waiting for active ETL cycle to finish.")
            self.current_worker.join()


if __name__ == "__main__":
    scheduler = ETLScheduler()
    scheduler.start()