from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class StreamShutdownTests(unittest.TestCase):
    def test_stop_reaches_python_before_the_submit_process_exits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ready = Path(directory) / "ready"
            stopped = Path(directory) / "stopped"
            query = """
from pathlib import Path
import signal, sys, time
def stop(*_):
    Path(sys.argv[2]).write_text('query stopped')
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
Path(sys.argv[1]).touch()
while True:
    time.sleep(0.05)
"""
            submit = """
from pathlib import Path
import subprocess, sys
child = subprocess.Popen([sys.executable, '-c', sys.argv[1], *sys.argv[2:]])
result = child.wait()
with Path(sys.argv[3]).open('a') as output:
    output.write('; submit exited')
raise SystemExit(result)
"""
            process = subprocess.Popen([
                "bash", str(PROJECT_ROOT / "spark-apps/run_stream.sh"),
                sys.executable, "-c", submit, query, str(ready), str(stopped),
            ])
            try:
                deadline = time.monotonic() + 5
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(ready.exists(), "query did not start")
                process.send_signal(signal.SIGTERM)
                self.assertEqual(process.wait(timeout=5), 0)
                self.assertEqual(stopped.read_text(), "query stopped; submit exited")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
