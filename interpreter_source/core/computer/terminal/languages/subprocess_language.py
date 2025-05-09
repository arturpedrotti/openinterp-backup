import os
import queue
import re
import subprocess
import threading
import time
import traceback

from ..base_language import BaseLanguage


class SubprocessLanguage(BaseLanguage):
    def __init__(self):
        self.start_cmd = []
        self.process = None
        self.verbose = False
        self.output_queue = queue.Queue()
        self.done = threading.Event()

    def detect_active_line(self, line):
        return None

    def detect_end_of_execution(self, line):
        return None

    def line_postprocessor(self, line):
        return line

    def preprocess_code(self, code):
        """
        This needs to insert an end_of_execution marker of some kind,
        which can be detected by detect_end_of_execution.

        Optionally, add active line markers for detect_active_line.
        """
        return code

    def terminate(self):
        if self.process:
            self.process.terminate()
            self.process.stdin.close()
            self.process.stdout.close()

    def start_process(self):
        if self.process:
            self.terminate()

        my_env = os.environ.copy()
        my_env["PYTHONIOENCODING"] = "utf-8"
        self.process = subprocess.Popen(
            self.start_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=0,
            universal_newlines=True,
            env=my_env,
            encoding="utf-8",
            errors="replace",
        )
        threading.Thread(
            target=self.handle_stream_output,
            args=(self.process.stdout, False),
            daemon=True,
        ).start()
        threading.Thread(
            target=self.handle_stream_output,
            args=(self.process.stderr, True),
            daemon=True,
        ).start()

    def run(self, code):
        try:
            result = subprocess.run(code, shell=True, capture_output=True, text=True)
            if result.stdout:
                yield {
                    "type": "console",
                    "format": "output",
                    "content": result.stdout.strip(),
                }
            if result.stderr:
                yield {
                    "type": "console",
                    "format": "output",
                    "content": f"stderr: {result.stderr.strip()}",
                }
        except Exception as e:
            yield {
                "type": "console",
                "format": "output",
                "content": f"Exception: {str(e)}",
            } 

    def handle_stream_output(self, stream, is_error_stream):
        try:
            for line in iter(stream.readline, ""):
                if self.verbose:
                    print(f"Received output line:\n{line}\n---")

                line = self.line_postprocessor(line)

                if line is None:
                    continue  # `line = None` is the postprocessor's signal to discard completely

                if self.detect_active_line(line):
                    active_line = self.detect_active_line(line)
                    self.output_queue.put(
                        {
                            "type": "console",
                            "format": "active_line",
                            "content": active_line,
                        }
                    )
                    # Sometimes there's a little extra on the same line, so be sure to send that out
                    line = re.sub(r"##active_line\d+##", "", line)
                    if line:
                        self.output_queue.put(
                            {"type": "console", "format": "output", "content": line}
                        )
                elif self.detect_end_of_execution(line):
                    # Sometimes there's a little extra on the same line, so be sure to send that out
                    line = line.replace("##end_of_execution##", "").strip()
                    if line:
                        self.output_queue.put(
                            {"type": "console", "format": "output", "content": line}
                        )
                    self.done.set()
                elif is_error_stream and "KeyboardInterrupt" in line:
                    self.output_queue.put(
                        {
                            "type": "console",
                            "format": "output",
                            "content": "KeyboardInterrupt",
                        }
                    )
                    time.sleep(0.1)
                    self.done.set()
                else:
                    self.output_queue.put(
                        {"type": "console", "format": "output", "content": line}
                    )
        except ValueError as e:
            if "operation on closed file" in str(e):
                if self.verbose:
                    print("Stream closed while reading.")
            else:
                raise e
