#infrastructure/runners/python_script_runner.py
import os, shlex, subprocess, sys
from pathlib import Path

class PythonScriptRunner:
    def __init__(self, repo_root: Path, env: dict | None = None):
        self.repo_root = repo_root
        self.env = {**os.environ, **(env or {})}

    def run(self, script_rel: Path, args: list[str]) -> int:
        script_path = (self.repo_root / script_rel).resolve()
        cmd = [sys.executable, str(script_path), *args]
        print("[RUN]", " ".join(shlex.quote(s) for s in cmd))
        return subprocess.run(cmd, env=self.env).returncode
