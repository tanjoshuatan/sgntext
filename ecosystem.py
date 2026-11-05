from __future__ import annotations

import asyncio
import os
import sqlite3
import struct
import subprocess
import time
import uuid
from pathlib import Path

import fcntl
import pty
import termios
import yaml

HERE = Path(__file__).resolve().parent

# ---------------------------------------------------------------- helpers

def log(msg: str) -> None:
    print(f"[ecosystem] {msg}", flush=True)

def get_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

# ---------------------------------------------------------------- shell

class PersistentShell:
    """Manages a single bash process mapped to a session_id."""

    def __init__(self, sid: str, out_file: Path):
        self.sid = sid
        self.out_file = out_file
        self.proc = None
        self.master_fd = None
        self.reader = None
        self.transport = None
        self.pump_task = None
        self.lock = asyncio.Lock()
        
        self.current_marker = None
        self.exit_code = None
        self.buf = ""
        
        self._lines = 0
        if self.out_file.exists():
            try:
                self._lines = sum(1 for _ in open(self.out_file, "r", encoding="utf-8"))
            except Exception:
                pass

    def _proc_dead(self) -> bool:
        if self.proc is None:
            return True
        return self.proc.poll() is not None

    async def start(self):
        if self.pump_task:
            self.pump_task.cancel()
            try: await self.pump_task
            except (asyncio.CancelledError, Exception): pass
            self.pump_task = None
            
        if self.transport:
            self.transport.close()
            self.transport = None

        master, slave = pty.openpty()
        flags = fcntl.fcntl(master, fcntl.F_GETFL)
        fcntl.fcntl(master, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        
        try:
            attrs = termios.tcgetattr(slave)
            attrs[3] &= ~(termios.ECHO | termios.ECHONL)
            attrs[3] |= termios.NOFLSH
            attrs[1] &= ~termios.ONLCR
            termios.tcsetattr(slave, termios.TCSANOW, attrs)
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))
        except Exception:
            pass

        env = os.environ.copy()
        env["TERM"] = "xterm-256color"
        env["PS1"] = ""
        env["PS2"] = ""
        env["PROMPT_COMMAND"] = ""
        env["PYTHONUNBUFFERED"] = "1"

        def _child_setup():
            os.setsid()
            try: fcntl.ioctl(0, termios.TIOCSCTTY, 0)
            except OSError: pass

        rc_file = HERE / ".bashrc"
        cmd = ["/bin/bash", "--noediting"]
        if rc_file.exists():
            cmd.extend(["--rcfile", str(rc_file)])
        cmd.append("-i")

        self.proc = subprocess.Popen(
            cmd, stdin=slave, stdout=slave, stderr=slave,
            env=env, preexec_fn=_child_setup, close_fds=True,
        )
        os.close(slave)
        self.master_fd = os.dup(master)

        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        self.transport, _ = await loop.connect_read_pipe(lambda: protocol, _PipeEndpoint(master))
        self.reader = reader
        self.pump_task = asyncio.create_task(self._pump())

    async def _pump(self):
        try:
            while True:
                try:
                    chunk = await self.reader.read(4096)
                except Exception:
                    break
                if not chunk:
                    break
                
                text = chunk.decode("utf-8", errors="replace")
                
                if not self.current_marker:
                    self._append_output(text)
                    continue

                self.buf += text
                idx = self.buf.find(self.current_marker)
                
                if idx != -1:
                    # Marker found
                    pre = self.buf[:idx]
                    if pre.endswith("\n"):
                        pre = pre[:-1]
                    self._append_output(pre)
                    
                    rest = self.buf[idx + len(self.current_marker):]
                    nl = rest.find("\n")
                    if nl != -1:
                        code_str = rest[:nl].strip()
                        try:
                            self.exit_code = int(code_str)
                        except ValueError:
                            self.exit_code = -1
                        
                        self.buf = rest[nl+1:]
                        self.current_marker = None
                        
                        # Write any remaining text after the marker
                        if self.buf:
                            self._append_output(self.buf)
                            self.buf = ""
                else:
                    # No marker yet, keep a safe buffer size to avoid cutting a marker in half
                    keep = len(self.current_marker) - 1
                    if len(self.buf) > keep:
                        self._append_output(self.buf[:-keep])
                        self.buf = self.buf[-keep:]
        finally:
            pass

    def _append_output(self, text: str):
        if not text:
            return
        try:
            with open(self.out_file, "a", encoding="utf-8") as f:
                f.write(text)
            
            self._lines += text.count("\n")
            if self._lines > 50000:
                self._truncate()
        except Exception as e:
            log(f"Failed to write output: {e}")

    def _truncate(self):
        try:
            with open(self.out_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > 50000:
                with open(self.out_file, "w", encoding="utf-8") as f:
                    f.writelines(lines[-50000:])
            self._lines = 50000
        except Exception as e:
            log(f"Failed to truncate: {e}")

    async def run(self, cmd: str, on_finish):
        async with self.lock:
            if self._proc_dead():
                await self.start()
            
            marker = f"__RELAY_DONE_{uuid.uuid4().hex}__"
            self.current_marker = marker
            self.exit_code = None
            self.buf = ""
            
            # Escape single quotes for safe bash injection
            escaped_cmd = cmd.replace("'", "'\\''")
            
            # This prompt evaluates the shell's current pwd and prints it in green!
            prompt = f"printf '\\n\\033[1;32m%s > %s\\033[0m\\n' \"$(pwd)\" '{escaped_cmd}'\n"
            payload = f"{prompt}{cmd}\nprintf '\\n{marker}%d\\n' $?\n"
            
            view = memoryview(payload.encode("utf-8"))
            while view:
                try:
                    n = os.write(self.master_fd, view)
                    view = view[n:]
                except BlockingIOError:
                    await asyncio.sleep(0.005)
                except Exception:
                    break
            
            while self.current_marker is not None:
                await asyncio.sleep(0.1)
            
            on_finish(self.exit_code if self.exit_code is not None else -1)

    async def interrupt(self):
        if self.master_fd is not None:
            try:
                os.write(self.master_fd, b"\x03")
            except OSError:
                pass

    async def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try: self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired: self.proc.kill()
        if self.transport:
            self.transport.close()
        if self.master_fd is not None:
            os.close(self.master_fd)
            self.master_fd = None
        if self.pump_task:
            self.pump_task.cancel()


class _PipeEndpoint:
    __slots__ = ("_fd",)
    def __init__(self, fd): self._fd = fd
    def fileno(self): return self._fd
    def close(self):
        try: os.close(self._fd)
        except OSError: pass

# ---------------------------------------------------------------- main loop

async def main():
    with open(HERE / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    
    command_root = Path(cfg.get("COMMAND_ROOT", "command"))
    if not command_root.is_absolute():
        command_root = HERE / command_root
    
    db_path = command_root / "commands.db"
    out_dir = command_root / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    db = get_db(db_path)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            job_name TEXT,
            current_command TEXT,
            status TEXT,
            exit_code INTEGER,
            kill_requested INTEGER DEFAULT 0,
            created_at REAL,
            updated_at REAL
        )
    """)
    db.commit()

    shells: dict[str, PersistentShell] = {}
    log(f"Ecosystem running. DB: {db_path}")

    while True:
        try:
            rows = db.execute("SELECT * FROM sessions").fetchall()
            active_sids = set(shells.keys())
            db_sids = {r["session_id"] for r in rows}

            for sid in active_sids - db_sids:
                log(f"Session {sid} removed. Killing shell.")
                await shells[sid].stop()
                del shells[sid]

            for row in rows:
                sid = row["session_id"]
                
                if sid not in shells:
                    shells[sid] = PersistentShell(sid, out_dir / f"{sid}.txt")
                    await shells[sid].start()

                if row["kill_requested"] == 1:
                    db.execute("UPDATE sessions SET kill_requested=0 WHERE session_id=?", (sid,))
                    db.commit()
                    await shells[sid].interrupt()

                if row["status"] == "requested":
                    db.execute("UPDATE sessions SET status='running', updated_at=? WHERE session_id=?", (time.time(), sid))
                    db.commit()
                    
                    cmd = row["current_command"]
                    
                    def make_on_finish(s_id):
                        def on_finish(code):
                            new_status = "exited" if code == 0 else "error"
                            db.execute("UPDATE sessions SET status=?, exit_code=?, updated_at=? WHERE session_id=?", (new_status, code, time.time(), s_id))
                            db.commit()
                        return on_finish
                    
                    asyncio.create_task(shells[sid].run(cmd, make_on_finish(sid)))

        except Exception as e:
            log(f"Loop error: {e}")
        
        await asyncio.sleep(1.0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Shutting down.")