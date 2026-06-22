"""Local service startup for deepest-crawl.

The crawler depends on two local services:

- an OpenAI-compatible MLX-VLM server on 127.0.0.1:8765
- Chrome with the Open Browser Use extension transport active

Autostart is enabled by default and can be disabled with:

- DEEPEST_BRAIN_AUTOSTART=0
- DEEPEST_CHROME_AUTOSTART=0
"""

from __future__ import annotations

import os
import platform
import subprocess
import sys
import time
import json
from pathlib import Path

ACTIVE_REGISTRY = Path("/tmp/open-browser-use/active.json")
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_DIR = ROOT / "outputs" / "logs"
_BRAIN_PROC: subprocess.Popen | None = None
_BRAIN_LOG_HANDLE = None
_CHROME_PROC: subprocess.Popen | None = None
_CHROME_STATUS_CACHE = {
    "checked_at": 0.0,
    "ready": False,
    "error": "",
    "registry": None,
}


KNOWN_BRAIN_MODELS = [
    {
        "id": "qwen3.6-27b-heretic-mlx-4bit",
        "label": "Qwen3.6 27B Heretic MLX 4-bit",
        "model": "froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit",
        "vision": True,
        "source": "huggingface",
        "note": "Vision-capable, but currently Metal OOMs on this machine during generation.",
    },
    {
        "id": "holo-3.1-9b-mlx",
        "label": "Holo 3.1 9B MLX",
        "model": str(Path.home() / "models" / "Holo-3.1-9B-mlx"),
        "vision": True,
        "source": "local",
        "note": "Vision-language Holo model for computer-use agents.",
    },
    {
        "id": "holo-3.1-9b",
        "label": "Holo 3.1 9B",
        "model": str(Path.home() / "models" / "Holo-3.1-9B"),
        "vision": False,
        "source": "local",
        "note": "Local Holo source directory; this copy has no vision_tower tensors in its safetensors index.",
    },
]


def _enabled(name: str) -> bool:
    return os.environ.get(name, "1").lower() not in {"0", "false", "no", "off"}


def _brain_log_path() -> Path:
    return Path(os.environ.get(
        "DEEPEST_BRAIN_LOG",
        str(DEFAULT_LOG_DIR / "mlx-brain.log"),
    )).expanduser()


def _tail_text(path: Path, max_bytes: int = 12000) -> str:
    try:
        if not path.exists():
            return ""
        with path.open("rb") as f:
            if path.stat().st_size > max_bytes:
                f.seek(-max_bytes, os.SEEK_END)
            return f.read().decode("utf-8", errors="replace").strip()
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"


def _brain_exit_code() -> int | None:
    if _BRAIN_PROC is None:
        return None
    return _BRAIN_PROC.poll()


def _chrome_probe_timeout() -> float:
    try:
        return max(0.5, float(os.environ.get("DEEPEST_CHROME_PROBE_TIMEOUT", "2.5")))
    except ValueError:
        return 2.5


def _read_chrome_registry() -> tuple[dict | None, str]:
    if not ACTIVE_REGISTRY.exists():
        return None, "Open Browser Use active registry is missing."
    try:
        registry = json.loads(ACTIVE_REGISTRY.read_text())
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(registry, dict):
        return None, "Open Browser Use active registry is not a JSON object."
    socket_path = registry.get("socketPath")
    if not socket_path:
        return registry, "Open Browser Use active registry has no socketPath."
    if not Path(str(socket_path)).exists():
        return registry, f"Open Browser Use socket is missing: {socket_path}"
    return registry, ""


def _probe_chrome_transport(registry: dict, timeout: float) -> tuple[bool, str]:
    socket_path = str(registry.get("socketPath") or "")
    try:
        from open_browser_use.client import OpenBrowserUseClient  # type: ignore
        client = OpenBrowserUseClient(
            socket_path=socket_path,
            session_id=f"deepest-health-{int(time.time() * 1000)}",
            timeout=timeout,
        ).connect()
        try:
            client.get_tabs()
        finally:
            client.close()
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def chrome_transport_status(*, ttl_seconds: float = 2.0, timeout: float | None = None) -> dict:
    now = time.time()
    cached = _CHROME_STATUS_CACHE
    if ttl_seconds > 0 and now - float(cached["checked_at"]) < ttl_seconds:
        return dict(cached)

    registry, registry_error = _read_chrome_registry()
    ready = False
    error = registry_error
    if registry is not None and not registry_error:
        ready, probe_error = _probe_chrome_transport(registry, timeout or _chrome_probe_timeout())
        error = probe_error

    cached.update({
        "checked_at": now,
        "ready": ready,
        "error": error,
        "registry": registry,
    })
    return dict(cached)


def _available_brain_models() -> list[dict]:
    current = os.environ.get(
        "DEEPEST_BRAIN_MODEL",
        "froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit",
    )
    models: list[dict] = []
    seen: set[str] = set()
    for item in KNOWN_BRAIN_MODELS:
        model = item["model"]
        exists = item["source"] != "local" or Path(model).expanduser().exists()
        entry = {
            **item,
            "exists": exists,
            "active": model == current,
        }
        models.append(entry)
        seen.add(model)

    hf_root = Path.home() / ".cache" / "huggingface" / "hub"
    if hf_root.exists():
        for path in sorted(hf_root.glob("models--*")):
            model = path.name.removeprefix("models--").replace("--", "/")
            if model in seen:
                continue
            models.append({
                "id": path.name.removeprefix("models--"),
                "label": model,
                "model": model,
                "vision": None,
                "source": "huggingface-cache",
                "exists": True,
                "active": model == current,
                "note": "Cached locally; MLX server compatibility not verified.",
            })
            seen.add(model)
    return models


def _resolve_brain_model(model_id: str | None = None,
                         model: str | None = None) -> dict:
    requested = (model_id or model or "").strip()
    models = _available_brain_models()
    if requested:
        for item in models:
            if requested in {item["id"], item["model"], item["label"]}:
                return item
        if model:
            return {
                "id": model,
                "label": model,
                "model": model,
                "vision": os.environ.get("DEEPEST_BRAIN_VISION", "1") == "1",
                "source": "custom",
                "exists": True,
                "active": False,
                "note": "Custom model path/id supplied by operator.",
            }
        raise ValueError(f"Unknown brain model: {requested}")

    current = os.environ.get(
        "DEEPEST_BRAIN_MODEL",
        "froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit",
    )
    for item in models:
        if item["model"] == current:
            return item
    return {
        "id": current,
        "label": current,
        "model": current,
        "vision": os.environ.get("DEEPEST_BRAIN_VISION", "1") == "1",
        "source": "env",
        "exists": True,
        "active": True,
        "note": "Current environment model.",
    }


def stop_brain() -> None:
    global _BRAIN_PROC, _BRAIN_LOG_HANDLE
    if _BRAIN_PROC is not None and _BRAIN_PROC.poll() is None:
        _BRAIN_PROC.terminate()
        try:
            _BRAIN_PROC.wait(timeout=10)
        except Exception:
            _BRAIN_PROC.kill()
    _BRAIN_PROC = None
    if _BRAIN_LOG_HANDLE is not None:
        try:
            _BRAIN_LOG_HANDLE.close()
        except Exception:
            pass
        _BRAIN_LOG_HANDLE = None


def configure_brain(model_id: str | None = None, model: str | None = None,
                    restart: bool = False) -> dict:
    selected = _resolve_brain_model(model_id=model_id, model=model)
    os.environ["DEEPEST_BRAIN_MODEL"] = selected["model"]
    os.environ["DEEPEST_BRAIN_VISION"] = "1" if selected.get("vision") else "0"
    from . import brain
    brain.configure(model=selected["model"], vision=bool(selected.get("vision")))
    if restart:
        stop_brain()
    return {**selected, "active": True}


def _brain_failure_message(prefix: str, model: str) -> str:
    log_path = _brain_log_path()
    exit_code = _brain_exit_code()
    pieces = [f"{prefix} (model: {model})"]
    if exit_code is not None:
        pieces.append(f"exit code: {exit_code}")
    pieces.append(f"log: {log_path}")
    tail = _tail_text(log_path)
    if tail:
        pieces.append(f"recent log:\n{tail[-4000:]}")
    return "\n".join(pieces)


def _mlx_server_args() -> list[str]:
    args: list[str] = []
    options = [
        ("DEEPEST_MLX_VISION_CACHE_SIZE", "--vision-cache-size", ""),
        ("DEEPEST_MLX_PREFILL_STEP_SIZE", "--prefill-step-size", ""),
        ("DEEPEST_MLX_MAX_TOKENS", "--max-tokens", ""),
        ("DEEPEST_MLX_MAX_KV_SIZE", "--max-kv-size", ""),
        ("DEEPEST_MLX_KV_BITS", "--kv-bits", ""),
    ]
    for env_name, flag, default in options:
        value = os.environ.get(env_name, default).strip()
        if value:
            args.extend([flag, value])
    return args


def status() -> dict:
    from . import brain

    chrome_status = chrome_transport_status()
    registry = chrome_status.get("registry")

    log_path = _brain_log_path()
    exit_code = _brain_exit_code()
    return {
        "brain": {
            "ready": brain.alive(timeout=1.0),
            "endpoint": os.environ.get(
                "DEEPEST_BRAIN_ENDPOINT",
                "http://127.0.0.1:8765/v1/chat/completions",
            ),
            "models_endpoint": brain.models_endpoint(),
            "model": os.environ.get(
                "DEEPEST_BRAIN_MODEL",
                "froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit",
            ),
            "autostart": _enabled("DEEPEST_BRAIN_AUTOSTART"),
            "managed_pid": _BRAIN_PROC.pid if _BRAIN_PROC and _BRAIN_PROC.poll() is None else None,
            "managed_exit_code": exit_code,
            "log_path": str(log_path),
            "log_tail": _tail_text(log_path, max_bytes=6000),
            "available_models": _available_brain_models(),
        },
        "chrome": {
            "ready": bool(chrome_status.get("ready")),
            "registry": str(ACTIVE_REGISTRY),
            "socket_path": registry.get("socketPath") if isinstance(registry, dict) else None,
            "registry_error": chrome_status.get("error", ""),
            "autostart": _enabled("DEEPEST_CHROME_AUTOSTART"),
        },
    }


def ensure_brain(status=None, wait_seconds: float | None = None):
    """Return the brain module after verifying or launching its local server."""
    global _BRAIN_PROC, _BRAIN_LOG_HANDLE
    from . import brain

    if brain.alive():
        return brain
    if _BRAIN_PROC is not None and _BRAIN_PROC.poll() is None:
        if status:
            status("waiting for existing MLX brain process")
        wait_s = wait_seconds if wait_seconds is not None else float(os.environ.get("DEEPEST_BRAIN_WAIT", "180"))
        deadline = time.time() + wait_s
        while time.time() < deadline:
            if brain.alive():
                if status:
                    status("MLX brain ready")
                return brain
            if _BRAIN_PROC.poll() is not None:
                break
            time.sleep(1)
    if not _enabled("DEEPEST_BRAIN_AUTOSTART"):
        raise RuntimeError("Brain server is not reachable and autostart is disabled.")

    selected = configure_brain()
    model = selected["model"]
    host = os.environ.get("DEEPEST_BRAIN_HOST", "127.0.0.1")
    port = os.environ.get("DEEPEST_BRAIN_PORT", "8765")
    extra_args = _mlx_server_args()
    wait_s = wait_seconds if wait_seconds is not None else float(os.environ.get("DEEPEST_BRAIN_WAIT", "180"))

    if status:
        status(f"launching MLX brain ({Path(model).name})")
    log_path = _brain_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if _BRAIN_LOG_HANDLE is not None:
        try:
            _BRAIN_LOG_HANDLE.close()
        except Exception:
            pass
    _BRAIN_LOG_HANDLE = log_path.open("a", buffering=1)
    _BRAIN_LOG_HANDLE.write(
        f"\n--- starting MLX brain at {time.strftime('%Y-%m-%d %H:%M:%S')} "
        f"model={model} host={host} port={port} extra_args={extra_args} ---\n"
    )
    _BRAIN_PROC = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "mlx_vlm.server",
            "--model",
            model,
            "--host",
            host,
            "--port",
            port,
            *extra_args,
        ],
        stdout=_BRAIN_LOG_HANDLE,
        stderr=subprocess.STDOUT,
    )

    deadline = time.time() + wait_s
    while time.time() < deadline:
        if brain.alive():
            if status:
                status("MLX brain ready")
            return brain
        if _BRAIN_PROC.poll() is not None:
            raise RuntimeError(_brain_failure_message(
                "Brain server exited before becoming ready",
                model,
            ))
        time.sleep(1)
    raise RuntimeError(_brain_failure_message(
        f"Brain server failed to start within {wait_s:.0f}s",
        model,
    ))


def launch_chrome(status=None) -> None:
    """Launch Chrome using the operator's normal profile."""
    global _CHROME_PROC
    app = os.environ.get("DEEPEST_CHROME_APP", "Google Chrome")
    if status:
        status(f"launching Chrome ({app})")

    if platform.system() == "Darwin":
        _CHROME_PROC = subprocess.Popen(
            ["open", "-a", app, "about:blank"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return

    binary = os.environ.get("DEEPEST_CHROME_BIN", "google-chrome")
    _CHROME_PROC = subprocess.Popen(
        [binary, "about:blank"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def ensure_chrome_transport(status=None) -> Path:
    """Return the OBU active registry after verifying or launching Chrome."""
    chrome_status = chrome_transport_status(ttl_seconds=0)
    if chrome_status.get("ready"):
        return ACTIVE_REGISTRY
    if not _enabled("DEEPEST_CHROME_AUTOSTART"):
        error = chrome_status.get("error") or "Chrome OBU transport is not active."
        raise RuntimeError(f"{error} Chrome autostart is disabled.")

    if status and chrome_status.get("error"):
        status(f"Chrome OBU transport not ready: {chrome_status['error']}")
    launch_chrome(status=status)
    wait_s = float(os.environ.get("DEEPEST_CHROME_WAIT", "60"))
    deadline = time.time() + wait_s
    while time.time() < deadline:
        chrome_status = chrome_transport_status(ttl_seconds=0)
        if chrome_status.get("ready"):
            if status:
                status("Chrome OBU transport ready")
            return ACTIVE_REGISTRY
        time.sleep(1)

    error = chrome_status.get("error") or "transport probe did not become ready"
    raise RuntimeError(
        "Chrome launched, but the Open Browser Use transport did not become active. "
        f"{error}. Make sure the Open Browser Use extension and native host are installed and enabled."
    )


def shutdown_autostarted() -> None:
    global _CHROME_PROC
    stop_brain()
    _CHROME_PROC = None
