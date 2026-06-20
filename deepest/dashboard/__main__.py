"""Start the dashboard server standalone."""
from .server import start

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8766)
    a = ap.parse_args()
    start(host=a.host, port=a.port)
