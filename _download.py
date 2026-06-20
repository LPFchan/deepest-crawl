import os, time
from huggingface_hub import snapshot_download

MODEL = os.environ.get(
    "DEEPEST_BRAIN_MODEL",
    "froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit",
)

t0=time.time()
p=snapshot_download(MODEL, token=os.environ.get("HF_TOKEN"))
print(f"DOWNLOADED in {time.time()-t0:.0f}s -> {p}", flush=True)
