import os, time
from huggingface_hub import snapshot_download
t0=time.time()
p=snapshot_download("froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit", token=os.environ["HF_TOKEN"])
print(f"DOWNLOADED in {time.time()-t0:.0f}s -> {p}", flush=True)
