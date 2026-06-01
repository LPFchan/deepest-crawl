"""Manual smoke crawl: pick first valid URL and run it."""
import json, time, urllib.parse
from pathlib import Path

from deepest import brain, signals
from deepest.engines.chrome import RealChromeEngine
from deepest.perception.policy import perceive
from deepest.skills_store.store import SkillStore

links = json.loads(Path("inputs/links.json").read_text())
valid = [l for l in links if urllib.parse.urlparse(l["url"]).netloc and l["url"].startswith("http")]
target = valid[0]
print(f"Target: [{target['id']}] {target['url']}")

engine = RealChromeEngine().connect()
store = SkillStore()
tab = engine.new_tab(target["url"])
engine.cdp(tab, "Page.enable")
time.sleep(2)

p = perceive(engine, tab, target["url"], skill_store=store)
print(f"Mode: {p.mode}, text len: {len(p.text or '')}, has_image: {p.image_png is not None}")

if p.mode == "vision":
    summary = brain.summarize_image(target["url"], p.image_png)
else:
    summary = brain.summarize_text(target["url"], p.text or "")
print(f"Summary ({len(summary)} chars):")
print(summary[:500])

engine.close_tab(tab)
engine.close()
