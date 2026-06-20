#!/usr/bin/env python3
"""Extract stuck links from Eastself's cache.db -> deepest-crawl input JSON.

Source: ~/Documents/Eastself/data/cache.db  table `link_cache`
Filter: triage_status IN (unretrievable, needs_deep_crawl)
Output: inputs/links.json  [{id, url, reason, sub_reason, has_image, prior_len}]
"""
import argparse, hashlib, json, re, sqlite3
from pathlib import Path


def sanitize_url(url: str) -> str:
    u = url.strip()
    u = re.sub(r"[\uAC00-\uD7AF]+$", "", u)
    u = re.sub(r"[)\]}>\"']+$", "", u)
    u = u.rstrip(".,;:!? ")
    return u

DB = Path.home() / "Documents/Eastself/data/cache.db"
OUT = Path(__file__).resolve().parent / "inputs" / "links.json"
STATUSES = ("unretrievable", "needs_deep_crawl")


def export_links(db: str | Path = DB, out: str | Path = OUT,
                 statuses: list[str] | tuple[str, ...] = STATUSES,
                 limit: int = 0) -> dict:
    db = Path(db).expanduser()
    out = Path(out).expanduser()
    statuses = tuple(statuses or STATUSES)
    if not db.exists():
        raise FileNotFoundError(f"Eastself cache DB not found: {db}")
    if not statuses:
        raise ValueError("At least one triage status is required.")

    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)  # read-only; won't disturb Eastself
    con.row_factory = sqlite3.Row
    q = ("SELECT url, triage_status, triage_source, image_url, "
         "       length(coalesce(content,'')) AS prior_len "
         "FROM link_cache WHERE triage_status IN (%s) AND url IS NOT NULL "
         "ORDER BY triage_status, url" % ",".join("?" * len(statuses)))
    rows = con.execute(q, statuses).fetchall()

    seen, links = set(), []
    for r in rows:
        u = sanitize_url(r["url"])
        if not u or u in seen:
            continue
        seen.add(u)
        links.append({
            "id": hashlib.sha1(u.encode()).hexdigest()[:12],
            "url": u,
            "reason": r["triage_status"],
            "sub_reason": r["triage_source"],
            "has_image": bool(r["image_url"]),
            "prior_len": r["prior_len"],
        })
    if limit:
        links = links[:limit]

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(links, ensure_ascii=False, indent=2))
    by = {}
    for o in links:
        by[o["reason"]] = by.get(o["reason"], 0) + 1
    return {
        "db": str(db),
        "out": str(out),
        "count": len(links),
        "by_reason": by,
        "statuses": list(statuses),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DB))
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--statuses", nargs="+", default=list(STATUSES))
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    result = export_links(a.db, a.out, a.statuses, a.limit)
    print(f"wrote {result['count']} links -> {result['out']}")
    by = result["by_reason"]
    print("by reason:", by)


if __name__ == "__main__":
    main()
