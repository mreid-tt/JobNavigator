"""trulyremotework.com source — the board publishes a machine-readable corpus and invites
ingestion: `llms-full.txt` lists every worldwide-remote job, and each job has a `.md` twin with
its full description and the employer's apply link. So the roster is one request, and only the
kept jobs need a detail fetch. Every role is screened worldwide-remote, so the search needs no
term or URL — the title and company filters do the narrowing.
"""
import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone

import httpx
from sqlalchemy.exc import IntegrityError

from backend.models.db import (
    SessionLocal, Job, Search, Setting, get_existing_external_ids,
    get_global_title_exclude,
)
from backend.scraper._shared.dedup import make_external_id, make_content_hash
from backend.scraper._shared.filters import build_search_exclude_sets
from backend.scraper._shared.analysis import analyze_inline

logger = logging.getLogger("jobnavigator.trulyremote")

BASE = "https://trulyremotework.com"
ROSTER_URL = f"{BASE}/llms-full.txt"
_UA = "JobNavigator/1.0 (+https://github.com/vesaias/JobNavigator)"
_DETAIL_CONCURRENCY = 4
_RATE_LIMIT_TRIES = 3


def _get_url(url: str) -> str:
    """The board publishes a `.md` twin for every page; use it for structured content."""
    return url.rstrip("/") + ".md"


def _md_text(value: str) -> str:
    """Strip markdown emphasis/links from a field value: `[Name](url)` → `Name`."""
    value = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", value or "")
    return re.sub(r"[*_`]+", "", value).strip()


def _parse_salary(text: str) -> tuple[int | None, int | None]:
    """The first one or two amounts in a salary string (`$152k - $205k`, `€100K - €125K`)."""
    if not text:
        return None, None
    nums: list[float] = []
    for amount, suffix in re.findall(r"(\d[\d,]*(?:\.\d+)?)\s*([kK])?", text):
        try:
            value = float(amount.replace(",", ""))
        except ValueError:
            continue
        nums.append(value * 1000 if suffix else value)
    nums = [n for n in nums if n >= 1000]
    if not nums:
        return None, None
    if len(nums) == 1:
        return int(nums[0]), None
    return int(min(nums[0], nums[1])), int(max(nums[0], nums[1]))


def _block_description(block: str) -> str:
    """Everything after the `- Field:` lines in a roster block."""
    lines = []
    for line in block.splitlines():
        if line.startswith("## "):
            continue
        if re.match(r"^-\s+[A-Za-z][\w /]*:", line):
            continue
        lines.append(line)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _parse_roster(text: str) -> list[dict]:
    """The `## <title> at <company>` blocks of `llms-full.txt` (or a category `.md` page)."""
    out = []
    for block in re.split(r"^---\s*$", text, flags=re.M):
        m = re.search(r"^##\s+(.*)\s+at\s+(.*?)\s*$", block, flags=re.M)
        if not m:
            continue
        fields = dict(re.findall(r"^-\s+([A-Za-z][\w /]*):\s*(.*)$", block, flags=re.M))
        salary_min, salary_max = _parse_salary(fields.get("Salary", ""))
        out.append({
            "title": _md_text(m.group(1)),
            "company": _md_text(m.group(2)),
            "url": (fields.get("URL") or "").strip(),
            "location": (fields.get("Location") or "").strip(),
            "description": _block_description(block),
            "employment_type": (fields.get("Type") or "").strip(),
            "posted": (fields.get("Posted") or "").strip(),
            "salary_min": salary_min,
            "salary_max": salary_max,
        })
    return out


_DETAIL_FIELD_RE = re.compile(r"^\*\*(Company|Location|Category|Posted|Page|Apply|Tags|Salary):\*\*\s*(.*)$", re.M)


def _parse_detail(text: str, fallback: dict) -> dict:
    """Enrich a roster row with its `.md` twin: full description, apply URL, salary."""
    out = dict(fallback)
    m = re.search(r"^#\s+(.*)$", text, flags=re.M)
    if m:
        out["title"] = _md_text(m.group(1))
    fields = {k: v for k, v in _DETAIL_FIELD_RE.findall(text)}
    if fields.get("Company"):
        out["company"] = _md_text(fields["Company"])
    if fields.get("Location"):
        out["location"] = _md_text(fields["Location"])
    if fields.get("Posted"):
        out["posted"] = fields["Posted"].strip()
    if fields.get("Apply"):
        out["apply_url"] = fields["Apply"].strip()
    desc = re.search(r"^##\s+Description\s*$(.*)", text, flags=re.M | re.S)
    if desc:
        out["description"] = desc.group(1).strip()
    if not out.get("salary_min"):
        out["salary_min"], out["salary_max"] = _parse_salary(fields.get("Salary", ""))
    return out


def _retry_after(resp) -> float:
    try:
        return min(float(resp.headers.get("Retry-After", "")), 60.0)
    except (TypeError, ValueError):
        return 0.0


async def _get(client: httpx.AsyncClient, url: str, params: dict | None = None):
    """GET with a bounded backoff on 429, so a throttling board slows us down instead of failing."""
    resp = await client.get(url, params=params)
    for attempt in range(_RATE_LIMIT_TRIES - 1):
        if resp.status_code != 429:
            return resp
        wait = _retry_after(resp) or 5.0 * (2 ** attempt)
        logger.warning(f"trulyremote rate-limited (429); waiting {wait:.0f}s")
        await asyncio.sleep(wait)
        resp = await client.get(url, params=params)
    return resp


async def _fetch_roster(client: httpx.AsyncClient, wanted: int) -> list[dict]:
    """One request for the whole corpus (`llms-full.txt` lists every open job)."""
    resp = await _get(client, ROSTER_URL)
    resp.raise_for_status()
    return _parse_roster(resp.text)[:wanted]


async def _fetch_details(client: httpx.AsyncClient, candidates: list[dict]) -> dict:
    """Fetch each kept job's `.md` twin (bounded concurrency) for the full description. Keyed by
    URL; a detail that fails leaves the roster's short blurb in place."""
    sem = asyncio.Semaphore(_DETAIL_CONCURRENCY)
    details: dict[str, dict] = {}

    async def one(job: dict) -> None:
        async with sem:
            try:
                resp = await _get(client, _get_url(job["url"]))
                if resp.status_code == 200:
                    details[job["url"]] = _parse_detail(resp.text, job)
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.debug(f"trulyremote detail failed for {job['url']}: {e}")

    await asyncio.gather(*(one(j) for j in candidates))
    return details


def _title_filters(search: Search, db) -> tuple[list, list]:
    include_kw = search.title_include_keywords or []
    exclude_kw = list(set((search.title_exclude_keywords or []) + get_global_title_exclude(db)))
    return include_kw, exclude_kw


def _title_kept(title: str, include_kw: list, exclude_kw: list) -> tuple[bool, str | None]:
    tl = title.lower()
    if include_kw and not any(kw.lower() in tl for kw in include_kw):
        return False, f"No match for: {', '.join(include_kw)}"
    if exclude_kw:
        matched = [kw for kw in exclude_kw if re.search(r'\b' + re.escape(kw) + r'\b', tl)]
        if matched:
            return False, f"Excluded by: {', '.join(matched)}"
    return True, None


async def run(search: Search) -> dict:
    """Full scrape entry point. Fetch roster → filter → fetch details → save to DB."""
    start = time.time()
    try:
        db = SessionLocal()
        try:
            include_kw, exclude_kw = _title_filters(search, db)
            global_exclude_set, search_exclude_set = build_search_exclude_sets(db, search)
            existing_ids = get_existing_external_ids(db)

            async with httpx.AsyncClient(timeout=30, headers={"User-Agent": _UA}) as client:
                listings = await _fetch_roster(client, search.results_wanted or 100)
                candidates = []
                seen = 0
                for j in listings:
                    if not j["url"]:
                        continue
                    ok, _ = _title_kept(j["title"], include_kw, exclude_kw)
                    if not ok:
                        continue
                    company_lower = (j.get("company") or "").lower()
                    if company_lower in global_exclude_set or company_lower in search_exclude_set:
                        continue
                    seen += 1
                    if make_external_id(j["company"], j["title"], j["url"]) in existing_ids:
                        continue
                    candidates.append(j)
                details = await _fetch_details(client, candidates)

            logger.info(f"trulyremote '{search.name}': {len(listings)} rows / {seen} seen / {len(candidates)} new to fetch")

            new_jobs = 0
            for j in candidates:
                if j["url"] in details:
                    j = {**j, **details[j["url"]]}
                if not j["company"]:
                    continue

                ext_id = make_external_id(j["company"], j["title"], j["url"])
                job = Job(
                    external_id=ext_id,
                    content_hash=make_content_hash(j["company"], j["title"]),
                    company=j["company"],
                    title=j["title"],
                    url=j.get("apply_url") or j["url"],
                    source="trulyremote",
                    search_id=search.id,
                    location=j.get("location") or "Worldwide",
                    description=j.get("description") or None,
                    status="new",
                    seen=False,
                    saved=False,
                )

                if j.get("salary_min"):
                    job.salary_min = j["salary_min"]
                    if j.get("salary_max"):
                        job.salary_max = j["salary_max"]
                    job.salary_source = "posting"

                try:
                    await analyze_inline(job, db=db)
                except Exception as e:
                    logger.warning(f"trulyremote inline analysis failed for {j['title']}: {e}")

                if job.h1b_jd_flag:
                    logger.info(f"Skipping (body exclusion): {j['title']} @ {j.get('company', '?')}")
                    continue

                try:
                    with db.begin_nested():
                        db.add(job)
                        db.flush()
                    new_jobs += 1
                    existing_ids.add(ext_id)
                except IntegrityError:
                    logger.debug(f"Duplicate external_id for '{j['title']}' @ {j.get('company')}, skipping")
                    continue
                except Exception as e:
                    logger.warning(f"Insert failed for '{j['title']}' @ {j.get('company')} ({j['url']}): {e}")
                    continue

            search_obj = db.query(Search).filter(Search.id == search.id).first()
            if search_obj:
                search_obj.last_run_at = datetime.now(timezone.utc)
            db.commit()
        finally:
            db.close()

        duration = time.time() - start
        from backend.activity import log_activity
        log_activity("scrape", f"trulyremote '{search.name}': {new_jobs} new / {seen} seen in {duration:.1f}s")
        return {"jobs_found": seen, "new_jobs": new_jobs, "error": None, "duration": duration}

    except Exception as e:
        duration = time.time() - start
        logger.error(f"trulyremote scrape failed for '{search.name}': {e}")
        from backend.activity import log_activity
        log_activity("scrape", f"trulyremote '{search.name}' failed: {e}")
        return {"jobs_found": 0, "new_jobs": 0, "error": str(e), "duration": duration}


async def preview(search: Search, db) -> dict:
    """Dry-run: fetch the roster, apply filters, return per-job diagnostics without saving."""
    start = time.time()
    try:
        include_kw, exclude_kw = _title_filters(search, db)
        global_exclude_set, search_exclude_set = build_search_exclude_sets(db, search)
        all_exclude = list(global_exclude_set | search_exclude_set)

        async with httpx.AsyncClient(timeout=30, headers={"User-Agent": _UA}) as client:
            unique = await _fetch_roster(client, search.results_wanted or 100)
            survivors = []
            for j in unique:
                kept, _ = _title_kept(j["title"], include_kw, exclude_kw)
                if kept:
                    cl = (j.get("company") or "").lower()
                    if cl not in global_exclude_set and cl not in search_exclude_set:
                        survivors.append(j)
            details = await _fetch_details(client, survivors)

        raw_count = len(unique)
        from collections import Counter
        company_breakdown = dict(Counter(j["company"] for j in unique if j.get("company")).most_common(20))

        body_row = db.query(Setting).filter(Setting.key == "body_exclusion_phrases").first()
        body_phrases = []
        if body_row and body_row.value:
            try:
                body_phrases = json.loads(body_row.value)
            except json.JSONDecodeError:
                pass

        results = []
        for j in unique:
            j = details.get(j["url"], j)
            kept, reason = _title_kept(j["title"], include_kw, exclude_kw)
            if kept:
                cl = (j.get("company") or "").lower()
                if cl in global_exclude_set:
                    kept, reason = False, f"Company excluded (global): {cl}"
                elif cl in search_exclude_set:
                    kept, reason = False, f"Company excluded: {cl}"
            if kept and body_phrases and j.get("description"):
                from backend.analyzer.h1b_checker import scan_jd_for_h1b_flags
                br = scan_jd_for_h1b_flags(j["description"], body_phrases)
                if br["jd_flag"]:
                    kept = False
                    reason = f"Body exclusion: {(br['jd_snippet'] or 'matched')[:80]}"

            salary = None
            if j.get("salary_min"):
                salary = f"{j['salary_min']:,}"
                if j.get("salary_max") and j["salary_max"] != j["salary_min"]:
                    salary += f" – {j['salary_max']:,}"

            desc = j.get("description") or ""
            results.append({
                "title": j["title"],
                "company": j.get("company", ""),
                "url": j.get("url", ""),
                "source": "trulyremote",
                "location": j.get("location", ""),
                "salary": salary,
                "has_description": bool(desc and len(desc) > 50),
                "desc_length": len(desc),
                "kept": kept,
                "reason": reason if not kept else None,
                "employment_type": j.get("employment_type") or None,
                "posted": j.get("posted"),
            })

        after_filter = sum(1 for r in results if r["kept"])
        return {
            "search_name": search.name,
            "duration": round(time.time() - start, 1),
            "raw_count": raw_count,
            "after_filter": after_filter,
            "source_breakdown": {"trulyremote": raw_count},
            "company_breakdown": company_breakdown,
            "include_keywords": include_kw,
            "exclude_keywords": exclude_kw,
            "company_filter": search.company_filter or [],
            "company_exclude": all_exclude,
            "jobs": results,
            "config": {
                "mode": "trulyremote",
                "results_wanted": search.results_wanted or 100,
            },
        }
    except Exception as e:
        return {
            "search_name": search.name,
            "error": str(e),
            "duration": round(time.time() - start, 1),
            "config": {"mode": "trulyremote"},
        }
