"""trulyremote orchestrator wiring + preview/run pipeline (no network); uses the shared `test_db`
fixture, which rebinds the module-level SessionLocal so run() hits the test DB."""
import json

import pytest

from backend.models.db import Setting, Search, Job
from backend.scraper.sources import trulyremote


def _job(title, company, url, desc="Great role", salary_min=None, salary_max=None):
    return {"title": title, "company": company, "url": url, "location": "Worldwide",
            "description": desc, "employment_type": "Full-time", "posted": "2026-10-01",
            "salary_min": salary_min, "salary_max": salary_max}


def _patch_fetch(monkeypatch, jobs, details=None):
    async def fake_roster(client, wanted):
        return jobs

    async def fake_details(client, candidates):
        return {j["url"]: dict(j) for j in candidates} if details is None else details

    monkeypatch.setattr(trulyremote, "_fetch_roster", fake_roster)
    monkeypatch.setattr(trulyremote, "_fetch_details", fake_details)


# ── orchestrator wiring ─────────────────────────────────────────────────────

def test_always_valid():
    from backend.scraper import orchestrator
    assert orchestrator._search_mode_is_valid(Search(search_mode="trulyremote")) is True


def test_source_label():
    from backend.scraper import orchestrator
    assert orchestrator._source_for_search(Search(search_mode="trulyremote")) == "trulyremote"


@pytest.mark.asyncio
async def test_dispatch_routes_to_trulyremote(monkeypatch):
    from backend.scraper import orchestrator
    called = {}

    async def fake_run(search, **kw):
        called["ok"] = True
        return {"jobs_found": 0, "new_jobs": 0, "error": None, "duration": 0}

    monkeypatch.setattr(trulyremote, "run", fake_run)
    await orchestrator.run_search(Search(search_mode="trulyremote"))
    assert called.get("ok") is True


# ── preview() filtering ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_preview_applies_title_and_body_filters(monkeypatch, test_db):
    test_db.add(Setting(key="body_exclusion_phrases", value=json.dumps(["us citizen only"])))
    test_db.commit()

    _patch_fetch(monkeypatch, [
        _job("Backend Engineer", "Acme", "https://trw/1", desc="Great backend role"),
        _job("Data Intern", "Acme", "https://trw/2", desc="entry role"),
        _job("Senior Engineer", "BadCo", "https://trw/3", desc="US citizen only please"),
    ])

    search = Search(name="t", search_mode="trulyremote", title_exclude_keywords=["intern"], company_exclude=[])
    r = await trulyremote.preview(search, test_db)

    assert r["raw_count"] == 3
    by = {j["title"]: j for j in r["jobs"]}
    assert by["Backend Engineer"]["kept"] is True
    assert by["Data Intern"]["kept"] is False and "intern" in (by["Data Intern"]["reason"] or "").lower()
    assert by["Senior Engineer"]["kept"] is False and "Body exclusion" in (by["Senior Engineer"]["reason"] or "")
    assert r["after_filter"] == 1


# ── run() save + filter + counts ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_saves_and_filters(monkeypatch, test_db):
    search = Search(name="t", search_mode="trulyremote", results_wanted=10,
                    title_exclude_keywords=["intern"], company_exclude=[])
    test_db.add(search)
    test_db.commit()

    _patch_fetch(monkeypatch, [
        _job("Backend Engineer", "Acme", "https://trw/1"),
        _job("Data Intern", "Acme", "https://trw/2"),
    ])

    async def noop_analyze(job, db=None, h1b_median=None, arrangement=None):
        pass
    monkeypatch.setattr(trulyremote, "analyze_inline", noop_analyze)
    monkeypatch.setattr("backend.activity.log_activity", lambda *a, **k: None)

    res = await trulyremote.run(search)
    assert res["error"] is None
    assert res["jobs_found"] == 1 and res["new_jobs"] == 1

    test_db.expire_all()
    saved = test_db.query(Job).all()
    assert len(saved) == 1
    assert saved[0].source == "trulyremote"
    assert saved[0].title == "Backend Engineer"


@pytest.mark.asyncio
async def test_run_reports_a_seen_count_for_an_already_stored_job(monkeypatch, test_db):
    search = Search(name="t", search_mode="trulyremote", results_wanted=10,
                    title_exclude_keywords=[], company_exclude=[])
    test_db.add(search)
    test_db.commit()

    _patch_fetch(monkeypatch, [_job("Fixed Role", "Acme", "https://trw/1")])

    async def noop(job, db=None, h1b_median=None, arrangement=None):
        pass
    monkeypatch.setattr(trulyremote, "analyze_inline", noop)
    monkeypatch.setattr("backend.activity.log_activity", lambda *a, **k: None)

    first = await trulyremote.run(search)
    assert first["jobs_found"] == 1 and first["new_jobs"] == 1

    second = await trulyremote.run(search)
    assert second["new_jobs"] == 0
    assert second["jobs_found"] == 1   # seen before dedup, not 0


@pytest.mark.asyncio
async def test_run_stores_a_detail_salary_as_posting(monkeypatch, test_db):
    search = Search(name="t", search_mode="trulyremote", results_wanted=10,
                    title_exclude_keywords=[], company_exclude=[])
    test_db.add(search)
    test_db.commit()

    listing = _job("Paid Role", "Acme", "https://trw/1")
    details = {"https://trw/1": {**listing, "salary_min": 152000, "salary_max": 205000}}
    _patch_fetch(monkeypatch, [listing], details=details)

    async def noop(job, db=None, h1b_median=None, arrangement=None):
        pass
    monkeypatch.setattr(trulyremote, "analyze_inline", noop)
    monkeypatch.setattr("backend.activity.log_activity", lambda *a, **k: None)

    await trulyremote.run(search)
    test_db.expire_all()
    row = test_db.query(Job).filter(Job.title == "Paid Role").first()
    assert row.salary_min == 152000 and row.salary_max == 205000
    assert row.salary_source == "posting"
