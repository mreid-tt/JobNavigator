"""trulyremotework.com source — roster/detail parsing (no network)."""
import pytest

from backend.scraper.sources import trulyremote

_ROSTER = """# TrulyRemoteWork.com — Full Job Corpus

---

## Senior Backend Engineer at Acme
- URL: https://trulyremotework.com/jobs/senior-backend-engineer-acme
- Type: Full-time
- Category: Engineering
- Location: Worldwide (no restrictions)
- Skills: Go, Kubernetes
- Salary: $152k - $205k
- Posted: 2026-10-01

Build the backend.

---

## Other Role at Beta
- URL: https://trulyremotework.com/jobs/other-role-beta
- Location: Worldwide (no restrictions)
- Posted: 2026-09-30

Do other things.
"""


def test_parse_roster_maps_every_field():
    jobs = trulyremote._parse_roster(_ROSTER)
    assert [j["title"] for j in jobs] == ["Senior Backend Engineer", "Other Role"]
    first = jobs[0]
    assert first["company"] == "Acme"
    assert first["url"] == "https://trulyremotework.com/jobs/senior-backend-engineer-acme"
    assert first["employment_type"] == "Full-time"
    assert first["location"] == "Worldwide (no restrictions)"
    assert first["posted"] == "2026-10-01"
    assert (first["salary_min"], first["salary_max"]) == (152000, 205000)
    assert first["description"] == "Build the backend."
    # The second has no salary/type and still parses.
    assert jobs[1]["company"] == "Beta"
    assert jobs[1]["salary_min"] is None
    assert jobs[1]["employment_type"] == ""


def test_parse_roster_splits_on_the_last_at():
    jobs = trulyremote._parse_roster("## Engineer at Large at Acme\n- URL: u\n\ndesc\n")
    assert jobs[0]["title"] == "Engineer at Large"
    assert jobs[0]["company"] == "Acme"


@pytest.mark.parametrize("text,expected", [
    ("$152k - $205k", (152000, 205000)),
    ("$123,840—$209,520", (123840, 209520)),
    ("€100K - €125K", (100000, 125000)),
    ("$178.5K", (178500, None)),
    ("Competitive", (None, None)),
    ("", (None, None)),
])
def test_parse_salary(text, expected):
    assert trulyremote._parse_salary(text) == expected


def test_get_url_appends_the_md_twin():
    assert trulyremote._get_url("https://trulyremotework.com/jobs/x") == "https://trulyremotework.com/jobs/x.md"


def test_md_text_strips_links_and_emphasis():
    assert trulyremote._md_text("[Growe](https://trulyremotework.com/companies/growe)") == "Growe"
    assert trulyremote._md_text("**Full-time**") == "Full-time"


_DETAIL = """# Affiliate Manager

**Company:** [Growe](https://trulyremotework.com/companies/growe)
**Location:** Worldwide (Fully Remote)
**Category:** Sales
**Posted:** Thu Oct 01 2026
**Page:** https://trulyremotework.com/jobs/affiliate-manager-growe
**Apply:** https://job-boards.eu.greenhouse.io/growetalents/jobs/4965477101
**Tags:** Manager, Growe Partners

## Summary

Short summary.

## Description

The full description body.
"""


def test_parse_detail_prefers_the_md_twin():
    out = trulyremote._parse_detail(_DETAIL, {"title": "stale", "company": "", "url": "u"})
    assert out["title"] == "Affiliate Manager"
    assert out["company"] == "Growe"
    assert out["location"] == "Worldwide (Fully Remote)"
    assert out["posted"] == "Thu Oct 01 2026"
    assert out["apply_url"] == "https://job-boards.eu.greenhouse.io/growetalents/jobs/4965477101"
    assert out["description"] == "The full description body."


class _Resp:
    def __init__(self, status, headers=None):
        self.status_code = status
        self.headers = headers or {}


class _FakeClient:
    def __init__(self, statuses):
        self._statuses = list(statuses)
        self.calls = 0

    async def get(self, url, params=None):
        self.calls += 1
        return _Resp(self._statuses.pop(0))


def test_retry_after_parses_and_caps():
    assert trulyremote._retry_after(_Resp(429, {"Retry-After": "7"})) == 7.0
    assert trulyremote._retry_after(_Resp(429, {"Retry-After": "999"})) == 60.0
    assert trulyremote._retry_after(_Resp(429, {})) == 0.0


@pytest.mark.asyncio
async def test_get_backs_off_on_429_then_returns(monkeypatch):
    async def _no_sleep(_seconds):
        return None
    monkeypatch.setattr(trulyremote.asyncio, "sleep", _no_sleep)

    client = _FakeClient([429, 200])
    resp = await trulyremote._get(client, "u")
    assert resp.status_code == 200
    assert client.calls == 2


def test_user_agent_identifies_the_project():
    assert "github.com/vesaias/JobNavigator" in trulyremote._UA
