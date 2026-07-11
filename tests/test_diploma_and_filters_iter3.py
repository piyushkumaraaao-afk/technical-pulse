"""Iteration 3 backend tests — Diploma Eligible category, state/age filters, new fields, regressions."""
import uuid
from datetime import date, timedelta
from tests.conftest import API


# Orgs that must be present in Diploma Eligible category (partial-match on post_name OR organization)
REQUIRED_DIPLOMA_ORGS = [
    "SSC CHSL", "SSC MTS", "SSC JE",
    "RRB NTPC", "RRB JE",
    "India Post", "GDS",
    "Police Constable",
    "BSF", "CRPF", "CISF", "ITBP", "SSB",
    "Assam Rifles",
    "Merchant Navy",
    "Indian Coast Guard",
    "PGCIL",
    "State",  # State JE
    "Apprentice",  # Apprenticeship
]

REQUIRED_JOB_FIELDS = [
    "post_name", "organization", "category", "last_date", "apply_link",
    "eligibility", "salary", "vacancies",
    "selection_process", "important_dates", "previous_year_cutoff",
    "min_age", "max_age",
]


# =====================================================
# Diploma Eligible Category
# =====================================================
class TestDiplomaEligibleCategory:
    def test_diploma_eligible_returns_exactly_18(self, client):
        r = client.get(f"{API}/jobs", params={"category": "Diploma Eligible", "limit": 100})
        assert r.status_code == 200, r.text
        body = r.json()
        # Match seeded 18 jobs exactly
        jobs = [j for j in body["jobs"] if j.get("category") == "Diploma Eligible"]
        assert len(jobs) == 18, f"Expected exactly 18 Diploma Eligible jobs, got {len(jobs)}"

    def test_diploma_eligible_all_required_fields_non_null(self, client):
        r = client.get(f"{API}/jobs", params={"category": "Diploma Eligible", "limit": 100})
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        assert len(jobs) == 18
        for j in jobs:
            for f in REQUIRED_JOB_FIELDS:
                assert f in j, f"Missing field '{f}' in job {j.get('post_name')}"
                assert j[f] not in (None, ""), (
                    f"Field '{f}' is null/empty in job '{j.get('post_name')}' / '{j.get('organization')}'"
                )
            assert j["category"] == "Diploma Eligible"

    def test_all_18_required_orgs_present(self, client):
        r = client.get(f"{API}/jobs", params={"category": "Diploma Eligible", "limit": 100})
        jobs = r.json()["jobs"]
        # Build a haystack of "org + post_name" per job (case-insensitive)
        haystacks = [(j.get("organization", "") + " " + j.get("post_name", "")).lower() for j in jobs]
        missing = []
        for keyword in REQUIRED_DIPLOMA_ORGS:
            if not any(keyword.lower() in h for h in haystacks):
                missing.append(keyword)
        assert not missing, f"Missing Diploma Eligible orgs: {missing}"

    def test_job_detail_returns_selection_and_important_dates(self, client):
        r = client.get(f"{API}/jobs", params={"category": "Diploma Eligible", "limit": 100})
        jobs = r.json()["jobs"]
        assert jobs
        # Test the first 3 individually via /jobs/{id}
        for j in jobs[:3]:
            detail = client.get(f"{API}/jobs/{j['job_id']}")
            assert detail.status_code == 200
            d = detail.json()["job"]
            assert d.get("selection_process"), f"selection_process missing on {j['post_name']}"
            assert d.get("important_dates"), f"important_dates missing on {j['post_name']}"
            assert d.get("previous_year_cutoff"), f"previous_year_cutoff missing on {j['post_name']}"


# =====================================================
# State Filter
# =====================================================
class TestStateFilter:
    def test_state_assam_includes_assam_rifles(self, client):
        r = client.get(f"{API}/jobs", params={"state": "Assam", "limit": 100})
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        assert jobs, "state=Assam returned no jobs"
        has_assam_rifles = any("Assam Rifles" in j.get("organization", "") for j in jobs)
        assert has_assam_rifles, "Assam Rifles not returned for state=Assam"

    def test_state_all_bypasses_filter(self, client):
        r_all = client.get(f"{API}/jobs", params={"state": "All", "limit": 200})
        r_none = client.get(f"{API}/jobs", params={"limit": 200})
        assert r_all.status_code == 200 and r_none.status_code == 200
        assert r_all.json()["count"] == r_none.json()["count"], (
            "state=All should return same count as no state filter"
        )
        # And also >= 18 (Diploma) + prior seeded (12) — total at least 30
        assert r_all.json()["count"] >= 30


# =====================================================
# Age Filter
# =====================================================
class TestAgeFilter:
    def test_age_20_includes_bsf_and_gds(self, client):
        r = client.get(f"{API}/jobs", params={"age": 20, "limit": 200})
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        assert jobs, "age=20 returned no jobs"
        # All returned jobs must satisfy min_age <= 20 <= max_age (allowing null)
        for j in jobs:
            mn = j.get("min_age")
            mx = j.get("max_age")
            if mn is not None:
                assert mn <= 20, f"{j['post_name']} has min_age={mn} > 20"
            if mx is not None:
                assert mx >= 20, f"{j['post_name']} has max_age={mx} < 20"
        # BSF is 18-23 → must be included
        orgs = [j["organization"] for j in jobs]
        assert any("BSF" in o for o in orgs), "BSF (18-23) missing from age=20 results"
        # GDS is 18-40 → must be included
        assert any("India Post" in o for o in orgs), "India Post GDS (18-40) missing from age=20 results"

    def test_age_35_excludes_jobs_with_max_age_lt_35(self, client):
        r = client.get(f"{API}/jobs", params={"age": 35, "limit": 200})
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        for j in jobs:
            mx = j.get("max_age")
            if mx is not None:
                assert mx >= 35, f"{j['post_name']} max_age={mx} should have been excluded for age=35"
        # BSF (max 23) must NOT be in age=35 results
        orgs = [j["organization"] for j in jobs]
        assert not any("BSF" in o for o in orgs), "BSF (max 23) should be excluded for age=35"


# =====================================================
# Admin creation with new fields
# =====================================================
class TestAdminNewJobFields:
    def test_admin_post_accepts_selection_important_state(self, client, admin_headers):
        payload = {
            "organization": "TEST_iter3 Org",
            "post_name": f"TEST_iter3 Post {uuid.uuid4().hex[:6]}",
            "category": "Diploma Eligible",
            "branches": ["Computer Science"],
            "qualifications": ["Diploma"],
            "vacancies": "5",
            "salary": "₹30,000",
            "eligibility": "Diploma CSE",
            "location": "Bengaluru",
            "state": "Karnataka",
            "last_date": (date.today() + timedelta(days=30)).isoformat(),
            "apply_link": "https://example.com/apply",
            "min_age": 18,
            "max_age": 30,
            "description": "Iter3 test job",
            "selection_process": "CBT → Interview → DV",
            "important_dates": "Apply: Now • CBT: 30 days",
            "previous_year_cutoff": "Gen: 70 | OBC: 65",
        }
        r = client.post(f"{API}/admin/jobs", json=payload, headers=admin_headers)
        assert r.status_code == 200, r.text
        job = r.json()["job"]
        job_id = job["job_id"]
        try:
            assert job["state"] == "Karnataka"
            assert job["selection_process"] == payload["selection_process"]
            assert job["important_dates"] == payload["important_dates"]

            # Fetch via public GET and verify fields persisted
            r2 = client.get(f"{API}/jobs/{job_id}")
            assert r2.status_code == 200
            d = r2.json()["job"]
            assert d["selection_process"] == payload["selection_process"]
            assert d["important_dates"] == payload["important_dates"]
            assert d["state"] == "Karnataka"

            # Verify state filter picks it up
            r3 = client.get(f"{API}/jobs", params={"state": "Karnataka", "limit": 200})
            assert r3.status_code == 200
            assert any(j["job_id"] == job_id for j in r3.json()["jobs"])
        finally:
            client.delete(f"{API}/admin/jobs/{job_id}", headers=admin_headers)


# =====================================================
# Regression — existing filters still work
# =====================================================
class TestRegressionFilters:
    def test_location_filter_still_works(self, client):
        r = client.get(f"{API}/jobs", params={"location": "Bengaluru", "limit": 100})
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        assert jobs, "location=Bengaluru returned no jobs"
        for j in jobs:
            assert "bengaluru" in (j.get("location") or "").lower()

    def test_search_still_works(self, client):
        r = client.get(f"{API}/jobs", params={"search": "Railway", "limit": 100})
        assert r.status_code == 200
        assert r.json()["count"] > 0

    def test_category_all_returns_all(self, client):
        r_all = client.get(f"{API}/jobs", params={"category": "All", "limit": 200})
        r_none = client.get(f"{API}/jobs", params={"limit": 200})
        assert r_all.json()["count"] == r_none.json()["count"]

    def test_branch_and_qualification_still_work(self, client):
        r = client.get(f"{API}/jobs", params={"branch": "Computer Science", "qualification": "BTech"})
        assert r.status_code == 200
        for j in r.json()["jobs"]:
            assert "Computer Science" in j["branches"]
            assert "BTech" in j["qualifications"]

    def test_active_jobs_at_least_30(self, client):
        """12 prior seeded + 18 new Diploma Eligible = at least 30 active."""
        r = client.get(f"{API}/jobs", params={"limit": 200})
        assert r.status_code == 200
        assert r.json()["count"] >= 30, f"Expected >=30 active jobs, got {r.json()['count']}"


# =====================================================
# Regression — endpoints still work
# =====================================================
class TestRegressionEndpoints:
    def test_login_still_works(self, client):
        r = client.post(f"{API}/auth/login",
                        json={"email": "admin@careerpulse.in", "password": "Admin@123"})
        assert r.status_code == 200
        assert "access_token" in r.json()

    def test_profile_avatar_update(self, client, user_headers):
        avatar = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
        r = client.put(f"{API}/auth/profile", json={"avatar": avatar}, headers=user_headers)
        assert r.status_code == 200
        assert r.json()["user"]["avatar"] == avatar

    def test_recommended_and_eligibility(self, client, user_headers):
        # ensure profile is set for filtering
        client.put(f"{API}/auth/profile",
                   json={"qualification": "BTech", "branch": "Computer Science", "age": 22},
                   headers=user_headers)
        r = client.get(f"{API}/jobs/recommended", headers=user_headers)
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        assert jobs
        # eligibility
        r2 = client.post(f"{API}/jobs/check-eligibility",
                         json={"job_id": jobs[0]["job_id"]}, headers=user_headers)
        assert r2.status_code == 200
        assert "eligible" in r2.json()

    def test_ai_chat_reachable(self, client, user_headers):
        r = client.post(f"{API}/ai/chat", json={"message": "Hi"}, headers=user_headers, timeout=60)
        assert r.status_code in (200, 502)
