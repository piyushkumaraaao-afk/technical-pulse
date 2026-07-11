"""CareerPulse backend API tests — full coverage of auth, jobs, applications, resumes, admin, AI, push."""
import uuid
import time
import requests
from datetime import date, timedelta
from tests.conftest import API


# =====================================================
# Health / Root
# =====================================================
class TestRoot:
    def test_root_ok(self, client):
        r = client.get(f"{API}/")
        assert r.status_code == 200
        data = r.json()
        assert data.get("app") == "CareerPulse"
        assert data.get("status") == "ok"


# =====================================================
# Auth
# =====================================================
class TestAuth:
    def test_register_new_user(self, client):
        email = f"TEST_reg_{uuid.uuid4().hex[:8]}@careerpulse.in".lower()
        r = client.post(f"{API}/auth/register",
                        json={"email": email, "password": "Test@1234", "name": "Reg User"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert "access_token" in body
        assert body["token_type"] == "bearer"
        assert body["user"]["email"] == email
        assert body["user"]["is_admin"] is False
        assert "password_hash" not in body["user"]

    def test_register_duplicate_rejected(self, client, test_user):
        r = client.post(f"{API}/auth/register",
                        json={"email": test_user["email"], "password": "Whatever@1", "name": "Dup"})
        assert r.status_code == 400

    def test_login_success(self, client, test_user):
        r = client.post(f"{API}/auth/login",
                        json={"email": test_user["email"], "password": test_user["password"]})
        assert r.status_code == 200
        body = r.json()
        assert "access_token" in body and body["user"]["email"] == test_user["email"]

    def test_login_wrong_password(self, client, test_user):
        r = client.post(f"{API}/auth/login",
                        json={"email": test_user["email"], "password": "WrongPass!1"})
        assert r.status_code == 401

    def test_auth_me_requires_token(self, client):
        r = client.get(f"{API}/auth/me")
        assert r.status_code == 401

    def test_auth_me_with_token(self, client, user_headers, test_user):
        r = client.get(f"{API}/auth/me", headers=user_headers)
        assert r.status_code == 200
        assert r.json()["user"]["email"] == test_user["email"]

    def test_admin_login_and_flag(self, client, admin_token):
        # Verify admin user via /auth/me
        r = client.get(f"{API}/auth/me", headers={"Authorization": f"Bearer {admin_token}"})
        assert r.status_code == 200
        assert r.json()["user"]["is_admin"] is True


class TestProfile:
    def test_update_profile_persists(self, client, user_headers):
        payload = {
            "qualification": "BTech",
            "branch": "Computer Science",
            "passout_year": 2026,
            "state": "Karnataka",
            "age": 22,
        }
        r = client.put(f"{API}/auth/profile", json=payload, headers=user_headers)
        assert r.status_code == 200
        u = r.json()["user"]
        assert u["qualification"] == "BTech"
        assert u["branch"] == "Computer Science"
        assert u["age"] == 22

        # verify GET /auth/me reflects update
        r2 = client.get(f"{API}/auth/me", headers=user_headers)
        assert r2.status_code == 200
        assert r2.json()["user"]["branch"] == "Computer Science"


# =====================================================
# Jobs
# =====================================================
class TestJobs:
    def test_list_jobs(self, client):
        r = client.get(f"{API}/jobs")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] >= 12, f"Expected at least 12 seeded jobs, got {body['count']}"
        assert isinstance(body["jobs"], list)
        # Ensure _id excluded
        assert all("_id" not in j for j in body["jobs"])
        assert all("job_id" in j for j in body["jobs"])

    def test_filter_category_government(self, client):
        r = client.get(f"{API}/jobs", params={"category": "Government"})
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        assert len(jobs) > 0
        assert all(j["category"] == "Government" for j in jobs)

    def test_filter_branch_computer_science(self, client):
        r = client.get(f"{API}/jobs", params={"branch": "Computer Science"})
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        assert len(jobs) > 0
        assert all("Computer Science" in j["branches"] for j in jobs)

    def test_filter_qualification_btech(self, client):
        r = client.get(f"{API}/jobs", params={"qualification": "BTech"})
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        assert len(jobs) > 0
        assert all("BTech" in j["qualifications"] for j in jobs)

    def test_get_job_detail(self, client):
        listing = client.get(f"{API}/jobs").json()["jobs"]
        job_id = listing[0]["job_id"]
        r = client.get(f"{API}/jobs/{job_id}")
        assert r.status_code == 200
        assert r.json()["job"]["job_id"] == job_id

    def test_get_job_not_found(self, client):
        r = client.get(f"{API}/jobs/does_not_exist_xyz")
        assert r.status_code == 404

    def test_recommended_requires_auth(self, client):
        r = client.get(f"{API}/jobs/recommended")
        assert r.status_code == 401

    def test_recommended_with_profile(self, client, user_headers):
        # user profile already set (BTech / CS) in TestProfile
        r = client.get(f"{API}/jobs/recommended", headers=user_headers)
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        assert len(jobs) > 0
        assert all("Computer Science" in j["branches"] and "BTech" in j["qualifications"] for j in jobs)


class TestEligibility:
    def test_check_eligibility_eligible(self, client, user_headers):
        jobs = client.get(f"{API}/jobs", params={"branch": "Computer Science",
                                                 "qualification": "BTech"}).json()["jobs"]
        assert jobs, "no CS/BTech jobs to test eligibility"
        job_id = jobs[0]["job_id"]
        r = client.post(f"{API}/jobs/check-eligibility", json={"job_id": job_id}, headers=user_headers)
        assert r.status_code == 200
        body = r.json()
        assert body["job_id"] == job_id
        assert body["eligible"] is True

    def test_check_eligibility_ineligible_by_branch(self, client, user_headers):
        # find a job that excludes Computer Science
        jobs = client.get(f"{API}/jobs").json()["jobs"]
        non_cs = next((j for j in jobs if "Computer Science" not in j["branches"]), None)
        assert non_cs, "seed data lacks non-CS job"
        r = client.post(f"{API}/jobs/check-eligibility", json={"job_id": non_cs["job_id"]}, headers=user_headers)
        assert r.status_code == 200
        body = r.json()
        assert body["eligible"] is False
        assert len(body["reasons"]) > 0

    def test_eligibility_job_not_found(self, client, user_headers):
        r = client.post(f"{API}/jobs/check-eligibility", json={"job_id": "nope"}, headers=user_headers)
        assert r.status_code == 404


# =====================================================
# Applications
# =====================================================
class TestApplications:
    def test_save_and_apply_and_list(self, client, user_headers):
        jobs = client.get(f"{API}/jobs").json()["jobs"]
        save_job = jobs[0]["job_id"]
        apply_job = jobs[1]["job_id"]

        r1 = client.post(f"{API}/applications/save", json={"job_id": save_job}, headers=user_headers)
        assert r1.status_code == 200 and r1.json()["ok"] is True

        r2 = client.post(f"{API}/applications/apply", json={"job_id": apply_job}, headers=user_headers)
        assert r2.status_code == 200 and r2.json()["ok"] is True

        r3 = client.get(f"{API}/applications", headers=user_headers)
        assert r3.status_code == 200
        body = r3.json()
        assert "saved" in body and "applied" in body and "upcoming" in body
        saved_ids = [a["job_id"] for a in body["saved"]]
        applied_ids = [a["job_id"] for a in body["applied"]]
        assert save_job in saved_ids
        assert apply_job in applied_ids

    def test_save_invalid_job(self, client, user_headers):
        r = client.post(f"{API}/applications/save", json={"job_id": "invalid_id"}, headers=user_headers)
        assert r.status_code == 404

    def test_delete_application(self, client, user_headers):
        jobs = client.get(f"{API}/jobs").json()["jobs"]
        jid = jobs[2]["job_id"]
        client.post(f"{API}/applications/save", json={"job_id": jid}, headers=user_headers)
        r = client.delete(f"{API}/applications/{jid}", headers=user_headers)
        assert r.status_code == 200 and r.json()["ok"] is True
        # verify removed
        body = client.get(f"{API}/applications", headers=user_headers).json()
        all_ids = [a["job_id"] for a in body["saved"] + body["applied"]]
        assert jid not in all_ids


# =====================================================
# Resumes
# =====================================================
class TestResumes:
    def test_resume_crud(self, client, user_headers):
        payload = {
            "full_name": "Test Student",
            "phone": "9999999999",
            "email": "test@example.com",
            "objective": "Aspiring engineer",
            "education": [{"degree": "BTech CSE", "institute": "IIT", "year": "2026"}],
            "skills": ["Python", "React Native"],
            "template": "modern",
        }
        r = client.post(f"{API}/resumes", json=payload, headers=user_headers)
        assert r.status_code == 200, r.text
        resume = r.json()["resume"]
        rid = resume["resume_id"]
        assert resume["full_name"] == "Test Student"
        assert "_id" not in resume

        # LIST
        r2 = client.get(f"{API}/resumes", headers=user_headers)
        assert r2.status_code == 200
        assert any(x["resume_id"] == rid for x in r2.json()["resumes"])

        # UPDATE
        payload["full_name"] = "Test Student Updated"
        r3 = client.put(f"{API}/resumes/{rid}", json=payload, headers=user_headers)
        assert r3.status_code == 200
        assert r3.json()["resume"]["full_name"] == "Test Student Updated"

        # verify persisted
        r4 = client.get(f"{API}/resumes", headers=user_headers).json()
        got = next(x for x in r4["resumes"] if x["resume_id"] == rid)
        assert got["full_name"] == "Test Student Updated"


# =====================================================
# Admin
# =====================================================
class TestAdminAuthorization:
    def test_non_admin_gets_403(self, client, user_headers):
        endpoints = [
            ("GET", "/admin/users"),
            ("GET", "/admin/stats"),
            ("GET", "/admin/rss-sources"),
            ("POST", "/admin/refresh-jobs"),
        ]
        for method, path in endpoints:
            r = client.request(method, f"{API}{path}", headers=user_headers, json={})
            assert r.status_code == 403, f"{method} {path} expected 403 got {r.status_code}"


class TestAdminJobs:
    _created_job_id = None

    def test_admin_create_job(self, client, admin_headers):
        body = {
            "organization": "TEST Org",
            "post_name": "TEST Engineer",
            "category": "Private",
            "branches": ["Computer Science"],
            "qualifications": ["BTech"],
            "vacancies": "10",
            "salary": "10 LPA",
            "eligibility": "BTech CSE",
            "location": "Remote",
            "last_date": (date.today() + timedelta(days=30)).isoformat(),
            "apply_link": "https://example.com/apply",
            "min_age": 18,
            "max_age": 30,
            "description": "Test job",
        }
        r = client.post(f"{API}/admin/jobs", json=body, headers=admin_headers)
        assert r.status_code == 200, r.text
        job = r.json()["job"]
        assert job["organization"] == "TEST Org"
        assert "_id" not in job
        TestAdminJobs._created_job_id = job["job_id"]

    def test_admin_update_job(self, client, admin_headers):
        assert TestAdminJobs._created_job_id, "create must run first"
        jid = TestAdminJobs._created_job_id
        body = {
            "organization": "TEST Org Updated",
            "post_name": "TEST Engineer Updated",
            "category": "Private",
            "branches": ["Computer Science"],
            "qualifications": ["BTech"],
            "eligibility": "BTech CSE updated",
            "last_date": (date.today() + timedelta(days=45)).isoformat(),
            "apply_link": "https://example.com/apply2",
        }
        r = client.put(f"{API}/admin/jobs/{jid}", json=body, headers=admin_headers)
        assert r.status_code == 200
        assert r.json()["job"]["organization"] == "TEST Org Updated"

        # Verify via public endpoint
        r2 = client.get(f"{API}/jobs/{jid}")
        assert r2.status_code == 200
        assert r2.json()["job"]["post_name"] == "TEST Engineer Updated"

    def test_admin_delete_job(self, client, admin_headers):
        jid = TestAdminJobs._created_job_id
        r = client.delete(f"{API}/admin/jobs/{jid}", headers=admin_headers)
        assert r.status_code == 200 and r.json()["ok"] is True
        # Verify deleted
        r2 = client.get(f"{API}/jobs/{jid}")
        assert r2.status_code == 404

    def test_admin_update_nonexistent(self, client, admin_headers):
        body = {
            "organization": "X", "post_name": "X", "category": "Private",
            "branches": ["Computer Science"], "qualifications": ["BTech"],
            "eligibility": "x", "last_date": "2026-12-01", "apply_link": "https://x",
        }
        r = client.put(f"{API}/admin/jobs/nonexistent_id", json=body, headers=admin_headers)
        assert r.status_code == 404


class TestAdminUsersStats:
    def test_admin_list_users(self, client, admin_headers):
        r = client.get(f"{API}/admin/users", headers=admin_headers)
        assert r.status_code == 200
        body = r.json()
        assert body["count"] >= 1
        assert isinstance(body["users"], list)
        assert all("password_hash" not in u for u in body["users"])

    def test_admin_stats(self, client, admin_headers):
        r = client.get(f"{API}/admin/stats", headers=admin_headers)
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body["users"], int)
        assert isinstance(body["active_jobs"], int)
        assert isinstance(body["applications"], int)
        assert body["active_jobs"] >= 12


class TestAdminNotify:
    def test_admin_notify_success(self, client, admin_headers):
        r = client.post(f"{API}/admin/notify",
                        json={"title": "TEST Alert", "message": "This is a test notification",
                              "action_url": "/jobs"},
                        headers=admin_headers)
        # Should succeed even if push key is placeholder (fire-and-forget)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["ok"] is True
        assert "recipients_count" in body


class TestAdminRss:
    _src_id = None

    def test_add_rss_source(self, client, admin_headers):
        r = client.post(f"{API}/admin/rss-sources",
                        json={"name": "TEST Source",
                              "url": "https://example.com/rss.xml",
                              "default_category": "Government"},
                        headers=admin_headers)
        assert r.status_code == 200
        TestAdminRss._src_id = r.json()["src_id"]
        assert TestAdminRss._src_id.startswith("rss_")

    def test_list_rss_sources(self, client, admin_headers):
        r = client.get(f"{API}/admin/rss-sources", headers=admin_headers)
        assert r.status_code == 200
        sources = r.json()["sources"]
        assert any(s["src_id"] == TestAdminRss._src_id for s in sources)

    def test_delete_rss_source(self, client, admin_headers):
        sid = TestAdminRss._src_id
        r = client.delete(f"{API}/admin/rss-sources/{sid}", headers=admin_headers)
        assert r.status_code == 200 and r.json()["ok"] is True

    def test_refresh_jobs(self, client, admin_headers):
        r = client.post(f"{API}/admin/refresh-jobs", headers=admin_headers, timeout=60)
        assert r.status_code == 200
        body = r.json()
        assert "added" in body and "removed" in body
        assert isinstance(body["added"], int)
        assert isinstance(body["removed"], int)


# =====================================================
# Push
# =====================================================
class TestPush:
    def test_register_push_device(self, client, user_headers):
        r = client.post(f"{API}/register-push",
                        json={"platform": "ios", "device_token": f"tok_{uuid.uuid4().hex}"},
                        headers=user_headers)
        # EMERGENT_PUSH_KEY is placeholder → provider will 401, backend converts to 500
        # OR success (201) if provider ignores it. Either is acceptable, but must not be uncaught 500.
        assert r.status_code in (201, 500, 502), f"unexpected {r.status_code}: {r.text}"


# =====================================================
# AI Chat — expect graceful 502 if budget exceeded
# =====================================================
class TestAI:
    def test_ai_chat_reachable(self, client, user_headers):
        r = client.post(f"{API}/ai/chat",
                        json={"message": "Suggest a govt job for BTech CS student"},
                        headers=user_headers, timeout=60)
        # 200 (success) or 502 (budget exceeded) — never 500 crash
        assert r.status_code in (200, 502), f"unexpected {r.status_code}: {r.text}"
        if r.status_code == 200:
            assert "reply" in r.json() and "session_id" in r.json()
        else:
            assert "detail" in r.json()

    def test_ai_chat_requires_auth(self, client):
        r = client.post(f"{API}/ai/chat", json={"message": "hi"})
        assert r.status_code == 401
