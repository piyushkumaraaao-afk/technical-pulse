"""Shared pytest fixtures for CareerPulse backend tests."""
import os
import uuid
import requests
import pytest
from pathlib import Path
from dotenv import load_dotenv

# Load frontend .env for the public URL used by the app
load_dotenv(Path(__file__).resolve().parents[2] / "frontend" / ".env")

BASE_URL = os.environ.get("EXPO_PUBLIC_BACKEND_URL", "https://careerpulse-13.preview.emergentagent.com").rstrip("/")
API = f"{BASE_URL}/api"

ADMIN_EMAIL = "admin@careerpulse.in"
ADMIN_PASSWORD = "Admin@123"


@pytest.fixture(scope="session")
def api_base() -> str:
    return API


@pytest.fixture(scope="session")
def client() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})
    return s


@pytest.fixture(scope="session")
def admin_token(client) -> str:
    r = client.post(f"{API}/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}, timeout=30)
    assert r.status_code == 200, f"Admin login failed: {r.status_code} {r.text}"
    return r.json()["access_token"]


@pytest.fixture(scope="session")
def admin_headers(admin_token) -> dict:
    return {"Authorization": f"Bearer {admin_token}", "Content-Type": "application/json"}


@pytest.fixture(scope="session")
def test_user(client) -> dict:
    """Register (or login) a unique test user for this session."""
    email = f"TEST_user_{uuid.uuid4().hex[:8]}@careerpulse.in".lower()
    password = "Test@1234"
    r = client.post(f"{API}/auth/register", json={"email": email, "password": password, "name": "Test User"}, timeout=30)
    assert r.status_code == 200, f"Register failed: {r.status_code} {r.text}"
    body = r.json()
    return {"email": email, "password": password, "token": body["access_token"], "user": body["user"]}


@pytest.fixture(scope="session")
def user_headers(test_user) -> dict:
    return {"Authorization": f"Bearer {test_user['token']}", "Content-Type": "application/json"}
