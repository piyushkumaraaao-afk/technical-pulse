"""CareerPulse Backend - Job alert app for Diploma/BTech Indian engineering students."""
import os
import uuid
import logging
import asyncio
import feedparser
import requests
import json
import httpx
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional, Literal, Any
from fastapi.security import HTTPBearer
from fastapi import Query
from io import BytesIO
import random
import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from crawl4ai import AsyncWebCrawler
from urllib.parse import urljoin
from fastapi import BackgroundTasks, APIRouter, Depends


import jwt
import bcrypt
from fastapi import FastAPI, APIRouter, HTTPException, Depends, Request, status
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field, EmailStr
from apscheduler.schedulers.asyncio import AsyncIOScheduler


ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")


# Config
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "careerpulse")
JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-key-change-me")
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.environ.get("JWT_EXPIRE_MINUTES", 10080))
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "admin@careerpulse.com")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("careerpulse")

# Mongo
client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]
_push_client = None


GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile" 

ALLOWED_POST_TYPES = {
    "Job", "Admit Card", "Result", "Scholarship", "Apprenticeship",
    "Internship", "Upcoming Exam", "IGNORE"
}

# =======================
# Bulletproof Helper Functions
# =======================
def first_json_object(model_text: str) -> dict:
    model_text = model_text.strip()
    if model_text.startswith("```"):
        model_text = re.sub(r"^```(?:json)?\s*|\s*```$", "", model_text, flags=re.I)
    try:
        return json.loads(model_text)
    except json.JSONDecodeError:
        start, end = model_text.find("{"), model_text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(model_text[start:end + 1])
        return {}

def canonical_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/") or "/",
                       urlencode(query, doseq=True), ""))

def detect_post_type(title: str) -> str:
    lower = title.lower()
    if any(val in lower for val in ("syllabus", "answer key", "cut off", "cutoff")): return "IGNORE"
    if "result" in lower or "merit list" in lower: return "Result"
    if any(val in lower for val in ("admit card", "hall ticket", "call letter")): return "Admit Card"
    if "scholarship" in lower: return "Scholarship"
    if any(val in lower for val in ("exam date", "date sheet", "schedule")): return "Upcoming Exam"
    if "apprentice" in lower: return "Apprenticeship"
    if "internship" in lower: return "Internship"
    if any(val in lower for val in ("admission", "counseling", "entrance")): return "IGNORE"
    return "Job"

def looks_like_detail_link(url: str, title: str) -> bool:
    lower = url.lower()
    path = urlsplit(url).path.rstrip("/").lower()
    blocked = ("facebook.com", "twitter.com", "[x.com/](https://x.com/)", "instagram.com", "youtube.com", "t.me/", "privacy-policy", "terms-and-conditions", "contact-us", "about-us", "javascript:", "mailto:")
    category_paths = {"/latestjob", "/result", "/admitcard", "/syllabus", "/answerkey"}
    return (
        url.startswith(("https://", "http://"))
        and len(title.strip()) >= 10
        and path not in {"", "/"}
        and path not in category_paths
        and not any(b in lower for b in blocked)
    )

def as_int_or_none(value: Any) -> int | None:
    match = re.search(r"\d+", str(value))
    return int(match.group()) if match else None

def valid_iso_date(value: Any) -> str | None:
    if not value or str(value).upper() == "NA": return None
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (TypeError, ValueError):
        return None

def fallback_qualifications(title: str, summary: str, qualifications: list) -> list:
    if qualifications and len(qualifications) > 0 and qualifications[0] != "NA": 
        return qualifications
    text = f"{title} {summary}".lower()
    found = []
    for phrase, label in (("12th", "12th"), ("intermediate", "12th"), ("iti", "ITI"), ("10th", "10th"), ("diploma", "Diploma"), ("graduate", "Graduate"), ("b.tech", "B.Tech"), ("btech", "B.Tech")):
        if phrase in text and label not in found: found.append(label)
    return found or ["Not Specified"]

# =======================
# Scrapers & Ultra-Smart AI Logic
# =======================
async def source_entries(src: dict, client: httpx.AsyncClient) -> list[dict]:
    response = await client.get(src["url"])
    response.raise_for_status()
    base_url = str(response.url)
    body = response.text
    is_xml = "<rss" in body[:500].lower() or "<?xml" in body[:500].lower()
    entries = []

    if is_xml:
        soup = BeautifulSoup(response.content, "xml")
        for item in soup.find_all("item")[:40]:
            title_tag, link_tag = item.find("title"), item.find("link")
            if not title_tag or not link_tag: continue
            entries.append({
                "title": title_tag.get_text(" ", strip=True),
                "link": canonical_url(urljoin(base_url, link_tag.get_text(strip=True))),
                "summary": item.find("description").get_text(" ", strip=True) if item.find("description") else "",
            })
        return entries

    soup = BeautifulSoup(body, "html.parser")
    anchors = soup.select("main a[href], article a[href], #post a[href], .post a[href]") or soup.find_all("a", href=True)
    seen = set()
    for anchor in anchors:
        title = anchor.get_text(" ", strip=True)
        link = canonical_url(urljoin(base_url, anchor["href"]))
        if link in seen or not looks_like_detail_link(link, title): continue
        seen.add(link)
        entries.append({"title": title, "link": link, "summary": f"Latest notification on {src['name']}."})
        if len(entries) >= 40: break
    return entries

async def extract_job_details_with_ai(url: str) -> dict:
    page_text = ""
    links_text = ""
    
    try:
        async with AsyncWebCrawler(verbose=False) as crawler:
            result = await crawler.arun(url=url, bypass_cache=True)
            page_text = result.markdown[:15000] if result.markdown else "" 
    except Exception as e:
        print(f"Crawl4AI failed for {url}: {e}")

    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        async with httpx.AsyncClient(timeout=15.0, headers=headers, follow_redirects=True) as client:
            response = await client.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            if not page_text:
                for tag in soup(["script", "style", "nav", "footer"]): tag.decompose()
                page_text = soup.get_text(separator=' | ', strip=True)[:15000]

            important_links = []
            for a in soup.find_all('a', href=True):
                link_url = canonical_url(urljoin(url, a['href']))
                if not link_url.startswith(("http", "https")): continue
                
                parent = a.find_parent(['tr', 'li', 'p', 'div'])
                context_text = parent.get_text(separator=' ', strip=True)[:100] if parent else a.get_text(strip=True)[:100]
                
                if any(k in context_text.lower() for k in ['apply', 'register', 'login', 'notification', 'pdf', 'admit card', 'result', 'official', 'click here']):
                    important_links.append(f"Context: [{context_text}] -> URL: {link_url}")
            links_text = "\n".join(important_links[:20])
    except Exception as e:
        print(f"Link extraction failed for {url}: {e}")

    # 🚀 THE ULTIMATE "ALL POSSIBLE WORDS" PROMPT
    prompt = f"""
    You are an elite Data Extraction AI. Analyze the Markdown content and Links from a recruitment website.
    
    CRITICAL RULES - SYNONYMS TO LOOK FOR:
    1. Organization: Look for 'Organization', 'Company', 'Board', 'Commission', 'Institution', 'Employer', 'Conducted By', 'Recruitment Board', 'Bank'.
    2. Post Name: Look for 'Post Name', 'Designation', 'Job Title', 'Position', 'Role', 'Name of the Post', 'Trade Name', 'Apprentice'.
    3. Salary: Look for 'Salary', 'Pay Scale', 'Stipend', 'Remuneration', 'CTC', 'Pay Level', 'Pay Matrix', 'Earnings', 'Wages', 'In Hand'.
    4. Age Limit: Look for 'Age Limit', 'Minimum Age', 'Maximum Age', 'Age as on', 'Age Relaxation', 'Umar', 'Ayoo'. (Extract MAX absolute age).
    5. Total Post: Look for 'Total Vacancies', 'No. of Post', 'Total Post', 'Number of Vacancies', 'Seat', 'Openings', 'Capacity'.
    6. Selection Process: Look for 'Selection Process', 'Recruitment Process', 'Exam Pattern', 'Stage', 'CBT', 'Written Exam', 'Interview', 'Physical', 'PET', 'PST', 'Skill Test', 'Document Verification', 'Medical'.
    7. Last Date: Look for 'Last Date', 'Apply Online Last Date', 'Closing Date', 'Deadline', 'Registration End Date', 'Antim Tithi', 'Valid Till'. (Format as YYYY-MM-DD).
    8. Qualifications: Look for 'Education Qualification', 'Eligibility', 'Academic Criteria', 'Essential Qualification', 'Passed', 'Degree', 'Diploma', 'ITI', '10th', '12th', 'B.Tech', 'Graduation'.
    9. Location: Look for 'Job Location', 'Posting', 'Place of Posting', 'State', 'City', 'All India'.
    10. Category/State Vacancies: Look for 'UR', 'Gen', 'Unreserved', 'OBC', 'EWS', 'SC', 'ST', 'PwD', 'Ex-Servicemen' or state names.

    JSON SCHEMA TO RETURN (Strictly use these keys, map the synonyms to these keys):
    {{
      "post_name": "Exact extracted Title/Role",
      "organization": "Exact conducting body/company",
      "category": "Choose ONE: ['Government', 'PSU', 'Private']",
      "post_type": "Choose ONE: ['Job', 'Admit Card', 'Result', 'Scholarship', 'Apprenticeship', 'Internship', 'Upcoming Exam', 'IGNORE']",
      "total_post": "Number only (e.g., 6557)",
      "category_vacancies": {{ "General": "NA", "OBC": "NA", "EWS": "NA", "SC": "NA", "ST": "NA" }},
      "state_wise_vacancies": [ {{ "state_name": "State", "vacancies": "Number" }} ],
      "multiple_posts": [
         {{ "post_name": "Specific Post Name OR Trade (e.g., Civil, Fitter)", "vacancies": "Number", "eligibility": "Qualification for this specific post" }}
      ],
      "mode_of_selection": ["Array of stages"],
      "min_age": "Minimum age (number only)",
      "max_age": "Maximum age (number only)",
      "salary": "Salary or Pay scale or Stipend or NA",
      "qualifications": ["B.Tech", "Diploma", "10th Pass", "12th Pass", "ITI", "Graduate", "PG"],
      "branches": ["Computer Science", "Mechanical", "Civil", "Electrical", "Electronics", "Fitter", "Welder", "Electrician"],
      "location": "City or State",
      "last_date": "YYYY-MM-DD",
      "check_official_notice": "Exact Notification URL from LINKS.",
      "apply_online_link": "Exact Apply URL from LINKS."
    }}
    
    --- DATA TO ANALYZE ---
    {page_text}
    
    --- IMPORTANT LINKS WITH CONTEXT ---
    {links_text}
    """

    try:
        await asyncio.sleep(4) 
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": GROQ_MODEL,
            "messages": [
                {"role": "system", "content": "You are a master data extractor. Understand synonyms carefully and map them to the exact JSON structure. Do NOT hallucinate."},
                {"role": "user", "content": prompt}
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.1 
        }
        async with httpx.AsyncClient(timeout=30.0) as ai_client:
            resp = await ai_client.post(GROQ_URL, headers=headers, json=payload)
            resp.raise_for_status()
            return first_json_object(resp.json()['choices'][0]['message']['content'])
    except Exception as e:
        print(f"Groq Extraction Error for {url}: {e}")
        return {"post_type": "NA"}


# =======================
# Main Processing Engine
# =======================
async def refresh_jobs_task() -> None:
    """Changed to return nothing, as it runs entirely in background now."""
    print("Background Scraping Started...")
    added = 0
    today_str = date.today().isoformat()

    result = await db.jobs.update_many(
        {"is_active": True, "last_date": {"$type": "string", "$lt": today_str}},
        {"$set": {"is_active": False}},
    )
    removed = result.modified_count
    sources = await db.rss_sources.find({}, {"_id": 0}).to_list(50)

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    timeout = httpx.Timeout(connect=15.0, read=35.0, write=20.0, pool=15.0)
    
    async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
        for src in sources:
            try:
                entries = await source_entries(src, client)
            except Exception as exc:
                print(f"Source fetch failed for {src.get('name')}: {exc}")
                continue

            for entry in entries:
                job_link = canonical_url(entry["link"])
                existing = await db.jobs.find_one(
                    {"$or": [{"source_url": job_link}, {"apply_link": job_link}]},
                    {"_id": 1},
                )
                if existing:
                    continue

                title, summary = entry["title"], entry["summary"]
                title_type = detect_post_type(title)
                if title_type == "IGNORE":
                    continue

                print(f"Deep scraping [{title_type}]: {title}")
                details = await extract_job_details_with_ai(job_link)
                
                if details.get("post_type") == "IGNORE":
                    continue

                post_type = details.get("post_type", "NA")
                if post_type not in ALLOWED_POST_TYPES or post_type == "NA":
                    post_type = title_type

                post_name = details.get("post_name", "NA")
                if post_name == "NA" or not post_name:
                    post_name = title

                extracted_apply = details.get("apply_online_link", "NA")
                action_link = extracted_apply if extracted_apply != "NA" else job_link
                
                exact_date = valid_iso_date(details.get("last_date"))
                final_last_date = exact_date if exact_date else (date.today() + timedelta(days=30)).isoformat()

                await db.jobs.insert_one({
                    "job_id": f"job_{uuid.uuid4().hex[:12]}",
                    "source_url": job_link,
                    "organization": details.get("organization", "NA") if details.get("organization", "NA") != "NA" else src["name"],
                    "post_name": post_name,
                    "post_type": post_type,
                    "category": details.get("category", "NA") if details.get("category", "NA") != "NA" else src.get("default_category", "Government"),
                    
                    "qualifications": fallback_qualifications(title, summary, details.get("qualifications", [])),
                    "branches": details.get("branches", []),
                    "vacancies": details.get("total_post", "NA"),
                    "salary": details.get("salary", "pay_scale", "NA"),
                    "eligibility": details.get("eligibility", "NA") if details.get("eligibility", "NA") != "NA" else summary,
                    
                    "category_vacancies": details.get("category_vacancies", {}),
                    "multiple_posts": details.get("multiple_posts", []),
                    "state_wise_vacancies": details.get("state_wise_vacancies", []),
                    "mode_of_selection": details.get("mode_of_selection", []),
                    "location": details.get("location", "NA"),
                    
                    "last_date": final_last_date,
                    "is_exact_date": bool(exact_date), 
                    
                    "notification_pdf": details.get("check_official_notice") if details.get("check_official_notice") != "NA" else None,
                    "apply_link": action_link,
                    "min_age": as_int_or_none(details.get("min_age")),
                    "max_age": as_int_or_none(details.get("max_age")),
                    "description": summary,
                    "is_active": True,
                    "source": f"scraper:{src['name']}",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })
                added += 1

    print(f"✅ Scraping cycle finished! +{added} added, {removed} expired")



# =======================
# Pydantic Models
# =======================
Qualification = Literal["Diploma", "BTech", "BE", "Final Year Student"]
Branch = Literal[
    "Civil Engineering",
    "Mechanical Engineering",
    "Electrical Engineering",
    "Electronics Engineering",
    "Computer Science",
]
JobCategory = Literal["Government", "PSU", "Apprenticeship", "Private", "Internship", "Diploma Eligible"]

class RegisterBody(BaseModel):
    email: EmailStr
    password: str
    name: str

class LoginBody(BaseModel):
    email: EmailStr
    password: str

class GoogleSessionBody(BaseModel):
    session_id: str

class ProfileUpdateBody(BaseModel):
    name: Optional[str] = None
    phone: Optional[int] = None
    qualification: Optional[Qualification] = None
    branch: Optional[Branch] = None
    passout_year: Optional[int] = None
    state: Optional[str] = None
    age: Optional[int] = None
    avatar: Optional[str] = None

class JobBody(BaseModel):
    organization: str
    post_name: str
    post_type: str
    category: JobCategory
    branches: List[Branch]
    qualifications: List[Qualification]
    vacancies: Optional[str] = None
    salary: Optional[str] = None
    eligibility: str
    location: Optional[str] = None
    state: Optional[str] = None
    last_date: str  # ISO date
    notification_pdf: Optional[str] = None
    apply_link: str
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    description: Optional[str] = None
    logo_url: Optional[str] = None
    previous_year_cutoff: Optional[str] = None
    selection_process: Optional[str] = None
    important_dates: Optional[str] = None
    railway_zone: Optional[str] = None
    medical_standard: Optional[str] = None

class EligibilityCheckBody(BaseModel):
    job_id: str

class SaveJobBody(BaseModel):
    job_id: str

class ApplyJobBody(BaseModel):
    job_id: str

class ResumeBody(BaseModel):
    full_name: str
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    objective: Optional[str] = None
    education: List[dict] = Field(default_factory=list)
    experience: List[dict] = Field(default_factory=list)
    projects: List[dict] = Field(default_factory=list)
    skills: List[str] = Field(default_factory=list)
    certifications: List[str] = Field(default_factory=list)
    template: str = "modern"

class ChatBody(BaseModel):
    message: str
    session_id: Optional[str] = None

class RegisterPushBody(BaseModel):
    platform: str
    device_token: str

class RssSourceBody(BaseModel):
    name: str
    url: str
    default_category: JobCategory = "Government"

class AdminNotifyBody(BaseModel):
    title: str
    message: str
    action_url: Optional[str] = None
    branch: Optional[str] = None
    qualification: Optional[str] = None

class FeedbackBody(BaseModel):
    message: str

# =======================
# Auth Utilities
# =======================
security = HTTPBearer()

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode(), hashed.encode())
    except Exception:
        return False

def create_jwt(user_id: str) -> str:
    payload = {
        "sub": user_id,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def decode_jwt(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload.get("sub")
    except Exception:
        return None

async def get_current_user(request: Request, auth = Depends(security)) -> dict:
    token = auth.credentials
    # Try JWT first
    user_id = decode_jwt(token)
    if user_id:
        user = await db.users.find_one({"user_id": user_id}, {"_id": 0, "password_hash": 0})
        if user:
            return user

    raise HTTPException(status_code=401, detail="Invalid or expired token")

async def require_admin(user: dict = Depends(get_current_user)) -> dict:
    if not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

    raise HTTPException(status_code=401, detail="Invalid or expired token")

async def require_admin(user: dict = Depends(get_current_user)) -> dict:
    if not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


# =======================
# Push Helper
# =======================
async def send_push(recipients: List[str], data: dict, idempotency_key: Optional[str] = None) -> None:
    if not recipients or _push_client is None:
        return
    if "title" not in data or "message" not in data:
        return
    for chunk_start in range(0, len(recipients), 100):
        chunk = recipients[chunk_start:chunk_start + 100]
        payload: dict = {"recipients": chunk, "data": data}
        if idempotency_key:
            payload["$idempotency_key"] = f"{idempotency_key}-{chunk_start}"
        try:
            resp = await _push_client.post("/api/v1/push/trigger", json=payload)
            if resp.status_code >= 400:
                logger.warning(f"push trigger failed {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.warning(f"push trigger error: {e}")


# =======================
# App / Router
# =======================
app = FastAPI(title="CareerPulse API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
api = APIRouter(prefix="/api")


# 1. Main Base URL (http://127.0.0.1:8000/)
@app.get("/")
async def main_root():
    return {"status": "ok", "message": "CareerPulse Backend Running"}

@app.get("/admin/users")
async def get_all_users(admin = Depends(require_admin)):
    users_cursor = db.users.find({})
    users_list = []
    async for u in users_cursor:
        users_list.append({
            "user_id": str(u.get("user_id") or u.get("_id")), # Ensure ID hamesha jaye
            "name": u.get("name"),
            "email": u.get("email"),
            "branch": u.get("branch"),
            "qualification": u.get("qualification"),
            "state": u.get("state"),
            "is_premium": u.get("is_premium", False),
            "is_blocked": u.get("is_blocked", False),
            "avatar": u.get("avatar")
        })
    return {"users": users_list}


# 2. Server Health Check URL
@app.get("/health")
async def health():
    return {"status": "healthy"}

@api.get("/")
async def api_root():
    return {"app": "CareerPulse API", "status": "ok"}


# ---- Auth ----
@api.post("/auth/register")
async def register(body: RegisterBody):
    existing = await db.users.find_one({"email": body.email.lower()})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    user_id = f"user_{uuid.uuid4().hex[:12]}"
    doc = {
        "user_id": user_id,
        "email": body.email.lower(),
        "name": body.name,
        "phone": body.phone,
        "password_hash": hash_password(body.password),
        "auth_provider": "email",
        "is_admin": False,
        "qualification": None,
        "branch": None,
        "passout_year": None,
        "state": None,
        "age": None,
        "avatar": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.users.insert_one(doc)
    token = create_jwt(user_id)
    user_public = {k: v for k, v in doc.items() if k not in ("password_hash", "_id")}
    return {"access_token": token, "token_type": "bearer", "user": user_public}


@api.post("/auth/login")
async def login(body: LoginBody):
    user = await db.users.find_one({"email": body.email.lower()})
    if not user or not user.get("password_hash") or not verify_password(body.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token = create_jwt(user["user_id"])
    user_public = {k: v for k, v in user.items() if k not in ("password_hash", "_id")}
    return {"access_token": token, "token_type": "bearer", "user": user_public}


@api.get("/auth/me")
async def me(user: dict = Depends(get_current_user)):
    return {"user": user}


@api.post("/auth/logout")
async def logout(request: Request, user: dict = Depends(get_current_user)):
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        await db.user_sessions.delete_one({"session_token": auth[7:]})
    return {"ok": True}


@api.put("/auth/profile")
async def update_profile(body: ProfileUpdateBody, user: dict = Depends(get_current_user)):
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if updates:
        await db.users.update_one({"user_id": user["user_id"]}, {"$set": updates})
    updated = await db.users.find_one({"user_id": user["user_id"]}, {"_id": 0, "password_hash": 0})
    return {"user": updated}


# ---- Jobs ----
def _clean_job(job: dict) -> dict:
    job.pop("_id", None)
    return job


@app.get("/jobs")
async def get_all_jobs(limit: int = 100):
    jobs_cursor = db.jobs.find({}).limit(limit)
    jobs_list = []
    async for j in jobs_cursor:
        jobs_list.append({
            "job_id": str(j.get("job_id") or j.get("_id")), # Ensure job_id hamesha jaye
            "post_name": j.get("post_name"),
            "post_type": j.get("post_type", "Job"),
            "organization": j.get("organization"),
                       "is_trending": j.get("is_trending", False),
            # ... baaki fields
        })
    return {"jobs": jobs_list}

@api.get("/jobs")
async def list_jobs(
    category: Optional[str] = None,
    branch: Optional[str] = None,
    qualification: Optional[str] = None,
    location: Optional[str] = None,
    state: Optional[str] = None,
    age: Optional[int] = None,
    search: Optional[str] = None,
    post_type: Optional[str] = None, # 🚀 Naya parameter add kiya
    limit: int = 50,
):
    q: dict = {"is_active": True}
    
    # Agar frontend se post_type (jaise 'Job', 'Admit Card', etc.) aaya hai toh filter karein
    if post_type and post_type != "All":
        q["post_type"] = post_type
    elif not post_type:
        q["post_type"] = "Job"
        
    if category and category != "All":
        q["category"] = category
    if branch and branch != "All":
        q["branches"] = branch
    if qualification and qualification != "All":
        q["qualifications"] = qualification
    if location and location != "All":
        q["location"] = {"$regex": location, "$options": "i"}
    if state and state != "All":
        q["$or"] = [
            {"state": {"$regex": state, "$options": "i"}},
            {"location": {"$regex": state, "$options": "i"}},
        ]
    if age is not None:
        q["$and"] = [
            {"$or": [{"min_age": None}, {"min_age": {"$lte": age}}]},
            {"$or": [{"max_age": None}, {"max_age": {"$gte": age}}]},
        ]
    if search:
        q["$or"] = [
            {"post_name": {"$regex": search, "$options": "i"}},
            {"post_type": {"$regex": search, "$options": "i"}},
            {"organization": {"$regex": search, "$options": "i"}},
            {"location": {"$regex": search, "$options": "i"}},
        ]
        
    cursor = db.jobs.find(q, {"_id": 0}).sort("last_date", 1).limit(limit)
    jobs = await cursor.to_list(length=limit)
    return {"jobs": jobs, "count": len(jobs)}


@api.get("/jobs/recommended")
async def recommended_jobs(user: dict = Depends(get_current_user)):
    q: dict = {"is_active": True, "post_type": "Job"}
    if user.get("branch"):
        q["branches"] = user["branch"]
    if user.get("qualification"):
        q["qualifications"] = user["qualification"]
    cursor = db.jobs.find(q, {"_id": 0}).sort("last_date", 1).limit(10)
    jobs = await cursor.to_list(length=10)
    return {"jobs": jobs}


@api.get("/jobs/{job_id}")
async def get_job(job_id: str):
    job = await db.jobs.find_one({"job_id": job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job": job}


@api.post("/jobs/check-eligibility")
async def check_eligibility(body: EligibilityCheckBody, user: dict = Depends(get_current_user)):
    job = await db.jobs.find_one({"job_id": body.job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    reasons = []
    eligible = True

    if user.get("qualification") and job.get("qualifications"):
        if user["qualification"] not in job["qualifications"]:
            eligible = False
            reasons.append(f"Requires qualification: {', '.join(job['qualifications'])}")

    if user.get("branch") and job.get("branches"):
        if user["branch"] not in job["branches"]:
            eligible = False
            reasons.append(f"Requires branch: {', '.join(job['branches'])}")

    if user.get("age") is not None:
        if job.get("min_age") is not None and user["age"] < job["min_age"]:
            eligible = False
            reasons.append(f"Minimum age: {job['min_age']}")
        if job.get("max_age") is not None and user["age"] > job["max_age"]:
            eligible = False
            reasons.append(f"Maximum age: {job['max_age']}")

    if not user.get("qualification") or not user.get("branch"):
        reasons.append("Complete your profile for accurate check")

    return {
        "eligible": eligible,
        "reasons": reasons,
        "job_id": body.job_id,
    }


# ---- Application Tracker ----
@api.post("/applications/save")
async def save_job(body: SaveJobBody, user: dict = Depends(get_current_user)):
    job = await db.jobs.find_one({"job_id": body.job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    await db.applications.update_one(
        {"user_id": user["user_id"], "job_id": body.job_id},
        {"$set": {
            "user_id": user["user_id"],
            "job_id": body.job_id,
            "status": "saved",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }},
        upsert=True,
    )
    return {"ok": True}


@api.post("/applications/apply")
async def apply_job(body: ApplyJobBody, user: dict = Depends(get_current_user)):
    job = await db.jobs.find_one({"job_id": body.job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    await db.applications.update_one(
        {"user_id": user["user_id"], "job_id": body.job_id},
        {"$set": {
            "user_id": user["user_id"],
            "job_id": body.job_id,
            "status": "applied",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }},
        upsert=True,
    )
    return {"ok": True}


@api.get("/applications")
async def get_applications(user: dict = Depends(get_current_user)):
    apps = await db.applications.find({"user_id": user["user_id"]}, {"_id": 0}).to_list(500)
    job_ids = [a["job_id"] for a in apps]
    jobs_list = await db.jobs.find({"job_id": {"$in": job_ids}}, {"_id": 0}).to_list(500)
    jobs_map = {j["job_id"]: j for j in jobs_list}
    saved = []
    applied = []
    upcoming = []
    today = date.today().isoformat()
    week_later = (date.today() + timedelta(days=7)).isoformat()
    for a in apps:
        job = jobs_map.get(a["job_id"])
        if not job:
            continue
        item = {**a, "job": job}
        if a["status"] == "applied":
            applied.append(item)
        else:
            saved.append(item)
        ld = job.get("last_date", "")
        if today <= ld <= week_later:
            upcoming.append(item)
    return {"saved": saved, "applied": applied, "upcoming": upcoming}


@api.delete("/applications/{job_id}")
async def remove_application(job_id: str, user: dict = Depends(get_current_user)):
    await db.applications.delete_one({"user_id": user["user_id"], "job_id": job_id})
    return {"ok": True}


# ---- Resume ----
@api.post("/resumes")
async def save_resume(body: ResumeBody, user: dict = Depends(get_current_user)):
    resume_id = f"res_{uuid.uuid4().hex[:10]}"
    doc = {"resume_id": resume_id, "user_id": user["user_id"],
           **body.model_dump(), "created_at": datetime.now(timezone.utc).isoformat()}
    await db.resumes.insert_one(doc)
    doc.pop("_id", None)
    return {"resume": doc}


@api.get("/resumes")
async def get_resumes(user: dict = Depends(get_current_user)):
    resumes = await db.resumes.find({"user_id": user["user_id"]}, {"_id": 0}).to_list(50)
    return {"resumes": resumes}


@api.put("/resumes/{resume_id}")
async def update_resume(resume_id: str, body: ResumeBody, user: dict = Depends(get_current_user)):
    result = await db.resumes.update_one(
        {"resume_id": resume_id, "user_id": user["user_id"]},
        {"$set": body.model_dump()},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Resume not found")
    updated = await db.resumes.find_one({"resume_id": resume_id}, {"_id": 0})
    return {"resume": updated}


@api.delete("/resumes/{resume_id}")
async def delete_resume(resume_id: str, user: dict = Depends(get_current_user)):
    await db.resumes.delete_one({"resume_id": resume_id, "user_id": user["user_id"]})
    return {"ok": True}


# ---- AI Career Assistant ----
@api.post("/ai/chat")
async def ai_chat(body: ChatBody, user: dict = Depends(get_current_user)):
    session_id = body.session_id or f"chat_{user['user_id']}"
    profile_ctx = (
        f"Student profile: {user.get('name')}, Qualification: {user.get('qualification')}, Branch: {user.get('branch')}."
    )
    
    try:    
        groq_url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "llama3.1-8b-instant",
            "messages": [
                {"role": "system", "content": "You are CareerPulse Assistant, a helpful career advisor for students in India. Keep answers under 150 words."},
                {"role": "user", "content": f"Profile: {profile_ctx}\nQuestion: {body.message}"}
            ]
        }
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(groq_url, headers=headers, json=payload)
            if resp.status_code != 200:
                raise Exception("AI API Error")
            data = resp.json()
            reply = data['choices'][0]['message']['content']
            
    except Exception as e:
        logger.exception("AI chat failed")
        raise HTTPException(status_code=502, detail=f"AI service error: {e}")

    await db.chat_messages.insert_one({
        "user_id": user["user_id"],
        "session_id": session_id,
        "user_message": body.message,
        "assistant_message": reply,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    
    return {"reply": reply, "session_id": session_id}


@api.get("/ai/history")
async def ai_history(session_id: Optional[str] = None, user: dict = Depends(get_current_user)):
    sid = session_id or f"chat_{user['user_id']}"
    msgs = await db.chat_messages.find(
        {"user_id": user["user_id"], "session_id": sid}, {"_id": 0}
    ).sort("created_at", 1).to_list(200)
    return {"messages": msgs, "session_id": sid}


# ---- Push ----
@api.post("/register-push", status_code=201)
async def register_push(body: RegisterPushBody, user: dict = Depends(get_current_user)):
    await db.push_devices.update_one(
        {"user_id": user["user_id"], "device_token": body.device_token},
        {"$set": {
            "user_id": user["user_id"],
            "platform": body.platform,
            "device_token": body.device_token,
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }},
        upsert=True,
    )
    return {"status": "registered"}


# ---- Admin ----
# Yeh aapki backend API file hogi (e.g., main.py ya admin.py)

@api.post("/admin/jobs")
async def create_admin_job(data: dict): # Ya jo bhi aapka Pydantic schema ho
    
    # 🚀 STEP 1: Frontend se aa raha post_type get karein (Agar na aaye toh 'Job' default set kardein)
    post_type = data.get("post_type", "Job")
    ai_details = await extract_job_details_with_ai(job_url)

    job_id = f"job_{uuid.uuid4().hex[:12]}"

    # Database Entry
    new_post = {
        "job_id": job_id,
        "organization": data.get("organization"),
        "post_name": data.get("post_name"),
        "post_type": data.get("post_type", "Job"),
        "category": data.get("category", "Government"),
        
        # 🚀 STEP 2: YAHAN POST TYPE KO DATABASE MEIN SAVE KARAYEIN 
        
        "branches": data.get("branches", []),
        "qualifications": data.get("qualifications", []),
        "vacancies": data.get("vacancies", "NA"),
        "salary": data.get("salary", "NA"),
        "eligibility": data.get("eligibility", ""),
        "location": data.get("location", "India"),
        "last_date": data.get("last_date"),
        "apply_link": data.get("apply_link"),
        "notification_pdf": data.get("notification_pdf"),
        "min_age": data.get("min_age"),
        "max_age": data.get("max_age"),
        "description": data.get("description"),
        "is_active": True,
        "is_trending": False # Default
    }

    await db.jobs.insert_one(new_post)
    return {"message": "Post created successfully"}


# 1. Trending status change karne ke liye API
# 🚀 NAYA: Admin Jobs Status (Trending/Active) Update karne ke liye API
@api.patch("/admin/jobs/{job_id}")
async def update_job_status(job_id: str, request: Request, admin: dict = Depends(require_admin)):
    data = await request.json()
    
    update_data = {}
    if "is_trending" in data:
        update_data["is_trending"] = data["is_trending"]
    if "is_active" in data:
        update_data["is_active"] = data["is_active"]
        
    query = {"$or": [{"job_id": job_id}]}
    if ObjectId.is_valid(job_id):
        query["$or"].append({"_id": ObjectId(job_id)})

    result = await db.jobs.update_one(query, {"$set": update_data})
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return {"success": True, "message": "Job status updated successfully"}


# 2. User ko Premium aur Block karne ke liye API
from bson import ObjectId

@app.patch("/admin/users/{user_id}")
async def update_user_status(user_id: str, request: Request):
    data = await request.json()
    
    update_data = {}
    if "is_premium" in data:
        update_data["is_premium"] = data["is_premium"]
    if "is_blocked" in data:
        update_data["is_blocked"] = data["is_blocked"]
        
    # 💡 Smart Query: Chahe user_id match kare ya MongoDB ki _id, dono ko check karega
    query = {"$or": [{"user_id": user_id}]}
    if ObjectId.is_valid(user_id):
        query["$or"].append({"_id": ObjectId(user_id)})

    result = await db.users.update_one(query, {"$set": update_data})
    
    if result.modified_count == 0:
        return {"success": False, "message": "User not found"}
        
    return {"success": {"message": "User updated successfully"}}

@api.put("/admin/jobs/{job_id}")
async def admin_update_job(job_id: str, request: Request, admin: dict = Depends(require_admin)):
    data = await request.json()
    
    # Smart query jo job_id aur _id dono check karegi
    query = {"$or": [{"job_id": job_id}]}
    if ObjectId.is_valid(job_id):
        query["$or"].append({"_id": ObjectId(job_id)})

    result = await db.jobs.update_one(query, {"$set": data})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
        
    updated = await db.jobs.find_one(query, {"_id": 0})
    return {"job": updated}


@api.delete("/admin/jobs/{job_id}")
async def admin_delete_job(job_id: str, admin: dict = Depends(require_admin)):
    await db.jobs.delete_one({"job_id": job_id})
    return {"ok": True}


@api.get("/admin/users")
async def admin_list_users(admin: dict = Depends(require_admin)):
    users = await db.users.find({}, {"_id": 0, "password_hash": 0}).to_list(500)
    return {"users": users, "count": len(users)}


@api.get("/admin/stats")
async def admin_stats(admin: dict = Depends(require_admin)):
    users_count = await db.users.count_documents({})
    jobs_count = await db.jobs.count_documents({"is_active": True})
    apps_count = await db.applications.count_documents({})
    return {"users": users_count, "active_jobs": jobs_count, "applications": apps_count}


@api.post("/admin/notify")
async def admin_notify(body: AdminNotifyBody, admin: dict = Depends(require_admin)):
    q: dict = {}
    if body.branch:
        q["branch"] = body.branch
    if hasattr(body, 'qualification') and body.qualification:
        q["qualification"] = body.qualification
    
    users = await db.users.find(q, {"_id": 0, "user_id": 1}).to_list(1000)
    recipients = [u["user_id"] for u in users]
    data: dict = {"title": body.title, "message": body.message}
    if body.action_url:
        data["action_url"] = body.action_url
    await send_push(recipients=recipients, data=data,
                    idempotency_key=f"admin-notify-{uuid.uuid4().hex[:8]}")
    return {"ok": True, "recipients_count": len(recipients)}


@api.post("/admin/rss-sources")
async def admin_add_rss(body: RssSourceBody, admin: dict = Depends(require_admin)):
    src_id = f"rss_{uuid.uuid4().hex[:8]}"
    await db.rss_sources.insert_one({
        "src_id": src_id, "name": body.name, "url": body.url,
        "default_category": body.default_category,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    return {"src_id": src_id}


@api.get("/admin/rss-sources")
async def admin_list_rss(admin: dict = Depends(require_admin)):
    sources = await db.rss_sources.find({}, {"_id": 0}).to_list(100)
    return {"sources": sources}


@api.delete("/admin/rss-sources/{src_id}")
async def admin_delete_rss(src_id: str, admin: dict = Depends(require_admin)):
    await db.rss_sources.delete_one({"src_id": src_id})
    return {"ok": True}


@api.post("/admin/refresh-jobs")
async def admin_refresh_jobs(background_tasks: BackgroundTasks, admin: dict = Depends(require_admin)):
    # Task background me daal diya (No 502 Error!)
    background_tasks.add_task(refresh_jobs_task)
    
    # Turant 0 bhej diya taaki frontend crash/undefined na ho. 
    # Asli data 5 min baad DB me khud update ho jayega.
    return {
        "added": 0, 
        "removed": 0, 
        "message": "Scraping has started in the background successfully."
    }

# =======================
# Feedback & User Management 
# =======================
@api.delete("/admin/users/{target_user_id}")
async def admin_delete_user(target_user_id: str, admin: dict = Depends(require_admin)):
    await db.users.delete_one({"user_id": target_user_id})
    return {"ok": True, "msg": "User deleted successfully"}

@api.post("/feedback")
async def submit_feedback(body: FeedbackBody, user: dict = Depends(get_current_user)):
    await db.feedback.insert_one({
        "user_id": user["user_id"],
        "name": user.get("name", "Unknown"),
        "email": user.get("email", ""),
        "message": body.message,
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    return {"ok": True}

@api.get("/admin/feedback")
async def get_admin_feedback(admin: dict = Depends(require_admin)):
    feedbacks = await db.feedback.find({}, {"_id": 0}).sort("created_at", -1).to_list(100)
    return {"feedbacks": feedbacks}


# =======================
# Startup / Seed
# =======================
scheduler = AsyncIOScheduler()

async def seed_admin():
    existing = await db.users.find_one({"email": ADMIN_EMAIL.lower()})
    admin_hash = hash_password(ADMIN_PASSWORD)
    if existing:
        await db.users.update_one(
            {"email": ADMIN_EMAIL.lower()},
            {"$set": {"is_admin": True, "password_hash": admin_hash, "auth_provider": "email"}},
        )
    else:
        await db.users.insert_one({
            "user_id": f"user_{uuid.uuid4().hex[:12]}",
            "email": ADMIN_EMAIL.lower(),
            "name": "CareerPulse Admin",
            "password_hash": admin_hash,
            "auth_provider": "email",
            "is_admin": True,
            "qualification": None,
            "branch": None,
            "passout_year": None,
            "state": None,
            "age": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

@app.on_event("startup")
async def startup_event():
    global _push_client
    _push_client = httpx.AsyncClient(base_url="https://push-service-placeholder.com")
    await seed_admin()
    # 🚀 Yahan purane naam ko naye background engine se replace kar diya gaya hai
    scheduler.add_job(refresh_jobs_task, 'interval', hours=12)
    scheduler.start()
    logger.info("CareerPulse Background Services Started Successfully")

@app.on_event("shutdown")
async def shutdown_event():
    global _push_client
    if _push_client:
        await _push_client.aclose()
    scheduler.shutdown()

app.include_router(api)


SAMPLE_JOBS = [
    {
        "organization": "Indian Railways (RRB)",
        "post_name": "Junior Engineer (JE) - Civil, Mechanical, Electrical",
        "category": "Government",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering"],
        "qualifications": ["Diploma", "BTech", "BE"],
        "vacancies": "7951",
        "salary": "₹35,400 - ₹1,12,400 (Level 6)",
        "eligibility": "Diploma or BE/BTech in relevant engineering. Age 18-33 years.",
        "location": "All India",
        "last_date": (date.today() + timedelta(days=25)).isoformat(),
        "notification_pdf": "https://www.rrbcdg.gov.in/",
        "apply_link": "https://www.rrbapply.gov.in",
        "min_age": 18, "max_age": 33,
        "description": "Railway Recruitment Board notification for Junior Engineer posts across zones. Written CBT followed by document verification.",
    },
    {
        "organization": "Bharat Heavy Electricals Limited (BHEL)",
        "post_name": "Engineer Trainee - Mechanical",
        "category": "PSU",
        "branches": ["Mechanical Engineering"],
        "qualifications": ["BTech", "BE"],
        "vacancies": "400",
        "salary": "₹60,000 - ₹1,80,000",
        "eligibility": "BTech/BE Mechanical with min 60% marks. Age 18-28 years.",
        "location": "Bhopal, Trichy, Haridwar",
        "last_date": (date.today() + timedelta(days=18)).isoformat(),
        "notification_pdf": "https://bhel.com/careers",
        "apply_link": "https://careers.bhel.in",
        "min_age": 18, "max_age": 28,
        "description": "BHEL recruits Engineer Trainees via GATE scores. Excellent PSU career track with all-India postings.",
    },
    {
        "organization": "ISRO (VSSC)",
        "post_name": "Technical Assistant - Electronics",
        "category": "Government",
        "branches": ["Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma"],
        "vacancies": "56",
        "salary": "₹35,400 - ₹1,12,400",
        "eligibility": "First class Diploma in Electronics/Computer/EEE. Age max 35.",
        "location": "Thiruvananthapuram",
        "last_date": (date.today() + timedelta(days=12)).isoformat(),
        "notification_pdf": "https://www.isro.gov.in/careers.html",
        "apply_link": "https://apps.vssc.gov.in/recruitment",
        "min_age": 18, "max_age": 35,
        "description": "Work with India's space program. Multiple technical assistant roles across ISRO centres.",
    },
    {
        "organization": "NTPC Limited",
        "post_name": "Assistant Engineer (Trainee) - Electrical",
        "category": "PSU",
        "branches": ["Electrical Engineering", "Electronics Engineering"],
        "qualifications": ["BTech", "BE"],
        "vacancies": "230",
        "salary": "₹50,000 - ₹1,60,000",
        "eligibility": "BE/BTech Electrical/EEE with 65% marks. GATE 2025 valid.",
        "location": "Pan India",
        "last_date": (date.today() + timedelta(days=30)).isoformat(),
        "notification_pdf": "https://ntpc.co.in/careers",
        "apply_link": "https://recruitment.ntpc.co.in",
        "min_age": 18, "max_age": 27,
        "description": "India's largest power producer hiring Assistant Engineer Trainees. Stable PSU career with excellent perks.",
    },
    {
        "organization": "TCS Digital",
        "post_name": "Systems Engineer - Digital Hire",
        "category": "Private",
        "branches": ["Computer Science", "Electronics Engineering"],
        "qualifications": ["BTech", "BE", "Final Year Student"],
        "vacancies": "5000+",
        "salary": "₹7 LPA - ₹9 LPA",
        "eligibility": "BE/BTech CSE/IT/ECE 2025/2026 batch, 60% throughout.",
        "location": "PAN India",
        "last_date": (date.today() + timedelta(days=20)).isoformat(),
        "notification_pdf": "https://www.tcs.com/careers",
        "apply_link": "https://ibegin.tcs.com",
        "min_age": 18, "max_age": 28,
        "description": "TCS NQT-based hiring for Digital profile. Coding + aptitude + interview. Global exposure & rapid growth.",
    },
    {
        "organization": "NATS (National Apprenticeship Training Scheme)",
        "post_name": "Graduate Apprentice - All Branches",
        "category": "Apprenticeship",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering",
                     "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "BTech", "BE", "Final Year Student"],
        "vacancies": "10000+",
        "salary": "₹9,000 - ₹15,000 stipend",
        "eligibility": "Fresh Engineering graduates or diploma holders. 1-year training.",
        "location": "All India (host industry based)",
        "last_date": (date.today() + timedelta(days=45)).isoformat(),
        "notification_pdf": "https://nats.education.gov.in",
        "apply_link": "https://nats.education.gov.in/apprentice_login.php",
        "min_age": 18, "max_age": 30,
        "description": "Government-backed apprenticeship in reputed PSUs and private firms. Certificate + industry experience.",
    },
    {
        "organization": "Infosys Ltd",
        "post_name": "Specialist Programmer - InfyTQ",
        "category": "Private",
        "branches": ["Computer Science", "Electronics Engineering"],
        "qualifications": ["BTech", "BE", "Final Year Student"],
        "vacancies": "2000",
        "salary": "₹9 LPA",
        "eligibility": "CS/IT/ECE 2025-26 batch. Clear InfyTQ certification.",
        "location": "Bengaluru, Pune, Hyderabad",
        "last_date": (date.today() + timedelta(days=15)).isoformat(),
        "notification_pdf": "https://www.infosys.com/careers.html",
        "apply_link": "https://infytq.onwingspan.com",
        "min_age": 18, "max_age": 27,
        "description": "Elite programmer role for InfyTQ certified candidates. Higher package, priority projects.",
    },
    {
        "organization": "DRDO (Defence Research)",
        "post_name": "Junior Research Fellow (JRF)",
        "category": "Government",
        "branches": ["Mechanical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["BTech", "BE"],
        "vacancies": "120",
        "salary": "₹37,000/month stipend",
        "eligibility": "BE/BTech with 60% + GATE. Age max 28.",
        "location": "New Delhi, Hyderabad, Bengaluru",
        "last_date": (date.today() + timedelta(days=22)).isoformat(),
        "notification_pdf": "https://drdo.gov.in/careers",
        "apply_link": "https://rac.gov.in",
        "min_age": 18, "max_age": 28,
        "description": "Work on cutting-edge defense R&D. 2-year JRF tenure with possibility of PhD registration.",
    },
    {
        "organization": "Wipro Elite NTH",
        "post_name": "Project Engineer",
        "category": "Private",
        "branches": ["Computer Science", "Electronics Engineering", "Electrical Engineering"],
        "qualifications": ["BTech", "BE", "Final Year Student"],
        "vacancies": "3000",
        "salary": "₹3.5 LPA - ₹6.5 LPA",
        "eligibility": "60% throughout, 2025-26 batch, no active backlogs.",
        "location": "PAN India",
        "last_date": (date.today() + timedelta(days=10)).isoformat(),
        "notification_pdf": "https://careers.wipro.com",
        "apply_link": "https://careers.wipro.com/elite-nth",
        "min_age": 18, "max_age": 26,
        "description": "Wipro Elite National Talent Hunt. Cross-domain project engineer roles across India.",
    },
    {
        "organization": "SAIL (Steel Authority of India)",
        "post_name": "Management Trainee (Technical)",
        "category": "PSU",
        "branches": ["Mechanical Engineering", "Electrical Engineering", "Civil Engineering"],
        "qualifications": ["BTech", "BE"],
        "vacancies": "391",
        "salary": "₹50,000 - ₹1,60,000",
        "eligibility": "BE/BTech with 65% + GATE 2025 valid score.",
        "location": "Bhilai, Bokaro, Durgapur, Rourkela",
        "last_date": (date.today() + timedelta(days=35)).isoformat(),
        "notification_pdf": "https://www.sail.co.in/careers",
        "apply_link": "https://sailcareers.com",
        "min_age": 18, "max_age": 28,
        "description": "Join India's largest steel maker as MT. Rotational training + fast-track promotion path.",
    },
    {
        "organization": "Google India",
        "post_name": "STEP Intern - Software Engineering",
        "category": "Internship",
        "branches": ["Computer Science"],
        "qualifications": ["BTech", "BE", "Final Year Student"],
        "vacancies": "150",
        "salary": "₹1.2L / month stipend",
        "eligibility": "1st or 2nd year BTech CSE. Strong DSA basics.",
        "location": "Bengaluru, Hyderabad, Remote",
        "last_date": (date.today() + timedelta(days=8)).isoformat(),
        "notification_pdf": "https://buildyourfuture.withgoogle.com/programs/step",
        "apply_link": "https://careers.google.com/students",
        "min_age": 18, "max_age": 22,
        "description": "12-week paid internship at Google. Mentorship + real code + potential return offer.",
    },
    {
        "organization": "L&T Construction",
        "post_name": "Graduate Engineer Trainee - Civil",
        "category": "Private",
        "branches": ["Civil Engineering"],
        "qualifications": ["BTech", "BE"],
        "vacancies": "500",
        "salary": "₹6.5 LPA",
        "eligibility": "BE/BTech Civil 2024-25 batch, 60%+ marks.",
        "location": "PAN India project sites",
        "last_date": (date.today() + timedelta(days=14)).isoformat(),
        "notification_pdf": "https://www.larsentoubro.com/corporate/careers/",
        "apply_link": "https://www.lntecc.com/careers",
        "min_age": 18, "max_age": 27,
        "description": "GET program with L&T ECC. Work on mega infrastructure projects across India.",
    },
    # ===================== DIPLOMA ELIGIBLE JOBS =====================
    {
        "organization": "SSC (Staff Selection Commission)", "post_name": "SSC CHSL (10+2) - LDC/JSA/PA/SA",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "3712",
        "salary": "₹19,900 - ₹63,200 (Pay Level 2/4)",
        "eligibility": "Passed 12th (or Diploma). Age 18-27 years.",
        "location": "All India", "state": "All India",
        "last_date": (date.today() + timedelta(days=20)).isoformat(),
        "notification_pdf": "https://ssc.gov.in", "apply_link": "https://ssc.gov.in/registration",
        "min_age": 18, "max_age": 27,
        "description": "Combined Higher Secondary Level exam for Lower Division Clerk, Postal Assistant, Data Entry Operator posts in Central government ministries.",
        "selection_process": "Tier 1 (CBT) → Tier 2 (Descriptive + Skill Test) → Document Verification",
        "important_dates": "Application: Now open • Tier 1: Next month • Result: 3 months",
        "previous_year_cutoff": "General: 158.5 | OBC: 148.2 | SC: 138.5 | ST: 125.2",
    },
    {
        "organization": "SSC (Staff Selection Commission)", "post_name": "SSC MTS (Multi Tasking Staff) & Havaldar",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "8326",
        "salary": "₹18,000 - ₹56,900 (Pay Level 1)",
        "eligibility": "Passed 10th. Age 18-25 (MTS), 18-27 (Havaldar).",
        "location": "All India", "state": "All India",
        "last_date": (date.today() + timedelta(days=15)).isoformat(),
        "notification_pdf": "https://ssc.gov.in", "apply_link": "https://ssc.gov.in/registration",
        "min_age": 18, "max_age": 27,
        "description": "MTS in central government offices + Havaldar in CBIC/CBN. Non-technical, stable Group C job.",
        "selection_process": "Session 1 (CBT: Numerical & Reasoning) → Session 2 (English + GK) → PET/PST (Havaldar only)",
        "important_dates": "Apply: Now • CBT: 45 days • Final: 4 months",
        "previous_year_cutoff": "General: 130.5 | OBC: 122.4 | SC: 110.2 | ST: 100.5",
    },
    {
        "organization": "SSC (Staff Selection Commission)", "post_name": "SSC JE (Junior Engineer) - Civil/Mech/Elec",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering"],
        "qualifications": ["Diploma", "BTech", "BE"], "vacancies": "1765",
        "salary": "₹35,400 - ₹1,12,400 (Level 6)",
        "eligibility": "Diploma/BE/BTech in Civil/Mech/Elec Engineering. Age 18-32.",
        "location": "All India", "state": "All India",
        "last_date": (date.today() + timedelta(days=28)).isoformat(),
        "notification_pdf": "https://ssc.gov.in", "apply_link": "https://ssc.gov.in/registration",
        "min_age": 18, "max_age": 32,
        "description": "Junior Engineer roles in CPWD, MES, BRO, CWC and Farakka Barrage. High-paying Group B post-Diploma job.",
        "selection_process": "Paper 1 (Objective CBT) → Paper 2 (Technical Descriptive) → Document Verification",
        "important_dates": "Apply: Now • Paper 1: 60 days • Paper 2: 4 months",
        "previous_year_cutoff": "Civil — Gen: 250 | OBC: 240 | SC: 210 | Mech — Gen: 245 | Elec — Gen: 260 (out of 400)",
    },
    {
        "organization": "Railway Recruitment Board (RRB)", "post_name": "RRB NTPC (Non-Technical Popular Categories)",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "BTech", "BE", "Final Year Student"], "vacancies": "11558",
        "salary": "₹19,900 - ₹35,400 (Undergraduate) / ₹35,400 - ₹1,12,400 (Graduate)",
        "eligibility": "12th pass or Graduate. Age 18-33.",
        "location": "All India (Railway zones)", "state": "All India",
        "last_date": (date.today() + timedelta(days=17)).isoformat(),
        "notification_pdf": "https://rrbcdg.gov.in", "apply_link": "https://www.rrbapply.gov.in",
        "min_age": 18, "max_age": 33,
        "description": "Clerks, Junior Accounts Assistants, Station Masters, Traffic Assistants, Commercial Apprentices — Indian Railways NTPC posts.",
        "selection_process": "CBT 1 → CBT 2 → Typing Skill Test / Aptitude → Document Verification → Medical",
        "important_dates": "Apply: Now • CBT 1: 3 months • Final: 8-10 months",
        "previous_year_cutoff": "Graduate Gen: 80.5 | OBC: 76.2 | SC: 68.5 | ST: 61.5 (Normalized)",
    },
    {
        "organization": "Railway Recruitment Board (RRB)", "post_name": "RRB JE (Junior Engineer) - All Branches",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "BTech", "BE"], "vacancies": "7951",
        "salary": "₹35,400 - ₹1,12,400 (Level 6)",
        "eligibility": "Diploma or BE/BTech in relevant engineering. Age 18-33.",
        "location": "All India (Railway zones)", "state": "All India",
        "last_date": (date.today() + timedelta(days=25)).isoformat(),
        "notification_pdf": "https://rrbcdg.gov.in", "apply_link": "https://www.rrbapply.gov.in",
        "min_age": 18, "max_age": 33,
        "description": "Junior Engineer, Depot Material Superintendent, Chemical & Metallurgical Assistant posts. Prestigious Group C railway job.",
        "selection_process": "CBT 1 → CBT 2 (Technical + General) → Document Verification → Medical",
        "important_dates": "Apply: Now • CBT 1: 60 days • CBT 2: 4 months",
        "previous_year_cutoff": "General: 82.5 | OBC: 78.2 | SC: 70.1 | ST: 65.4",
    },
    {
        "organization": "India Post", "post_name": "GDS (Gramin Dak Sevak) - BPM/ABPM/Dak Sevak",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "44228",
        "salary": "₹10,000 - ₹29,380 (TRCA)",
        "eligibility": "10th pass with Maths, English, Local Language. Age 18-40.",
        "location": "All States / Circles", "state": "All India",
        "last_date": (date.today() + timedelta(days=11)).isoformat(),
        "notification_pdf": "https://indiapostgdsonline.gov.in",
        "apply_link": "https://indiapostgdsonline.gov.in",
        "min_age": 18, "max_age": 40,
        "description": "India Post's largest recruitment. Branch Post Master, Assistant BPM and Dak Sevak roles in rural post offices across every state.",
        "selection_process": "Merit-list based on 10th marks (no written exam) → Document Verification",
        "important_dates": "Apply: Now • Merit list: 45 days • Joining: 3 months",
        "previous_year_cutoff": "Merit-based (10th %) — General: 92% | OBC: 88% | SC/ST: 82%",
    },
    {
        "organization": "State Police (various)", "post_name": "Police Constable - Male & Female",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "60000+",
        "salary": "₹21,700 - ₹69,100 (Pay Level 3)",
        "eligibility": "10th/12th (state-specific). Height & chest requirements. Age 18-25.",
        "location": "State-wise", "state": "All India",
        "last_date": (date.today() + timedelta(days=22)).isoformat(),
        "notification_pdf": "https://police.example.gov.in",
        "apply_link": "https://police.example.gov.in/apply",
        "min_age": 18, "max_age": 25,
        "description": "State police constable recruitments across UP, MP, Bihar, Rajasthan, Karnataka, Telangana etc. Physical + written combined.",
        "selection_process": "Written Test → PET (Physical Endurance) → PST (Measurement) → Medical → DV",
        "important_dates": "Apply: Now • Written: 60 days • PET: 90 days • Final: 6 months",
        "previous_year_cutoff": "Varies by state — Typical Gen: 65% | OBC: 60% | SC: 55%",
    },
    {
        "organization": "BSF (Border Security Force)", "post_name": "Constable (GD) / Head Constable",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "1526",
        "salary": "₹21,700 - ₹69,100 (Pay Level 3)",
        "eligibility": "10th pass. Age 18-23. Male/Female. Height 170cm (males).",
        "location": "PAN India — mostly border areas", "state": "All India",
        "last_date": (date.today() + timedelta(days=19)).isoformat(),
        "notification_pdf": "https://bsf.gov.in", "apply_link": "https://rectt.bsf.gov.in",
        "min_age": 18, "max_age": 23,
        "description": "Guard India's borders with BSF. Central Armed Police Force role — respectable, secure, pan-India postings.",
        "selection_process": "PET → PST → Written (CBT) → Medical → DV",
        "important_dates": "Apply: Now • PET: 45 days • Written: 90 days • Final: 6 months",
        "previous_year_cutoff": "General: 132/200 | OBC: 125 | SC: 118 | ST: 112",
    },
    {
        "organization": "CRPF (Central Reserve Police Force)", "post_name": "Constable / ASI / Head Constable",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "9212",
        "salary": "₹21,700 - ₹1,12,400",
        "eligibility": "10th/12th. Age 18-25. Physical standards apply.",
        "location": "PAN India", "state": "All India",
        "last_date": (date.today() + timedelta(days=24)).isoformat(),
        "notification_pdf": "https://crpf.gov.in", "apply_link": "https://rect.crpf.gov.in",
        "min_age": 18, "max_age": 25,
        "description": "India's largest CAPF. Constable, ASI (Steno/Clerk), Head Constable roles. Deployed across sensitive zones.",
        "selection_process": "PET → PST → Written CBT → Medical → DV",
        "important_dates": "Apply: Now • PET: 60 days • CBT: 3 months",
        "previous_year_cutoff": "Constable Gen: 128/200 | OBC: 120 | SC: 112 | ST: 105",
    },
    {
        "organization": "CISF (Central Industrial Security Force)", "post_name": "Constable / Head Constable / ASI",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "1130",
        "salary": "₹21,700 - ₹69,100",
        "eligibility": "10th/12th. Age 18-25. Physical & medical standards.",
        "location": "Metro/Airport/PSU sites", "state": "All India",
        "last_date": (date.today() + timedelta(days=13)).isoformat(),
        "notification_pdf": "https://cisf.gov.in", "apply_link": "https://cisfrectt.cisf.gov.in",
        "min_age": 18, "max_age": 25,
        "description": "Guard airports, metros, nuclear plants, refineries. Prestigious CAPF with urban postings preferred.",
        "selection_process": "PET → PST → Written → Medical → DV",
        "important_dates": "Apply: Now • PET: 45 days • Final: 5 months",
        "previous_year_cutoff": "General: 130/200 | OBC: 122 | SC: 114 | ST: 106",
    },
    {
        "organization": "ITBP (Indo-Tibetan Border Police)", "post_name": "Constable (GD) / Head Constable",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "819",
        "salary": "₹21,700 - ₹69,100",
        "eligibility": "10th pass. Age 18-23. Sound health for high-altitude duty.",
        "location": "Indo-Tibet border (Ladakh, Uttarakhand, HP, Sikkim, AP)", "state": "All India",
        "last_date": (date.today() + timedelta(days=16)).isoformat(),
        "notification_pdf": "https://itbpolice.nic.in", "apply_link": "https://recruitment.itbpolice.nic.in",
        "min_age": 18, "max_age": 23,
        "description": "Serve at the Indo-China border. Elite mountaineering CAPF with excellent training & altitude allowance.",
        "selection_process": "PET → PST → Written → Medical (rigorous) → DV",
        "important_dates": "Apply: Now • PET: 60 days • Final: 6 months",
        "previous_year_cutoff": "General: 128/200 | OBC: 120 | SC: 112",
    },
    {
        "organization": "SSB (Sashastra Seema Bal)", "post_name": "Constable (GD) / Tradesman",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "635",
        "salary": "₹21,700 - ₹69,100",
        "eligibility": "10th pass. Age 18-23. Male/Female.",
        "location": "Indo-Nepal & Indo-Bhutan border", "state": "All India",
        "last_date": (date.today() + timedelta(days=21)).isoformat(),
        "notification_pdf": "https://ssbrectt.gov.in", "apply_link": "https://ssbrectt.gov.in",
        "min_age": 18, "max_age": 23,
        "description": "Guard India's borders with Nepal & Bhutan. CAPF role with excellent perks and rural placements.",
        "selection_process": "PET → PST → Written → Medical → DV",
        "important_dates": "Apply: Now • PET: 45 days • Final: 6 months",
        "previous_year_cutoff": "General: 126/200 | OBC: 118 | SC: 110",
    },
    {
        "organization": "Assam Rifles", "post_name": "Rifleman (GD) / Warrant Officer",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "150",
        "salary": "₹21,700 - ₹69,100 + hardship allowance",
        "eligibility": "10th pass. Age 18-23.",
        "location": "North-East India", "state": "Assam, Nagaland, Manipur, Mizoram, Tripura, AP",
        "last_date": (date.today() + timedelta(days=26)).isoformat(),
        "notification_pdf": "https://assamrifles.gov.in", "apply_link": "https://assamrifles.gov.in/careers",
        "min_age": 18, "max_age": 23,
        "description": "Oldest paramilitary of India, guarding the North-East. Excellent hardship allowance and quick promotions.",
        "selection_process": "PET → PST → Written → Interview → Medical",
        "important_dates": "Apply: Now • PET: 60 days • Final: 5 months",
        "previous_year_cutoff": "General: 130/200 | OBC: 122 | SC: 114",
    },
    {
        "organization": "Merchant Navy (DG Shipping)", "post_name": "GP Rating / Trainee Marine Engineer",
        "category": "Diploma Eligible",
        "branches": ["Mechanical Engineering", "Electrical Engineering", "Electronics Engineering"],
        "qualifications": ["Diploma", "BTech", "BE", "Final Year Student"], "vacancies": "1500",
        "salary": "$800 - $3000 per month (₹65k-2.5L)",
        "eligibility": "10th/12th with PCM (60%) or Diploma. Age 17-25. Medically fit.",
        "location": "Global — ships worldwide", "state": "All India",
        "last_date": (date.today() + timedelta(days=27)).isoformat(),
        "notification_pdf": "https://dgshipping.gov.in", "apply_link": "https://imupune.edu.in",
        "min_age": 17, "max_age": 25,
        "description": "Sail the world on merchant ships. High-paying tax-free income, 6-month contracts, quick career growth to Officer.",
        "selection_process": "IMU-CET → Interview → Medical → Course (6 months) → On-ship placement",
        "important_dates": "Apply: Now • IMU-CET: 60 days • Joining course: 3 months",
        "previous_year_cutoff": "IMU-CET Gen: 130/200 | OBC: 120 | SC: 105",
    },
    {
        "organization": "Indian Coast Guard", "post_name": "Navik (GD) / Yantrik / Assistant Commandant",
        "category": "Diploma Eligible",
        "branches": ["Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "BTech", "BE", "Final Year Student"], "vacancies": "310",
        "salary": "₹21,700 - ₹69,100 (Navik) / ₹56,100 - ₹1,77,500 (AC)",
        "eligibility": "10th/12th (Navik), Diploma (Yantrik), Engineering degree (AC). Age 18-25.",
        "location": "Coast Guard Stations across India", "state": "All India",
        "last_date": (date.today() + timedelta(days=14)).isoformat(),
        "notification_pdf": "https://joinindiancoastguard.cdac.in",
        "apply_link": "https://joinindiancoastguard.cdac.in",
        "min_age": 18, "max_age": 25,
        "description": "Protect India's maritime interests. Navik (Sailor), Yantrik (Technical), Asst Commandant (Officer) posts. Excellent perks + free medical.",
        "selection_process": "Stage 1 (CBT) → Stage 2 (PFT + Medical) → Stage 3 (Doc Verification) → Final Merit",
        "important_dates": "Apply: Now • Stage 1: 45 days • Stage 2: 90 days",
        "previous_year_cutoff": "Navik Gen: 65% | OBC: 60% | SC: 55%",
    },
    {
        "organization": "PGCIL (Power Grid Corporation of India Ltd)", "post_name": "Diploma Trainee - Elec/ECE/Civil",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Electrical Engineering", "Electronics Engineering"],
        "qualifications": ["Diploma"], "vacancies": "425",
        "salary": "₹47,600 - ₹1,45,500",
        "eligibility": "Diploma in Elec/ECE/Civil with 60%. Age 18-27.",
        "location": "PAN India — grid stations", "state": "All India",
        "last_date": (date.today() + timedelta(days=23)).isoformat(),
        "notification_pdf": "https://powergrid.in/careers", "apply_link": "https://careers.powergrid.in",
        "min_age": 18, "max_age": 27,
        "description": "India's largest power transmission Maharatna PSU. Diploma Trainee is a top-tier diploma-holder job with fast confirmation to Junior Engineer.",
        "selection_process": "CBT (Objective) → Document Verification → Medical",
        "important_dates": "Apply: Now • CBT: 45 days • Joining: 4 months",
        "previous_year_cutoff": "Elec Gen: 72 | OBC: 68 | SC: 62 | ECE Gen: 70 | Civil Gen: 65 (out of 120)",
    },
    {
        "organization": "State PWD/Irrigation/Electricity Board", "post_name": "State Junior Engineer (JE) - Various States",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering"],
        "qualifications": ["Diploma", "BTech", "BE"], "vacancies": "5000+ (combined)",
        "salary": "₹35,400 - ₹1,12,400 (Pay Level 6)",
        "eligibility": "Diploma or BE/BTech in relevant engineering. Age 18-40 (state-specific).",
        "location": "State-wise (UP, Bihar, MP, Rajasthan, Haryana, Punjab, Karnataka)", "state": "State-wise",
        "last_date": (date.today() + timedelta(days=29)).isoformat(),
        "notification_pdf": "https://sssc.example.gov.in",
        "apply_link": "https://sssc.example.gov.in/apply",
        "min_age": 18, "max_age": 40,
        "description": "State Public Works Department, Irrigation and Electricity Board JE posts. Multiple state-specific recruitments running.",
        "selection_process": "Written Exam (Technical + GK) → Document Verification → Medical",
        "important_dates": "State-wise varies • Typically 60-90 days after notification",
        "previous_year_cutoff": "State-wise varies — Typical Civil Gen: 70% | Elec Gen: 72% | Mech Gen: 68%",
    },
    {
        "organization": "Various PSUs & Government Departments", "post_name": "Apprenticeship - Diploma & ITI Trades",
        "category": "Diploma Eligible",
        "branches": ["Civil Engineering", "Mechanical Engineering", "Electrical Engineering", "Electronics Engineering", "Computer Science"],
        "qualifications": ["Diploma", "Final Year Student"], "vacancies": "50000+",
        "salary": "₹9,000 - ₹18,000 stipend",
        "eligibility": "Diploma completed within last 3 years. Age 18-30.",
        "location": "Host industries across India", "state": "All India",
        "last_date": (date.today() + timedelta(days=45)).isoformat(),
        "notification_pdf": "https://apprenticeshipindia.gov.in",
        "apply_link": "https://apprenticeshipindia.gov.in",
        "min_age": 18, "max_age": 30,
        "description": "Government-backed 1-year apprenticeship in NTPC, IOCL, HAL, ISRO, Railways, private manufacturers. Certificate + industry experience.",
        "selection_process": "Online application → Merit shortlisting → Interview → Joining",
        "important_dates": "Rolling recruitment • Selection within 30 days of application",
        "previous_year_cutoff": "Merit-based on Diploma % — Gen: 65% | OBC: 60% | SC: 55%",
    },
]


async def seed_jobs():
    # Add any new sample jobs (idempotent by organization+post_name)
    for j in SAMPLE_JOBS:
        exists = await db.jobs.find_one(
            {"organization": j["organization"], "post_name": j["post_name"], "source": "seed"},
            {"_id": 1},
        )
        if exists:
            continue
        await db.jobs.insert_one({
            "job_id": f"job_{uuid.uuid4().hex[:12]}",
            **j,
            "logo_url": j.get("logo_url"),
            "is_active": True,
            "source": "seed",
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    logger.info(f"Seed check complete — {len(SAMPLE_JOBS)} sample entries")

    # Backfill previous_year_cutoff for seeded jobs (safe on re-runs)
    cutoffs = {
        "Indian Railways (RRB)": "General: 82.5 | OBC: 78.2 | SC: 70.1 | ST: 65.4 (out of 150)",
        "Bharat Heavy Electricals Limited (BHEL)": "GATE cutoff — General: 620 | OBC: 570 | SC/ST: 500",
        "ISRO (VSSC)": "General: 74% | OBC: 68% | SC/ST: 60%",
        "NTPC Limited": "GATE cutoff — General: 680 | OBC: 620 | SC/ST: 550",
        "TCS Digital": "NQT score ≥ 75 percentile + Advanced coding round cleared",
        "NATS (National Apprenticeship Training Scheme)": "No cutoff — merit-based selection by host industry",
        "Infosys Ltd": "InfyTQ Certification cleared + Aptitude ≥ 65%",
        "DRDO (Defence Research)": "GATE cutoff — General: 700 | OBC: 640 | SC/ST: 570",
        "Wipro Elite NTH": "Online test cutoff — 60% aptitude + 2 coding problems solved",
        "SAIL (Steel Authority of India)": "GATE cutoff — General: 640 | OBC: 590 | SC/ST: 520",
        "Google India": "DSA/coding round + system design; top 5% shortlisted",
        "L&T Construction": "Aptitude ≥ 60% + Technical interview cleared",
    }
    for org, cutoff in cutoffs.items():
        await db.jobs.update_many(
            {"organization": org, "source": "seed"},
            {"$set": {"previous_year_cutoff": cutoff}},
        )


async def ensure_indexes():
    await db.users.create_index("email", unique=True)
    await db.users.create_index("user_id", unique=True)
    await db.user_sessions.create_index("session_token", unique=True)
    await db.user_sessions.create_index("user_id")
    await db.jobs.create_index("job_id", unique=True)
    await db.jobs.create_index("apply_link")
    await db.jobs.create_index([("category", 1), ("branches", 1)])
    await db.applications.create_index([("user_id", 1), ("job_id", 1)], unique=True)
    await db.resumes.create_index("resume_id", unique=True)
    await db.push_devices.create_index([("user_id", 1), ("device_token", 1)], unique=True)


@app.on_event("startup")
async def startup_event():
    global _push_client
    _push_client = httpx.AsyncClient(base_url="https://push-service-placeholder.com")
    await seed_admin()
    
    # Check karein ki scheduler pehle se to nahi chal raha
    if not scheduler.running:
        scheduler.add_job(refresh_jobs_task, 'interval', hours=12)
        scheduler.start()
        
    logger.info("CareerPulse Background Services Started Successfully")


@app.on_event("shutdown")
async def on_shutdown():
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    
    client.close()


app.include_router(api)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

    # Check kariye kya aapke server.py ke end me aisa kuch hai:
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)