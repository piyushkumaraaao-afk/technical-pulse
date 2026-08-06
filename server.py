"""CareerPulse Backend - Job alert app for Diploma/BTech Indian engineering students."""
import os
import uuid
import logging
import asyncio
import feedparser
import requests
import json
import httpx
import hashlib
import pandas as pd
import razorpay
import hmac
from collections import defaultdict
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional, Literal, Any, Dict
from fastapi.security import HTTPBearer
from fastapi import FastAPI, Request, UploadFile, File, Depends, HTTPException, Header
from fastapi import Query
from io import BytesIO
import random
import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from crawl4ai import AsyncWebCrawler
from urllib.parse import urljoin
from fastapi import BackgroundTasks, APIRouter
from pymongo.errors import DuplicateKeyError
from typing import Optional, List
from bson import ObjectId
from flask import Flask, request, jsonify, url_for
from werkzeug.utils import secure_filename
from flask import send_from_directory


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
GROQ_MODEL = "llama-3.1-8b-instant" 

ALLOWED_POST_TYPES = {
    "Job", "Admit Card", "Result", "Scholarship", "Apprenticeship",
    "Internship", "Upcoming Exam", "Answer Key", "IGNORE"
}

app = FastAPI()

client = razorpay.Client(
    auth=(
        os.getenv("RAZORPAY_KEY_ID"),
        os.getenv("RAZORPAY_KEY_SECRET")
    )
)

@app.post("/create-order")
def create_order():
    order = client.order.create({
        "amount": 1000,  # ₹10 = 1000 paise
        "currency": "INR"
    })
    return order

# =======================
# Bulletproof Helper Functions
# =======================

def generate_content_hash(title: str, organization: str, link: str) -> str:
    content = f"{title}|{organization}|{link}".lower().strip()
    return hashlib.sha256(content.encode()).hexdigest()

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
    for phrase, label in (("12th", "12th"), ("intermediate", "12th"), ("iti", "ITI"), ("10th", "10th"), ("diploma", "Diploma"), ("graduate", "Graduate"), ("b.tech", "B.Tech"), ("btech", "B.Tech"), ("be", "B.Tech"), ("b.sc", "B.Sc"), ("bsc", "B.Sc")):
        if phrase in text and label not in found: found.append(label)
    return found or ["Not Specified"]

def generate_content_hash(
    organization: str,
    post_name: str,
    last_date: str
) -> str:
    text = f"{organization}|{post_name}|{last_date}"
    return hashlib.sha256(text.lower().encode()).hexdigest()    

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
def extract_tables_as_json(html: str) -> list:
    tables = []

    try:
        dfs = pd.read_html(html)

        for df in dfs[:10]:
            try:
                df = df.fillna("NA")
                tables.append(df.to_dict("records"))
            except:
                pass

    except Exception:
        pass

    return tables    

async def extract_job_details_with_ai(url: str) -> dict:
    page_text = ""
    links_text = ""
    
    try:
        async with AsyncWebCrawler(verbose=False) as crawler:
            result = await crawler.arun(url=url, bypass_cache=True)
            page_text = result.markdown[:6000] if result.markdown else "" 
    except Exception as e:
        print(f"Crawl4AI failed for {url}: {e}")

    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        async with httpx.AsyncClient(timeout=15.0, headers=headers, follow_redirects=True) as client:
            response = await client.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            table_json = extract_tables_as_json(response.text)
            
            if not page_text:
                # 🚀 BIG FIX HERE: HTML Tables ko pipe (|) formate mein convert kar rahe hain
                # Taaki AI SAIL Rourkela jaise table (Trade | ITI | Diploma) ko samajh sake
                for table in soup.find_all("table"):
                    for tr in table.find_all("tr"):
                        for td in tr.find_all(["td", "th"]):
                            td.append(" | ")
                        tr.append("\n")
                
                for tag in soup(["script", "style", "nav", "footer"]): tag.decompose()
                page_text = soup.get_text(separator=' ', strip=True)[:6000]

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

    # 🚀 AI PROMPT FIX: Admit Card/Result explicitly valid kiye hain!
    prompt = f"""
    You are an elite Data Extraction AI. Analyze the Markdown content and Links from a recruitment website.
    
    CRITICAL RULES - DO NOT IGNORE ADMIT CARDS & RESULTS:
    - If the page is about an Admit Card, Result, Exam Date, or Scholarship, it is VALID. DO NOT choose "IGNORE". 
    - Set the `post_type` to "Admit Card", "Result", "Upcoming Exam", or "Scholarship" accordingly.
    - For these types, it is okay if Salary, Vacancies, or Age Limits are "NA". Just extract the Organization, Post Name, and Links!
    
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

    JSON SCHEMA TO RETURN (Strictly use these keys):
    {{
      "post_name": "Exact extracted Title/Role",
      "organization": "Exact conducting body/company",
      "category": "Choose ONE: ['Government', 'PSU', 'Private']",
      "post_type": "Choose ONE: ['Job', 'Admit Card', 'Result', 'Scholarship', 'Apprenticeship', 'Internship', 'Upcoming Exam', 'IGNORE']",
      "total_post": "Number only (e.g., 6557)",
      "category_vacancies": [ {{ "post_name": "Specific Post Name OR Trade", "General": "NA", "OBC": "NA", "EWS": "NA", "SC": "NA", "ST": "NA" }} ],
      "state_wise_vacancies": [ {{ "state_name": "State", "vacancies": "Number" }} ],
      "trade_wise_vacancies": [ {{ "trade_name": "Trade (e.g., Fitter, COPA)", "ITI": "Number or NA", "Diploma": "Number or NA", "Degree": "Number or NA" }} ],
      "salary_wise_post_name":[ {{ "post_name": "Specific Post Name OR Trade", "salary": "Salary or Pay scale or Stipend" }} ],
      "multiple_posts": [
         {{ "post_name": "Specific Post Name OR Trade", "vacancies": "Number", "eligibility": "Qualification for this specific post" }}
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
      "apply_online_link": "Exact Apply URL from LINKS.",
      "admit_card_link": "Exact Admit Card URL",
      "answer_key_link": "Exact Answer Key URL",
      "result_link": "Exact Result URL"
    }}
    
    --- DATA TO ANALYZE ---
    {page_text}
    
    --- IMPORTANT LINKS WITH CONTEXT ---
    {links_text}
    --- STRUCTURED TABLE DATA ---
    {json.dumps(table_json)[:4000]}
    """

    try:
        await asyncio.sleep(1) 
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": GROQ_MODEL,
            "messages": [
                {"role": "system", "content": "You are a master data extractor. DO NOT ignore Admit Cards or Results just because salary/vacancy is missing."},
                {"role": "user", "content": prompt}
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.0 
        }
        async with httpx.AsyncClient(timeout=45.0) as ai_client:
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
    print("Background Scraping Started...")
    added = 0
    today_str = date.today().isoformat()

    result = await db.jobs.update_many(
        {"is_active": True, "last_date": {"$type": "string", "$lt": today_str}},
        {"$set": {"is_active": False, "archive": True}},
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
                content_hash = generate_content_hash(
                    entry["title"],
                    job_link
                )
                
                duplicate = await db.jobs.find_one(
                    {"content_hash": content_hash},
                    {"_id": 1}
                )

                if duplicate:
                    continue

                title, summary = entry["title"], entry["summary"]
                title_type = detect_post_type(title)
                
                # Agar hamara title_type already IGNORE bol raha hai (jaise Answer Key), toh seedha chhod do
                if title_type == "IGNORE":
                    continue

                print(f"Deep scraping [{title_type}]: {title}")
                details = await extract_job_details_with_ai(job_link)

                parent_job_id = None

                if post_type in ["Admit Card", "Result", "Answer Key"]:
                    original_job = await db.jobs.find_one({
                        "post_name": {"$regex": post_name[:20], "$options": "i"},
                        "post_type": "Job"
                    })

                    if original_job:
                        parent_job_id = original_job["job_id"]
                
                # 🚀 PYTHON FAILSAFE: Agar AI bewakoofi karke Result/Admit Card ko IGNORE kar de,
                # Toh hum AI ki baat nahi manenge, aur apna 'title_type' use karke usko add kar lenge!
                ai_post_type = details.get("post_type", "NA")
                
                if ai_post_type == "IGNORE" and title_type in ["Admit Card", "Result", "Scholarship", "Upcoming Exam"]:
                    ai_post_type = title_type
                    print(f"⚠️ Overriding AI: Saved as {title_type} instead of IGNORE.")
                
                if ai_post_type == "IGNORE":
                    print(f"🚫 Properly Ignored: {title}")
                    continue

                post_type = ai_post_type if ai_post_type in ALLOWED_POST_TYPES and ai_post_type != "NA" else title_type

                post_name = details.get("post_name", "NA")
                if post_name == "NA" or not post_name:
                    post_name = title

                extracted_apply = details.get("apply_online_link", "NA")
                action_link = extracted_apply if extracted_apply != "NA" else job_link
                
                exact_date = valid_iso_date(details.get("last_date"))
                final_last_date = exact_date if exact_date else (date.today() + timedelta(days=30)).isoformat()

                similar = await db.jobs.find_one({
                    "organization": details.get("organization"),
                    "post_name": post_name,
                    "last_date": final_last_date
                })

                if similar:
                    continue

                await db.jobs.insert_one({
                    "job_id": f"job_{uuid.uuid4().hex[:12]}",
                    "parent_job_id": parent_job_id,
                    "content_hash": content_hash,
                    "source_url": job_link,
                    "organization": details.get("organization", "NA") if details.get("organization", "NA") != "NA" else src["name"],
                    "post_name": post_name,
                    "post_type": post_type,
                    "category": details.get("category", "NA") if details.get("category", "NA") != "NA" else src.get("default_category", "Government"),
                    
                    "qualifications": fallback_qualifications(title, summary, details.get("qualifications", [])),
                    "branches": details.get("branches", []),
                    "vacancies": details.get("total_post", "NA"),
                    "salary": details.get("salary", "NA"),
                    "eligibility": details.get("eligibility", "NA") if details.get("eligibility", "NA") != "NA" else summary,
                    
                    "category_vacancies": details.get("category_vacancies", []),
                    "multiple_posts": details.get("multiple_posts", []),
                    "trade_wise_vacancies": details.get("trade_wise_vacancies", []), # For SAIL Rourkela style UI
                    "salary_wise_post_name": details.get("salary_wise_post_name", []),
                    "state_wise_vacancies": details.get("state_wise_vacancies", []),
                    "mode_of_selection": details.get("mode_of_selection", []),
                    "location": details.get("location", "NA"),
                    
                    "last_date": final_last_date,
                    "expires_at": final_last_date,
                    "is_exact_date": bool(exact_date), 
                    
                    "notification_pdf": details.get("check_official_notice") if details.get("check_official_notice") != "NA" else None,
                    "apply_link": action_link,
                    "min_age": as_int_or_none(details.get("min_age")),
                    "max_age": as_int_or_none(details.get("max_age")),
                    "detailed_age_info": details.get("detailed_age_info", "NA"), # For RRB / ISRO style complex age info
                    "description": summary,
                    "is_active": True,
                    "source": f"scraper:{src['name']}",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "admit_card_link": details.get("admit_card_link"),
                    "answer_key_link": details.get("answer_key_link"),
                    "result_link": details.get("result_link"),
                    "views": 0,
                    "saves": 0,
                    "applications": 0,
                    "trending_score": 0,
                    "search_count": 0,
                    
                })
                if parent_job_id:

                    if post_type == "Admit Card":
                        await db.applications.update_many(
                            {"job_id": parent_job_id},
                            {"$set": {"status": "admit_card"}}
                        )

                    elif post_type == "Result":
                        await db.applications.update_many(
                            {"job_id": parent_job_id},
                            {"$set": {"status": "result"}}
                        )

                    saved_users = await db.applications.find(
                        {"job_id": parent_job_id},
                        {"_id": 0, "user_id": 1}
                    ).to_list(1000)

                    recipients = [x["user_id"] for x in saved_users]

                    await send_push(
                        recipients,
                        {
                            "title": f"{post_type} Released",
                            "message": post_name
                        }
                    )
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
    name: str
    email: EmailStr
    password: str
    phone: Optional[str] = None

class LoginBody(BaseModel):
    email: EmailStr
    password: str


class ProfileUpdateBody(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    qualification: Optional[List[str]] = None 
    branch: Optional[List[str]] = None
    passout_year: Optional[int] = None
    state: Optional[str] = None
    age: Optional[int] = None
    avatar: Optional[str] = None

class MessageBody(BaseModel):
    receiver_id: str
    text: str
    type: Optional[str] = "text" # 'text' ya 'job'
    jobData: Optional[dict] = None
    disappearing_hours: Optional[int] = 0 # 0: Off, 24: 24h, 168: 7d, 720: 30d
    time: Optional[str] = None

class EditMessageBody(BaseModel):
    message_id: str
    new_text: str  

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
    layoutStyle: Optional[str] = "layout1"  # 🚀 Added to receive layout 1 to 6
    colorTheme: Optional[str] = "modern"

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

class UpgradePremiumBody(BaseModel):
    payment_id: str

# Razorpay client setup karein
razorpay_client = razorpay.Client(auth=("YOUR_KEY_ID", "YOUR_KEY_SECRET"))


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
async def notify_saved_users(parent_job_id, post_type, post_name):

    users = await db.applications.find(
        {"job_id": parent_job_id},
        {"_id": 0, "user_id": 1}
    ).to_list(1000)

    if users:
        await send_push(
            [u["user_id"] for u in users],
            {
                "title": f"{post_type} Released",
                "message": post_name
            }
        )


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

@api.get("/admin/users")
async def admin_list_users(admin: dict = Depends(require_admin)):
    users_cursor = db.users.find({}, {"password_hash": 0})
    users = []
    async for u in users_cursor:
        users.append({
            "user_id": str(u.get("user_id") or u.get("_id")),
            "name": u.get("name"),
            "email": u.get("email"),
            "branch": u.get("branch"),
            "phone": u.get("phone"),
            "qualification": u.get("qualification"),
            "state": u.get("state"),
            "is_premium": u.get("is_premium", False),
            "is_blocked": u.get("is_blocked", False),
            "avatar": u.get("avatar"),
            "created_at": u.get("created_at")
        })
    return {
        "users": users,
        "count": len(users)
    }


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
        "phone": getattr(body, 'phone', None),
        "password_hash": hash_password(body.password),
        "auth_provider": "email",
        "is_admin": False,
        "qualification": None,
        "branch": None,
        "passout_year": None,
        "state": None,
        "age": None,
        "avatar": None,
        "notification_settings": {
            "job_alert": True,
            "admit_card": True,
            "result": True
        },    
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


    from google.oauth2 import id_token
from google.auth.transport import requests

class GoogleTokenBody(BaseModel):
    id_token: str

@api.post("/auth/google")
async def google_login(body: GoogleTokenBody):

    google_user = id_token.verify_oauth2_token(
        body.id_token,
        requests.Request(),
        GOOGLE_CLIENT_ID
    )

    email = google_user["email"].lower()
    name = google_user.get("name", "")

    user = await db.users.find_one({"email": email})

    if not user:
        user_id = f"user_{uuid.uuid4().hex[:12]}"

        await db.users.insert_one({
            "user_id": user_id,
            "email": email,
            "name": name,
            "auth_provider": "google",
            "is_admin": False
        })

        user = await db.users.find_one({"email": email})

    token = create_jwt(user["user_id"])

    return {
        "access_token": token,
        "user": user
    }


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

@api.put("/notification-settings")
async def update_notification_settings(
    body: dict,
    user: dict = Depends(get_current_user)
):
    await db.users.update_one(
        {"user_id": user["user_id"]},
        {"$set": {
            "notification_settings": body
        }}
    )

    return {"ok": True, "settings": body}

@api.get("/notification-settings")
async def get_notification_settings(
    user: dict = Depends(get_current_user)
):
    data = await db.users.find_one(
        {"user_id": user["user_id"]},
        {"_id":0, "notification_settings":1}
    )

    return {
        "settings": data.get("notification_settings", {
            "job_alert":True,
            "admit_card":True,
            "result":True
        })
    }        


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
    page: int = 1,
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
        await db.search_logs.insert_one({
            "query": search,
            "created_at": datetime.now(timezone.utc).isoformat()
        })    
    if search:
        q["$or"] = [
            {"post_name": {"$regex": search, "$options": "i"}},
            {"post_type": {"$regex": search, "$options": "i"}},
            {"organization": {"$regex": search, "$options": "i"}},
            {"location": {"$regex": search, "$options": "i"}},
        ]
        
    skip = max(0, (page - 1) * limit)

    cursor = (
        db.jobs.find(q, {"_id": 0})
        .sort("last_date", 1)
        .skip(skip)
        .limit(limit)
    )
    jobs = await cursor.to_list(length=limit)
    total = await db.jobs.count_documents(q)
    return {
    "jobs": jobs,
    "count": len(jobs),
    "total": total,
    "page": page,
    "limit": limit
}


@api.get("/jobs/recommended")
async def recommended_jobs(user: dict = Depends(get_current_user)):
    q = {
        "is_active": True,
        "post_type": "Job",
        "$or": [
            {"branches": user.get("branch")},
            {"qualifications": user.get("qualification")},
            {"location": user.get("state")}
        ]
    }
    jobs = await db.jobs.find(
        q,
        {"_id":0}
    ).sort(
        [("trending_score",-1),("views",-1)]
    ).limit(20).to_list(20)
    return {"jobs": jobs}


@api.get("/jobs/{job_id}")
async def get_job(
    job_id: str, 
    user: dict = Depends(get_current_user)
):

    await db.jobs.update_one(
        {"job_id": job_id},
        {
            "$inc": {
                "views": 1,
                "trending_score": 1
            }
        }
    )
    
    job = await db.jobs.find_one(
        {"job_id": job_id},
        {"_id": 0}
    )
    if user:
        await db.recent_jobs.update_one(
            {
                "user_id": user["user_id"],
                "job_id": job_id
            },
            {
                "$set": {
                    "viewed_at": datetime.now(timezone.utc)
                }
            },
            upsert=True
        )
    if not job:
        raise HTTPException(
            status_code=404,
            detail="Job not found"
        )

    return {"job": job}

@api.get("/jobs/{job_id}/related")
async def related_jobs(job_id: str):

    job = await db.jobs.find_one(
        {"job_id": job_id},
        {"_id": 0}
    )

    if not job:
        raise HTTPException(404, "Job not found")

    related = await db.jobs.find({
        "job_id": {"$ne": job_id},
        "is_active": True,
        "$or": [
            {"category": job.get("category")},
            {"branches": {"$in": job.get("branches", [])}},
            {"qualifications": {"$in": job.get("qualifications", [])}}
        ]
    },
    {"_id": 0}
    ).limit(10).to_list(10)

    return {"jobs": related}

@api.get("/jobs/recent")
async def recent_jobs(
    user: dict = Depends(get_current_user)
):

    jobs = await db.recent_jobs.aggregate([
        {"$match": {"user_id": user["user_id"]}},
        {"$sort": {"viewed_at": -1}},
        {"$limit": 20},
        {"$lookup": {
            "from": "jobs",
            "localField": "job_id",
            "foreignField": "job_id",
            "as": "job"
        }},
        {"$unwind": "$job"},
        {"$replaceRoot": {"newRoot": "$job"}}
    ]).to_list(20)

    return {"jobs": jobs}

@api.get("/jobs/expiring")
async def expiring():
    return {
        "jobs": await db.jobs.find(
            {"is_active":True},
            {"_id":0}
        ).sort("last_date",1).limit(20).to_list(20)
    }

@api.get("/jobs/for-you")
async def for_you(user: dict = Depends(get_current_user)):

    jobs = await db.jobs.find({
        "is_active": True,
        "$or": [
            {"branches": {"$in": [user.get("branch","")]}},
            {"qualifications": {"$in": [user.get("qualification","")]}}
        ]
    }, {"_id": 0}).sort("trending_score",-1).limit(20).to_list(20)

    return {"jobs": jobs}                


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
async def save_job(
    body: SaveJobBody,
    user: dict = Depends(get_current_user)
):

    job = await db.jobs.find_one(
        {"job_id": body.job_id},
        {"_id": 0}
    )

    if not job:
        raise HTTPException(
            status_code=404,
            detail="Job not found"
        )

    result = await db.applications.update_one(
        {
            "user_id": user["user_id"],
            "job_id": body.job_id
        },
        {
            "$setOnInsert": {
                "user_id": user["user_id"],
                "job_id": body.job_id,
                "status": "saved",
                "post_name": job.get("post_name"),
                "organization": job.get("organization"),
                "post_type": job.get("post_type"),
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        },
        upsert=True
    )

    if result.upserted_id:
        await db.jobs.update_one(
            {"job_id": body.job_id},
            {
                "$inc": {
                    "saves": 1,
                    "trending_score": 3
                }
            }
        )

    await send_push(
        [user["user_id"]],
        {
            "title": "Job Saved",
            "message": job["post_name"]
        }
    )

    return {"ok": True}


@api.post("/applications/apply")
async def apply_job(
    body: ApplyJobBody,
    user: dict = Depends(get_current_user)
):

    job = await db.jobs.find_one(
        {"job_id": body.job_id},
        {"_id": 0}
    )

    if not job:
        raise HTTPException(
            status_code=404,
            detail="Job not found"
        )

    existing = await db.applications.find_one(
        {
            "user_id": user["user_id"],
            "job_id": body.job_id,
            "status": "applied"
        }
    )

    if not existing:
        await db.jobs.update_one(
            {"job_id": body.job_id},
            {
                "$inc": {
                    "applications": 1,
                    "trending_score": 5
                }
            }
        )

    await db.applications.update_one(
        {
            "user_id": user["user_id"],
            "job_id": body.job_id
        },
        {
            "$set": {
                "user_id": user["user_id"],
                "job_id": body.job_id,
                "status": "applied",
                "post_name": job.get("post_name"),
                "organization": job.get("organization"),
                "post_type": job.get("post_type"),
                "applied_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        },
        upsert=True
    )

    await send_push(
        [user["user_id"]],
        {
            "title": "Application Submitted",
            "message": job["post_name"]
        }
    )

    return {"ok": True}

@api.get("/applications/my")
async def my_applications(user: dict = Depends(get_current_user)):

    items = await db.applications.aggregate([
        {"$match": {"user_id": user["user_id"]}},
        {
            "$lookup": {
                "from": "jobs",
                "localField": "job_id",
                "foreignField": "job_id",
                "as": "job"
            }
        },
        {
            "$unwind": {
                "path": "$job",
                "preserveNullAndEmptyArrays": True
            }
        },
        {"$sort": {"updated_at": -1}},
        {"$project": {
            "_id": 0,
            "job.post_name": 1,
            "job.organization": 1,
            "job.admit_card_link": 1,
            "job.answer_key_link": 1,
            "job.result_link": 1,
            "status": 1,
            "updated_at": 1
        }}
    ]).to_list(500)

    return {"items": items}    


@api.get("/applications")
async def get_applications(user: dict = Depends(get_current_user)):
    apps = await db.applications.find({"user_id": user["user_id"]}, {"_id": 0}).to_list(500)
    job_ids = [a["job_id"] for a in apps]
    related_updates = await db.jobs.find({
        "parent_job_id": {"$in": job_ids}
    }, {"_id": 0}).to_list(500)

    updates_map = {}

    for item in related_updates:
        pid = item["parent_job_id"]

        if pid not in updates_map:
            updates_map[pid] = []

        updates_map[pid].append(item)
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
        item = {
            **a,
            "job": job,
            "updates": updates_map.get(job["job_id"], []),
            "post_name": job.get("post_name"),
            "organization": job.get("organization"),
            "admit_card_link": job.get("admit_card_link"),
            "answer_key_link": job.get("answer_key_link"),
            "result_link": job.get("result_link")
        }
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
    app = await db.applications.find_one(
        {"user_id": user["user_id"], "job_id": job_id}
    )

    if app:
        field = "applications" if app["status"] == "applied" else "saves"

        await db.jobs.update_one(
            {"job_id": job_id},
            {"$inc": {field: -1}}
        )

        await db.applications.delete_one(
            {"user_id": user["user_id"], "job_id": job_id}
        )

    return {"ok": True}

@api.get("/applications/tracker")
async def tracker(user: dict = Depends(get_current_user)):

    apps = await db.applications.find(
        {"user_id": user["user_id"]},
        {"_id": 0}
    ).to_list(500)

    return {"applications": apps}    


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
async def create_admin_job(
    data: dict,
    admin: dict = Depends(require_admin)
):
    job_id = f"job_{uuid.uuid4().hex[:12]}"

    # Har post_type aur post_name ka ekdum unique code banega
    hash_data = {
        "organization": data.get("organization", ""),
        "post_name": data.get("post_name", ""),
        "post_type": data.get("post_type", "Job"),
        "category": data.get("category", "Government")
    }
    
    hash_string = json.dumps(hash_data, sort_keys=True).encode('utf-8')
    content_hash = hashlib.sha256(hash_string).hexdigest()

    new_post = {
        "job_id": job_id,
        "content_hash": content_hash,  # <- Ab ye perfectly database me jayega
        "organization": data.get("organization"),
        "post_name": data.get("post_name"),
        "post_type": data.get("post_type", "Job"),
        "category": data.get("category", "Government"),
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
        "is_trending": False
    }

    try:
        await db.jobs.insert_one(new_post)
        return {"message": "Post created successfully"}
    except DuplicateKeyError:
        raise HTTPException(status_code=400, detail="This job post already exists in the database.")


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

@api.patch("/admin/users/{user_id}")
async def update_user_status(user_id: str, request: Request, admin: dict = Depends(require_admin)):
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
    # Yeh task background me chalayega, jisse connection kabhi timeout/fail nahi hoga!
    background_tasks.add_task(refresh_jobs_task)
    
    # Frontend ko 0 bhej do taaki 'undefined' na dikhe.
    # Frontend par '+0 added' aayega, lekin backend parde ke piche saari jobs aaram se save kar dega.
    return {"added": 0, "removed": 0}

@api.get("/admin/analytics")
async def analytics(
    admin: dict = Depends(require_admin)
):
    return {
        "top_views": await db.jobs.find({},{"_id":0}).sort("views",-1).limit(5).to_list(5),
        "top_saves": await db.jobs.find({},{"_id":0}).sort("saves",-1).limit(5).to_list(5),
        "top_applied": await db.jobs.find({},{"_id":0}).sort("applications",-1).limit(5).to_list(5),
    }    

# =======================
# Feedback & User Management 
# =======================
@api.delete("/admin/users/{target_user_id}")
async def admin_delete_user(target_user_id: str, admin: dict = Depends(require_admin)):
    if target_user_id == admin.get("user_id"):
        raise HTTPException(
            status_code=400,
            detail="You cannot delete your own admin account"
        )
        
    # 💡 Smart Query: user_id aur MongoDB _id dono ko check karega
    query = {"$or": [{"user_id": target_user_id}]}
    if ObjectId.is_valid(target_user_id):
        query["$or"].append({"_id": ObjectId(target_user_id)})

    # Ab user 100% delete hoga
    await db.users.delete_one(query)
    
    # Baki related data delete
    await db.applications.delete_many({"user_id": target_user_id})
    await db.resumes.delete_many({"user_id": target_user_id})
    await db.chat_messages.delete_many({"user_id": target_user_id})
    await db.push_devices.delete_many({"user_id": target_user_id})
    await db.recent_jobs.delete_many({"user_id": target_user_id})
    await db.feedback.delete_many({"user_id": target_user_id})

    return {"ok": True, "message": "User and all related data deleted successfully"}

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

@api.get("/admin/top-viewed")
async def top_viewed(
    admin: dict = Depends(require_admin)
):
    jobs = await db.jobs.find(
        {}, {"_id": 0, "post_name": 1, "organization": 1, "views": 1}
    ).sort("views", -1).limit(10).to_list(10)

    return {"jobs": jobs}

@api.get("/admin/top-saved")
async def top_saved(
    admin: dict = Depends(require_admin)
):
    jobs = await db.jobs.find(
        {}, {"_id": 0, "post_name": 1, "organization": 1, "saves": 1}
    ).sort("saves", -1).limit(10).to_list(10)

    return {"jobs": jobs}

@api.get("/admin/top-applied")
async def top_applied(
    admin: dict = Depends(require_admin)
):
    jobs = await db.jobs.find(
        {}, {"_id": 0, "post_name": 1, "organization": 1, "applications": 1}
    ).sort("applications", -1).limit(10).to_list(10)

    return {"jobs": jobs}

@api.get("/admin/top-searches")
async def top_searches(
    admin: dict = Depends(require_admin)
):
    data = await db.search_logs.aggregate([
        {"$group":{"_id":"$query","count":{"$sum":1}}},
        {"$sort":{"count":-1}},
        {"$limit":20}
    ]).to_list(20)

    return {"searches": data}    

@api.get("/users/me/stats")
async def my_stats(user: dict = Depends(get_current_user)):

    saved = await db.applications.count_documents({
        "user_id": user["user_id"],
        "status": "saved"
    })

    applied = await db.applications.count_documents({
        "user_id": user["user_id"],
        "status": "applied"
    })

    return {
        "saved_jobs": saved,
        "applied_jobs": applied
    }

@api.get("/stats")
async def stats():
    return {
        "jobs": await db.jobs.count_documents({"is_active":True}),
        "users": await db.users.count_documents({}),
        "applications": await db.applications.count_documents({"status":"applied"})
    }                    


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
    await db.jobs.create_index("job_id", unique=True)
    await db.jobs.create_index("source_url", unique=True, sparse=True)
    await db.jobs.create_index("content_hash", unique=True)

    await db.jobs.create_index("post_type")
    await db.jobs.create_index("last_date")
    await db.jobs.create_index("organization")
    await db.jobs.create_index("created_at")
    await db.jobs.create_index("qualifications")
    await db.jobs.create_index("branches")
    await db.jobs.create_index("parent_job_id")
    await db.jobs.create_index("views")
    await db.jobs.create_index("saves")
    await db.jobs.create_index("applications")
    await db.jobs.create_index("trending_score")
    await db.jobs.create_index("category")
    await db.jobs.create_index([
        ("organization", 1),
        ("post_name", 1),
        ("last_date", 1)
    ])
    await db.jobs.create_index(
        [("is_active", 1), ("last_date", 1)]
    )
    await db.applications.create_index("user_id")
    await db.applications.create_index("saved_at")
    await db.applications.create_index("applied_at")
    await db.applications.create_index(
        [("user_id", 1), ("job_id", 1)],
        unique=True
    )
    await db.recent_jobs.create_index(
        [("user_id", 1), ("job_id", 1)],
        unique=True
    )
    await db.push_devices.create_index("user_id")
    await db.push_devices.create_index("device_token", unique=True)
    await db.jobs.create_index([
        ("post_name", "text"),
        ("organization", "text"),
        ("description", "text")
    ])
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

@api.get("/jobs/trending")
async def trending_jobs():

    jobs = await db.jobs.find(
        {"is_active": True},
        {"_id": 0}
    ).sort("trending_score", -1).limit(20).to_list(20)

    return {"jobs": jobs}

@api.get("/jobs/popular")
async def popular_jobs():
    jobs = await db.jobs.find(
        {"is_active": True},
        {"_id": 0}
    ).sort(
        [("applications", -1), ("saves", -1)]
    ).limit(20).to_list(20)

    return {"jobs": jobs}

@api.get("/jobs/latest")
async def latest_jobs():
    jobs = await db.jobs.find(
        {"is_active": True},
        {"_id": 0}
    ).sort("created_at", -1).limit(20).to_list(20)

    return {"jobs": jobs}

@api.get("/jobs/count")
async def jobs_count():
    return {
        "active": await db.jobs.count_documents({"is_active":True}),
        "total": await db.jobs.count_documents({})
    }

@api.get("/jobs/deadline")
async def deadline_jobs():
    jobs = await db.jobs.find(
        {"is_active":True},
        {"_id":0}
    ).sort("last_date",1).limit(10).to_list(10)

    return {"jobs": jobs}

@api.get("/jobs/closing-soon")
async def closing_soon():
    return {
        "jobs": await db.jobs.find(
            {"is_active":True},
            {"_id":0}
        ).sort("last_date",1).limit(20).to_list(20)
    }            

@api.get("/home")
async def home():

    return {
        "trending": await db.jobs.find(
            {"is_active": True},
            {"_id": 0}
        ).sort("trending_score",-1).limit(10).to_list(10),

        "latest": await db.jobs.find(
            {"is_active": True},
            {"_id": 0}
        ).sort("created_at",-1).limit(10).to_list(10),

        "popular": await db.jobs.find(
            {"is_active": True},
            {"_id": 0}
        ).sort([
            ("applications",-1),
            ("saves",-1)
        ]).limit(10).to_list(10)
    }

from appwrite.id import ID
from appwrite.query import Query

# 1. MESSAGE SEND KARNE KE LIYE
from datetime import datetime, timedelta

@api.get("/api/users/search")
async def search_users(email: str = "", current_user: dict = Depends(get_current_user)):
    try:
        email_query = email.strip().lower()
        if not email_query:
            return []

        cursor = db.users.find(
            {
                "email": {"$regex": email_query, "$options": "i"},
                "user_id": {"$ne": current_user["user_id"]} # Apne aap ko search mein hide karein
            },
            {"_id": 0, "password_hash": 0}
        ).limit(10)

        users_list = await cursor.to_list(length=10)
        return users_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- 4. SEND MESSAGE ENDPOINT (With Disappearing Logic) ---
@api.post("/api/messages")
async def send_message(body: MessageBody, user: dict = Depends(get_current_user)):
    try:
        sender_id = user["user_id"]

        expires_at = None
        if body.disappearing_hours and body.disappearing_hours > 0:
            expiry_time = datetime.utcnow() + timedelta(hours=int(body.disappearing_hours))
            expires_at = expiry_time.isoformat()

        message_doc = {
            "sender_id": sender_id,
            "receiver_id": body.receiver_id,
            "text": body.text,
            "type": body.type,
            "job_data": body.jobData,
            "created_at": body.time or datetime.utcnow().isoformat(),
            "expires_at": expires_at
        }

        result = await db.messages.insert_one(message_doc)

        await db.messages.update_one(
            {"_id": result.inserted_id},
            {"$set": {"id": str(result.inserted_id)}}
        )

        message_doc["id"] = str(result.inserted_id)

        if "_id" in message_doc:
            del message_doc["_id"]

        return message_doc

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- 5. GET CHAT MESSAGES BETWEEN TWO USERS ---
@api.get("/api/messages/{other_user_id}")
async def get_chat_messages(other_user_id: str, current_user: dict = Depends(get_current_user)):
    try:
        my_id = current_user["user_id"]
        
        # 🚀 NAYA: Fetch karte time check karo ki maine message clear toh nahi kar diya
        cursor = db.messages.find({
            "$or": [
                {"sender_id": my_id, "receiver_id": other_user_id},
                {"sender_id": other_user_id, "receiver_id": my_id}
            ],
            "deleted_for": {"$ne": my_id} # <-- 🔥 SMART FIX: Jin messages mein meri ID deleted_for mein hai, wo mujhe na dikhayein!
        }, {"_id": 0}).sort("created_at", 1)

        messages = await cursor.to_list(length=500)
        return messages
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class DeleteMessagesBody(BaseModel):
    message_ids: List[str]
    delete_type: str # "for_me" (Clear Chat) ya "for_everyone" (Remove)

class FriendBody(BaseModel):
    friend_id: str

# ==========================================
# 1. ADD / REMOVE FRIEND ENDPOINTS
# ==========================================
@api.post("/api/friends/add")
async def add_friend(body: FriendBody, user: dict = Depends(get_current_user)):
    my_id = user["user_id"]
    friend_id = body.friend_id
    
    # Dono users ke 'friends' array mein ek dusre ko add karo
    await db.users.update_one({"user_id": my_id}, {"$addToSet": {"friends": friend_id}})
    await db.users.update_one({"user_id": friend_id}, {"$addToSet": {"friends": my_id}})
    
    return {"message": "Friend added successfully"}

@api.post("/api/friends/remove")
async def remove_friend(body: FriendBody, user: dict = Depends(get_current_user)):
    my_id = user["user_id"]
    friend_id = body.friend_id
    
    # Dono users ke 'friends' array se ek dusre ko remove karo
    await db.users.update_one({"user_id": my_id}, {"$pull": {"friends": friend_id}})
    await db.users.update_one({"user_id": friend_id}, {"$pull": {"friends": my_id}})
    
    return {"message": "Friend removed successfully"}

@api.get("/api/friends")
async def get_friends(user: dict = Depends(get_current_user)):
    my_id = user["user_id"]
    # User ka document nikalo
    me = await db.users.find_one({"user_id": my_id})
    friend_ids = me.get("friends", [])
    
    if not friend_ids:
        return []
        
    # Un sabhi doston ka data fetch karo
    cursor = db.users.find({"user_id": {"$in": friend_ids}}, {"_id": 0, "password_hash": 0})
    friends_list = await cursor.to_list(length=100)
    return friends_list

# ==========================================
# 2. DELETE MESSAGES (For Me / For Everyone)
# ==========================================
@api.post("/api/messages/delete")
async def delete_messages(body: DeleteMessagesBody, user: dict = Depends(get_current_user)):
    my_id = user["user_id"]
    
    if not body.message_ids:
        return {"message": "No messages to delete"}

    # 🚀 SMART FIX: Convert string IDs to MongoDB ObjectIds safely
    obj_ids = []
    for mid in body.message_ids:
        try:
            obj_ids.append(ObjectId(mid))
        except:
            obj_ids.append(mid) # Agar ID string format mein ho toh
            
    # Filter jo ObjectId aur string dono type ki IDs pakad lega
    query_filter = {"$or": [{"_id": {"$in": obj_ids}}, {"id": {"$in": body.message_ids}}]}

    if body.delete_type == "for_everyone":
        # 🚀 REMOVE BUTTON: Dono users ke liye message delete (Replace with "deleted" text)
        await db.messages.update_many(
            query_filter,
            {"$set": {
                "text": "🚫 This message was deleted",
                "is_deleted_for_everyone": True,
                "type": "text", # Agar job card tha, toh normal text ban jayega
                "job_data": None
            }}
        )
        return {"message": "Messages removed for everyone"}

    elif body.delete_type == "for_me":
        # 🚀 CLEAR CHAT: Sirf current user ke 'deleted_for' array mein ID add karo
        await db.messages.update_many(
            query_filter,
            {"$addToSet": {"deleted_for": my_id}}
        )
        return {"message": "Chat cleared for you"}

    return {"message": "Invalid operation"}

@api.put("/api/messages/edit")
async def edit_message(body: EditMessageBody, user: dict = Depends(get_current_user)):
    my_id = user["user_id"]
    
    # ObjectId conversion safety
    try:
        obj_id = ObjectId(body.message_id)
        query = {"$or": [{"_id": obj_id}, {"id": body.message_id}], "sender_id": my_id}
    except:
        query = {"id": body.message_id, "sender_id": my_id}

    # Sirf wahi message edit ho sakta hai jo current user ne bheja ho
    result = await db.messages.update_one(
        query,
        {"$set": {"text": body.new_text}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=403, detail="Message cannot be edited or not found")
        
    return {"message": "Message updated successfully"}

@api.post("/api/users/upgrade")
async def upgrade_to_premium(body: UpgradePremiumBody, user: dict = Depends(get_current_user)):
    my_id = user["user_id"]
    
    # 🚀 Yahan Razorpay payment verify hoti hai backend level par (Optional but recommended for production)
    # Abhi ke liye hum seedha database mein is_premium = True kar rahe hain
    
    result = await db.users.update_one(
        {"user_id": my_id},
        {"$set": {"is_premium": True}}
    )
    
    if result.modified_count == 0:
        # Agar already premium hai ya user nahi mila
        pass
        
    return {"message": "Welcome to Premium!", "is_premium": True}

@api.post("/api/razorpay-webhook")
async def razorpay_webhook(request: Request, x_razorpay_signature: str = Header(None)):
    # 1. Razorpay se aaya hua raw data read karein
    payload = await request.body()
    
    # 2. Apna Webhook Secret yahan daalein (Jo Razorpay dashboard me set kiya tha)
    secret = "MySecretKey123" 
    
    # 3. Security Check: Validate Signature (Taaki koi fake payment na bhej sake)
    expected_signature = hmac.new(
        bytes(secret, 'utf-8'),
        msg=payload,
        digestmod=hashlib.sha256
    ).hexdigest()

    if expected_signature != x_razorpay_signature:
        raise HTTPException(status_code=400, detail="Invalid Razorpay Signature")

    # 4. Data ko JSON mein convert karein
    data = json.loads(payload)
    event = data.get("event")

    # 5. Agar event payment success ka hai
    if event in ["payment.captured", "payment.link.paid"]:
        try:
            # User ka email nikalein (Jo app se link mein attach ho kar aaya tha)
            user_email = data["payload"]["payment"]["entity"]["email"]
            
            # 6. Database mein is_premium = True kar dein
            if user_email:
                result = await db.users.update_one(
                    {"email": user_email},
                    {"$set": {"is_premium": True}}
                )
                print(f"✅ Premium unlocked successfully for: {user_email}")
                
        except KeyError:
            print("⚠️ Email not found in Razorpay payload")

    return {"status": "ok"}

class VerifyPaymentBody(BaseModel):
    email: str    

@api.post("/verify-payment")
async def verify_payment(body: VerifyPaymentBody, user: dict = Depends(get_current_user)):
    # 1. Database mein us user ka record find karein
    user_db = await db.users.find_one({"email": body.email})
    
    # 2. Agar user nahi milta
    if not user_db:
        raise HTTPException(status_code=404, detail="User not found")
        
    # 3. Check karein ki Webhook ne is_premium ko True kiya ya nahi
    if user_db.get("is_premium") == True:
        return {
            "success": True, 
            "message": "Premium verified successfully!"
        }
    else:
        # Agar payment process mein delay hai ya webhook nahi aaya abhi tak
        return {
            "success": False, 
            "message": "Payment not received yet. Please wait a minute or refresh."
        }

@api.get("/api/jobs/for-you")
async def for_you(user: dict = Depends(get_current_user)):
    try:
        user_branch = user.get("branch", [])
        user_qual = user.get("qualification", [])

        # Agar user ki branch/qualification list hai ya string, usko safely handle karein
        branch_list = user_branch if isinstance(user_branch, list) else [user_branch] if user_branch else []
        qual_list = user_qual if isinstance(user_qual, list) else [user_qual] if user_qual else []

        query = {"is_active": True}
        
        if branch_list or qual_list:
            or_conditions = []
            if branch_list:
                or_conditions.append({"branches": {"$in": branch_list}})
            if qual_list:
                or_conditions.append({"qualifications": {"$in": qual_list}})
            query["$or"] = or_conditions

        jobs = await db.jobs.find(query, {"_id": 0}).sort("trending_score", -1).limit(20).to_list(20)

        # 🚀 Agar match karne par ek bhi job na mile, toh latest 20 active jobs bhej do taaki 404 error kabhi na aaye!
        if not jobs:
            jobs = await db.jobs.find({"is_active": True}, {"_id": 0}).sort("trending_score", -1).limit(20).to_list(20)

        return {"jobs": jobs}
        
    except Exception as e:
        # Koi bhi error ho, app crash na ho isliye safe khali list bhej do
        return {"jobs": []}

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# 1. Get Ads (Users aur Admin ke liye)
@api.get("/ads")
async def get_ads():
    ad_doc = await db.ads.find_one(sort=[('_id', -1)])
    if ad_doc and "ads" in ad_doc:
        ad_doc["_id"] = str(ad_doc["_id"])
        return ad_doc
    return {"ads": []}

# 2. Save Ads (Admin Panel se save karne ke liye)
@api.post("/admin/ads")
async def save_admin_ads(request: Request, admin: dict = Depends(require_admin)):
    data = await request.json()
    ads_data = data.get("ads", [])
    
    await db.ads.delete_many({})
    await db.ads.insert_one({"ads": ads_data})
    
    return {"success": True, "message": "Ads updated successfully"}

# 3. Upload Ad Image (Gallery preview ke liye)
@api.post("/admin/upload-ad-image")
async def upload_ad_image(request: Request, image: UploadFile = File(...), admin: dict = Depends(require_admin)):
    if not image:
        raise HTTPException(status_code=400, detail="No image file provided")
    
    file_path = os.path.join(UPLOAD_DIR, image.filename)
    
    # File save karna
    with open(file_path, "wb") as buffer:
        buffer.write(await image.read())
    
    # URL generate karna (Apne server ka IP/domain name yahan theek karein agar zarurat ho)
    image_url = f"{request.base_url}uploads/{image.filename}"
    
    return {"success": True, "url": image_url}                                                                   


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