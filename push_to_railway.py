import asyncio
import uuid
from datetime import datetime, timezone, date, timedelta
import httpx
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient

# 1. Yahan apna Railway wala asli MongoDB Connection URL dalein (mongodb+srv://...)
MONGO_URL = "mongodb+srv://careerpulseadmin:Piyushkumar7870079084@cluster0.1ohzpns.mongodb.net/?appName=Cluster0" 

async def push_jobs():
    print("Railway Database se connect ho rahe hain...")
    client = AsyncIOMotorClient(MONGO_URL)
    db = client.careerpulse # Aapke database ka naam
    
    # Simple test job data taaki turant pata chale ki app mein data aa gaya
    sample_jobs = [
        {
            "job_id": f"job_{uuid.uuid4().hex[:12]}",
            "organization": "SSC",
            "post_name": "SSC JE Admit Card 2026 Paper-1",
            "category": "Admit Card",
            "post_type": "Admit Card",
            "branches": ["Civil Engineering", "Electrical Engineering"],
            "qualifications": ["Diploma", "BTech"],
            "vacancies": "968",
            "salary": "Level 6",
            "eligibility": "Diploma/Degree in Engineering",
            "location": "All India",
            "last_date": (date.today() + timedelta(days=15)).isoformat(),
            "apply_link": "https://www.freejobalert.com",
            "min_age": 18,
            "max_age": 30,
            "description": "SSC Junior Engineer Admit Card is out.",
            "is_active": True,
            "source": "manual-push",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "job_id": f"job_{uuid.uuid4().hex[:12]}",
            "organization": "RRB",
            "post_name": "RRB JE Result 2025-26 CBT-1 Declared",
            "category": "Result",
            "post_type": "Result",
            "branches": ["Computer Science", "Mechanical Engineering"],
            "qualifications": ["BTech", "Diploma"],
            "vacancies": "7934",
            "salary": "Level 7",
            "eligibility": "Engineering Degree",
            "location": "All India",
            "last_date": (date.today() + timedelta(days=20)).isoformat(),
            "apply_link": "https://www.freejobalert.com",
            "min_age": 18,
            "max_age": 33,
            "description": "RRB JE CBT-1 Result has been declared online.",
            "is_active": True,
            "source": "manual-push",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "job_id": f"job_{uuid.uuid4().hex[:12]}",
            "organization": "ISRO",
            "post_name": "ISRO Scientist/Engineer Exam Date 2026",
            "category": "Upcoming Exam",
            "post_type": "Upcoming Exam",
            "branches": ["Electronics Engineering", "Mechanical Engineering"],
            "qualifications": ["BTech"],
            "vacancies": "242",
            "salary": "Level 10",
            "eligibility": "B.E/B.Tech in relevant branch",
            "location": "All India",
            "last_date": (date.today() + timedelta(days=30)).isoformat(),
            "apply_link": "https://www.freejobalert.com",
            "min_age": 21,
            "max_age": 35,
            "description": "ISRO has announced the exam date for Scientist posts.",
            "is_active": True,
            "source": "manual-push",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    ]

    print("Sample Jobs Railway Database mein daal rahe hain...")
    for j in sample_jobs:
        # Check duplicate
        exists = await db.jobs.find_one({"post_name": j["post_name"]})
        if not exists:
            await db.jobs.insert_one(j)
            print(f"Added: {j['post_name']} [{j['post_type']}]")
            
    print("✅ Kaam ho gaya! Ab app refresh karke dekhein.")

if __name__ == "__main__":
    asyncio.run(push_jobs())