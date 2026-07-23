import requests

BASE_URL = "https://careerpulse.fun"  # Aapka live URL

def test_database():
    print("Database check kar rahe hain...")
    res = requests.get(f"{BASE_URL}/api/jobs?limit=5")
    print("Jobs API Response Status:", res.status_code)
    print("Response Data:", res.json())

if __name__ == "__main__":
    test_database()