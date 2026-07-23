import requests

# 1. Apni details yahan daalein
BASE_URL = "https://careerpulse.fun"  # Apne Railway server ka link daalein
ADMIN_EMAIL = "piyushkumaraaao@gmail.com"                      # Apna admin email daalein
ADMIN_PASSWORD = "Piyushkumar123@%"                      # Apna password daalein

def run_live_scraper():
    print("1. Admin Login ho raha hai...")
    
    # Login API call
    login_response = requests.post(
        f"{BASE_URL}/api/auth/login", 
        json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    )
    
    if login_response.status_code != 200:
        print("Login Failed! Error:", login_response.text)
        return

    # Token nikalna
    token = login_response.json().get("access_token") # ya "token", jo bhi aapke backend mein ho
    print("✅ Login Success! Token mil gaya.")
    
    print("\n2. Scraper ko command bhej rahe hain...")
    # Refresh jobs call with Token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    scrape_response = requests.post(f"{BASE_URL}/api/admin/refresh-jobs", headers=headers)
    
    if scrape_response.status_code == 200:
        print("✅ Success! Railway server par background scraper chal gaya hai!")
        print("Scraper Response:", scrape_response.json())
        print("\nAb 2-3 minute wait karein aur apne Phone app ko refresh karein.")
    else:
        print("❌ Scraper start nahi hua. Error:", scrape_response.text)

if __name__ == "__main__":
    run_live_scraper()