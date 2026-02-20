
import requests
import sys

def check_url(url):
    try:
        print(f"Checking {url}...")
        response = requests.head(url)
        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type')}")
        print(f"Content-Length: {response.headers.get('Content-Length')}")
        
        if response.status_code == 200:
            print("SUCCESS: File is being served.")
        else:
            print("ERROR: File not reachable.")
    except Exception as e:
        print(f"ERROR: Could not connect to server: {e}")

if __name__ == "__main__":
    # Default reflex port is 3000 or 8000 depending on frontend/backend
    # Frontend usually 3000, but static assets might be served by backend on 8000?
    # Reflex serves frontend on 3000 (Next.js) and backend on 8000 (FastAPI).
    # Assets are usually served by Next.js on 3000.
    check_url("http://localhost:3000/map_8.0.html")
