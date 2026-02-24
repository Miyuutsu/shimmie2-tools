"""
Captcha and Anti-Bot handling logic.
Edit COOKIES_FILE to point to your Netscape-formatted cookies.txt
"""
import re
import hashlib
from pathlib import Path
from http.cookiejar import MozillaCookieJar
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================
# USER CONFIGURATION
# ==========================================
COOKIES_FILE = "cookies.txt"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
# ==========================================

class AntiBotSolver:
    """Handles PoW challenges for the target site."""
    def detect(self, html):
        """Detects if the response is a challenge page."""
        return "challenge-container" in html and "powSeed" in html

    def solve(self, session, response_text, current_url):
        """Solves the SHA1 PoW challenge."""
        print("[!] Anti-Bot Challenge triggered. Solving...")
        try:
            challenge_id = re.search(r'const challenge_id = "(.*?)";', response_text).group(1)
            challenge_gen = re.search(r'const challenge_generated = "(.*?)";', response_text).group(1)
            cookie_expires = re.search(r'const challenge_cookie_expires = "(.*?)";', response_text).group(1)
            pow_seed = re.search(r'const powSeed = "(.*?)";', response_text).group(1)
            prefix_match = re.search(r'const powPrefix = "(.*?)";', response_text)
            target_prefix = prefix_match.group(1) if prefix_match else "00000"
        except AttributeError:
            print("[!] Failed to parse challenge parameters.")
            return False

        nonce = 0
        h = ""
        while True:
            candidate = f"{pow_seed}:{nonce}"
            h = hashlib.sha1(candidate.encode()).hexdigest()
            if h.startswith(target_prefix):
                break
            nonce += 1
            if nonce > 5000000: # Safety break
                print("[!] Failed to solve PoW (Nonce limit reached).")
                return False

        payload = {
            "challenge_id": challenge_id,
            "challenge_generated": challenge_gen,
            "challenge_cookie_expires": cookie_expires,
            "pow_nonce": str(nonce),
            "pow_hash": h
        }
        headers = {
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "X-Verification-Challenge": "1",
            "Referer": current_url,
            "User-Agent": USER_AGENT
        }
        try:
            resp = session.post(current_url, json=payload, headers=headers)
            if resp.status_code == 200:
                print("[✓] Challenge solved successfully.")
                return True
        except Exception as e:
            print(f"[!] Error posting challenge solution: {e}")
            return False
        return False

def get_protected_session():
    """Initializes a robust session with cookies and retries."""
    session = requests.Session()

    # Retry Strategy
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    # Headers & Cookies
    session.headers.update({"User-Agent": USER_AGENT})

    if Path(COOKIES_FILE).exists():
        try:
            cj = MozillaCookieJar(COOKIES_FILE)
            cj.load(ignore_discard=True, ignore_expires=True)
            session.cookies = cj
            print(f"[INFO] Loaded cookies from {COOKIES_FILE}")
        except Exception as e:
            print(f"[WARNING] Failed to load cookies: {e}")
    else:
        print(f"[INFO] No cookies found at {COOKIES_FILE}. Proceeding without auth.")

    return session
