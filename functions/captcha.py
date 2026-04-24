"""
Captcha and Anti-Bot handling logic.
Edit COOKIES_FILE to point to your Netscape-formatted cookies.txt
"""
import re
import hashlib
from pathlib import Path
from http.cookiejar import MozillaCookieJar, LoadError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================
# USER CONFIGURATION
# ==========================================
COOKIES_FILE = "cookies.txt"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/115.0.0.0 Safari/537.36"
)
# ==========================================

class AntiBotSolver:
    """Handles PoW challenges for the target site."""
    def detect(self, html):
        """Detects if the response is a challenge page."""
        return "challenge-container" in html and "powSeed" in html

    def _extract_params(self, text):
        """Extracts regex parameters to reduce local variables in solve."""
        try:
            return {
                "id": re.search(r'const challenge_id = "(.*?)";', text).group(1),
                "gen": re.search(r'const challenge_generated = "(.*?)";', text).group(1),
                "exp": re.search(r'const challenge_cookie_expires = "(.*?)";', text).group(1),
                "seed": re.search(r'const powSeed = "(.*?)";', text).group(1),
                "prefix": (
                    re.search(r'const powPrefix = "(.*?)";', text).group(1)
                    if "powPrefix" in text else "00000"
                )
            }
        except AttributeError:
            return None

    def solve(self, session, response_text, current_url):
        """Solves the SHA1 PoW challenge."""
        print("[!] Anti-Bot Challenge triggered. Solving...")
        params = self._extract_params(response_text)
        if not params:
            print("[!] Failed to parse challenge parameters.")
            return False

        nonce = 0
        pow_hash = ""
        while True:
            candidate = f"{params['seed']}:{nonce}"
            pow_hash = hashlib.sha1(candidate.encode()).hexdigest()
            if pow_hash.startswith(params['prefix']):
                break
            nonce += 1
            if nonce > 5000000: # Safety break
                print("[!] Failed to solve PoW (Nonce limit reached).")
                return False

        payload = {
            "challenge_id": params['id'],
            "challenge_generated": params['gen'],
            "challenge_cookie_expires": params['exp'],
            "pow_nonce": str(nonce),
            "pow_hash": pow_hash
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
        except requests.RequestException as e:
            print(f"[!] Error posting challenge solution: {e}")
            return False
        return False

def get_protected_session(cookie_file=None):
    """Initializes a robust session with cookies and retries."""
    session = requests.Session()

    # Retry Strategy
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    # Headers & Cookies
    session.headers.update({"User-Agent": USER_AGENT})

    if cookie_file and Path(cookie_file).exists():
        try:
            cj = MozillaCookieJar(COOKIES_FILE)
            cj.load(ignore_discard=True, ignore_expires=True)
            session.cookies = cj
            print(f"[INFO] Loaded cookies from {COOKIES_FILE}")
        except (LoadError, OSError) as e:
            print(f"[WARNING] Failed to load cookies: {e}")
    elif cookie_file:
        print(f"[WARNING] No cookies found at {cookie_file}. Proceeding without auth.")

    return session
