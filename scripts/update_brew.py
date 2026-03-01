
import hashlib
import urllib.request
import sys
import re
from pathlib import Path

FORMULA_PATH = Path("Formula/iceberg-meta.rb")
REPO_URL = "https://github.com/MandlaSibanda196/iceberg-meta"

def calculate_sha256(url):
    print(f"Downloading {url}...")
    sha256_hash = hashlib.sha256()
    try:
        with urllib.request.urlopen(url) as response:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                sha256_hash.update(chunk)
    except urllib.error.HTTPError as e:
        print(f"Failed to download {url}: {e}")
        sys.exit(1)
    return sha256_hash.hexdigest()

def update_formula(version, sha256):
    if not FORMULA_PATH.exists():
        print(f"Error: {FORMULA_PATH} not found.")
        sys.exit(1)

    content = FORMULA_PATH.read_text()
    
    # Update URL
    new_url = f"{REPO_URL}/archive/refs/tags/{version}.tar.gz"
    
    # Regex to replace the url line
    content = re.sub(r'url ".*?"', f'url "{new_url}"', content)
    # Regex to replace the sha256 line
    content = re.sub(r'sha256 ".*?"', f'sha256 "{sha256}"', content)
    
    FORMULA_PATH.write_text(content)
    print(f"Updated {FORMULA_PATH} to {version} with SHA {sha256}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/update_brew.py <version>")
        sys.exit(1)
        
    version = sys.argv[1]
    # Ensure version starts with v if not present, though tags usually have it.
    # The script assumes the tag includes 'v' if the tag on github has it.
    
    url = f"{REPO_URL}/archive/refs/tags/{version}.tar.gz"
    sha = calculate_sha256(url)
    update_formula(version, sha)
