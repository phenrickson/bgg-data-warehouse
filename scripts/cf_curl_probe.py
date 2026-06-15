"""Throwaway probe: can curl_cffi (TLS-impersonating HTTP client, no browser)
reach BGG's Cloudflare-protected sitemapindex from a datacenter IP?

Run on a GitHub runner to get the datacenter verdict. Exits non-zero if blocked.
"""
import re
import sys

from curl_cffi import requests as cf

URL = "https://boardgamegeek.com/sitemapindex"

ok = False
for imp in ["chrome", "chrome124", "safari"]:
    try:
        r = cf.get(URL, impersonate=imp, timeout=30)
        n = len(re.findall(r"sitemap_geekitems_boardgame", r.text))
        chal = ("Just a moment" in r.text) or ("cf_chl" in r.text)
        print(f"impersonate={imp}: status={r.status_code} challenge={chal} sitemaprefs={n}")
        if r.status_code == 200 and not chal and n > 0:
            ok = True
    except Exception as e:
        print(f"impersonate={imp}: ERROR {str(e)[:120]}")

sys.exit(0 if ok else 1)
