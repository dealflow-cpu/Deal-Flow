"""
DealFlow Montana — MEGA Scraper
================================
Scrapes EVERY Montana business-for-sale source.

SOURCES (15+):
  1.  BizBuySell — bizbuysell.com/montana-businesses-for-sale/
  2.  BizQuest — bizquest.com/businesses-for-sale-in-montana-mt/
  3.  BusinessBroker.net — businessbroker.net/state/montana-businesses-for-sale.aspx
  4.  DealStream — dealstream.com/montana-businesses-for-sale
  5.  LoopNet Biz — loopnet.com/biz/montana-businesses-for-sale/
  6.  LoopNet CRE — loopnet.com/search/commercial-real-estate/mt/for-sale/
  7.  GlobalBX — globalbx.com/business/montana-businesses-for-sale.asp
  8.  Montana 406 Business Brokers — montana406businessbrokers.com/businesses-for-sale/
  9.  Synergy BB — synergybb.com/businesses-for-sale/montana/
  10. Murphy Business MT — murphybusiness.com/montana/businesses-for-sale/
  11. Transworld MT — tworld.com/locations/montana/buy-a-business/active-business-listings/
  12. Sunbelt Network MT — sunbeltnetwork.com/business-search/business-results/state-montana/
  13. Coldwell Banker Commercial MT — cbcmontana.com/property-type/business/
  14. BusinessesForSale.com — businessesforsale.com/us-businesses-for-sale/montana
  15. SearchBizBuySell via Google — catches anything else indexed

SETUP:
  pip install requests beautifulsoup4 lxml

RUN:
  python scraper.py

OUTPUT:
  listings.json — every real MT listing with actual URLs
"""

import requests
from bs4 import BeautifulSoup
import json, time, random, re, os, hashlib
from datetime import datetime
from urllib.parse import urljoin, urlparse

OUTPUT_FILE = "listings.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

def safe_int(text):
    if not text: return 0
    cleaned = re.sub(r"[^\d]", "", str(text))
    return int(cleaned) if cleaned else 0

def delay():
    time.sleep(random.uniform(1.5, 4.0))

def get_soup(url, timeout=25):
    """Fetch URL and return BeautifulSoup object, or None on failure."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        print(f"      ⚠ Fetch error: {e}")
    return None

def extract_price(text):
    """Find the first dollar amount in text."""
    m = re.search(r"\$[\d,]+(?:\.\d{2})?", text or "")
    return safe_int(m.group()) if m else 0

def extract_cf(text):
    m = re.search(r"[Cc]ash\s*[Ff]low[:\s]*\$?([\d,]+)", text or "")
    return safe_int(m.group(1)) if m else 0

def extract_rev(text):
    m = re.search(r"[Rr]evenue[:\s]*\$?([\d,]+)", text or "")
    return safe_int(m.group(1)) if m else 0

def extract_city(text):
    m = re.search(r"([\w\s\.]+),\s*(?:MT|Montana)", text or "")
    return m.group(1).strip() if m else ""

def find_links(soup, base_url, patterns):
    """Find all <a> tags whose href matches any of the given regex patterns."""
    links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        for pat in patterns:
            if re.search(pat, href, re.IGNORECASE):
                full = urljoin(base_url, href)
                if full not in [l[0] for l in links]:
                    links.append((full, a))
                break
    return links

def card_to_listing(source, url, card_el):
    """Extract listing data from a card element."""
    text = card_el.get_text(separator=" | ", strip=True) if card_el else ""
    title_el = card_el.find(["h2","h3","h4","h5"]) if card_el else None
    title = title_el.get_text(strip=True) if title_el else ""
    if not title:
        title = text.split("|")[0].strip()[:150]
    title = re.sub(r"\s+", " ", title).strip()
    if len(title) < 5:
        return None
    return {
        "source": source,
        "source_url": url,
        "title": title[:250],
        "asking_price": extract_price(text),
        "cash_flow": extract_cf(text),
        "gross_revenue": extract_rev(text),
        "city": extract_city(text),
        "state": "MT",
        "raw_text": text[:600],
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ═══════════════════════════════════════════════════════
# GENERIC PAGINATED SCRAPER
# ═══════════════════════════════════════════════════════
def scrape_paginated(name, url_template, max_pages, link_patterns, card_selectors=None):
    """
    Generic scraper for paginated listing sites.
    url_template: string with {page} placeholder, OR a list of URLs to try
    link_patterns: regex patterns to match listing detail URLs
    card_selectors: CSS selectors to try for listing cards
    """
    print(f"\n🔍 Scraping {name}...")
    listings = []
    seen_urls = set()

    urls = []
    if isinstance(url_template, list):
        urls = url_template
    else:
        urls = [url_template.format(page=p) for p in range(1, max_pages + 1)]

    for url in urls:
        print(f"   → {url}")
        soup = get_soup(url)
        if not soup:
            break

        found = 0
        base = urlparse(url).scheme + "://" + urlparse(url).netloc

        # Method 1: Find listing links by URL pattern
        links = find_links(soup, base, link_patterns)
        for link_url, link_el in links:
            if link_url in seen_urls:
                continue
            seen_urls.add(link_url)
            # Use the parent container or the link itself
            container = link_el.find_parent(["article", "div", "li"]) or link_el
            listing = card_to_listing(name, link_url, container)
            if listing:
                listings.append(listing)
                found += 1

        # Method 2: Try card selectors if no links found
        if found == 0 and card_selectors:
            for sel in card_selectors:
                cards = soup.select(sel)
                for card in cards:
                    a = card.find("a", href=True)
                    if not a:
                        continue
                    link_url = urljoin(base, a.get("href", ""))
                    if link_url in seen_urls:
                        continue
                    seen_urls.add(link_url)
                    listing = card_to_listing(name, link_url, card)
                    if listing:
                        listings.append(listing)
                        found += 1

        if found == 0:
            print(f"      No new listings found, stopping pagination.")
            break

        print(f"      Found {found} listings")
        delay()

    print(f"   ✅ {name}: {len(listings)} total")
    return listings


# ═══════════════════════════════════════════════════════
# ALL SOURCES
# ═══════════════════════════════════════════════════════

def scrape_all():
    all_listings = []

    # 1. BizBuySell
    all_listings += scrape_paginated(
        "BizBuySell",
        "https://www.bizbuysell.com/montana-businesses-for-sale/?p={page}",
        25,
        [r"/business-opportunity/", r"/Business/"],
        [".listing-card", ".search-result", "a.diamond", "a.showcase", "a.basic"]
    )

    # 2. BizQuest
    all_listings += scrape_paginated(
        "BizQuest",
        "https://www.bizquest.com/businesses-for-sale-in-montana-mt/?page={page}",
        20,
        [r"/listing/", r"/business-for-sale/"],
        [".listing-card", ".result-item", "article"]
    )

    # 3. BusinessBroker.net
    all_listings += scrape_paginated(
        "BusinessBroker.net",
        "https://www.businessbroker.net/state/montana-businesses-for-sale.aspx?page={page}",
        15,
        [r"/listing/", r"/businesses-for-sale/\d"],
        [".listing", ".result", "article", ".card"]
    )

    # 4. DealStream
    all_listings += scrape_paginated(
        "DealStream",
        "https://www.dealstream.com/montana-businesses-for-sale?page={page}",
        15,
        [r"/deal/", r"/listing/", r"/businesses-for-sale/.+/\d"],
        [".deal-card", ".listing", "article", ".card"]
    )

    # 5. LoopNet (Businesses)
    all_listings += scrape_paginated(
        "LoopNet",
        ["https://www.loopnet.com/biz/montana-businesses-for-sale/",
         "https://www.loopnet.com/biz/montana-businesses-for-sale/2/",
         "https://www.loopnet.com/biz/montana-businesses-for-sale/3/"],
        3,
        [r"/Listing/", r"/biz/\d"],
        [".placard", ".placardContent", "article"]
    )

    # 6. LoopNet (Commercial Real Estate — businesses with property)
    all_listings += scrape_paginated(
        "LoopNet CRE",
        ["https://www.loopnet.com/search/commercial-real-estate/montana/for-sale/",
         "https://www.loopnet.com/search/commercial-real-estate/montana/for-sale/2/"],
        2,
        [r"/Listing/"],
        [".placard", "article"]
    )

    # 7. GlobalBX
    all_listings += scrape_paginated(
        "GlobalBX",
        ["https://www.globalbx.com/business/montana-businesses-for-sale.asp",
         "https://www.globalbx.com/business/montana-businesses-for-sale.asp?page=2",
         "https://www.globalbx.com/business/montana-businesses-for-sale.asp?page=3"],
        3,
        [r"/listing/", r"/business/\d", r"detail"],
        [".listing", ".result", "article", "tr"]
    )

    # 8. Montana 406 Business Brokers (local MT broker)
    all_listings += scrape_paginated(
        "Montana 406 Brokers",
        ["https://montana406businessbrokers.com/businesses-for-sale/",
         "https://montana406businessbrokers.com/businesses-for-sale/page/2/",
         "https://montana406businessbrokers.com/businesses-for-sale/page/3/",
         "https://montana406businessbrokers.com/businesses-for-sale/page/4/",
         "https://montana406businessbrokers.com/businesses-for-sale/page/5/"],
        5,
        [r"/listing/", r"/business/", r"montana406"],
        ["article", ".listing", ".entry", ".post", ".et_pb_post"]
    )

    # 9. Synergy Business Brokers
    all_listings += scrape_paginated(
        "Synergy BB",
        ["https://synergybb.com/businesses-for-sale/montana/"],
        1,
        [r"/listing/", r"/business/", r"synergybb.com/.+montana"],
        [".listing", "article", ".entry"]
    )

    # 10. Murphy Business MT
    all_listings += scrape_paginated(
        "Murphy Business",
        ["https://murphybusiness.com/montana/businesses-for-sale/",
         "https://murphybusiness.com/montana/businesses-for-sale/page/2/",
         "https://murphybusiness.com/montana/businesses-for-sale/page/3/"],
        3,
        [r"/listing/", r"/business/", r"murphybusiness.com"],
        ["article", ".listing", ".entry", ".business-card"]
    )

    # 11. Transworld Business Advisors MT
    all_listings += scrape_paginated(
        "Transworld",
        ["https://www.tworld.com/locations/montana/buy-a-business/active-business-listings/"],
        1,
        [r"/listing/", r"/business/", r"tworld.com"],
        [".listing", "article", ".card", ".business-listing"]
    )

    # 12. Sunbelt Network MT
    all_listings += scrape_paginated(
        "Sunbelt Network",
        ["https://www.sunbeltnetwork.com/business-search/business-results/state-montana/",
         "https://www.sunbeltnetwork.com/state/montana/"],
        2,
        [r"/listing/", r"/business-detail/", r"sunbeltnetwork.com"],
        [".listing", ".card", "article", ".business-result"]
    )

    # 13. Coldwell Banker Commercial Montana
    all_listings += scrape_paginated(
        "CBC Montana",
        ["https://cbcmontana.com/property-type/business/",
         "https://cbcmontana.com/property-type/business/page/2/",
         "https://cbcmontana.com/property-type/business/page/3/"],
        3,
        [r"cbcmontana.com/.+", r"/property/"],
        ["article", ".listing", ".entry", ".property-card"]
    )

    # 14. BusinessesForSale.com
    all_listings += scrape_paginated(
        "BusinessesForSale.com",
        ["https://www.businessesforsale.com/us/businesses-for-sale/montana",
         "https://www.businessesforsale.com/us/businesses-for-sale/montana?page=2"],
        2,
        [r"/listing/", r"/us/.*\d"],
        [".listing", "article", ".search-result"]
    )

    # 15. BusinessForSale.com (different site)
    all_listings += scrape_paginated(
        "BusinessForSale.com",
        ["https://www.businessforsale.com/us/search/businesses-for-sale/montana"],
        1,
        [r"/listing/", r"/us/.*\d"],
        [".listing", "article"]
    )

    return all_listings


# ═══════════════════════════════════════════════════════
# ENRICHMENT — Visit each listing to get full details
# ═══════════════════════════════════════════════════════
def enrich(listing):
    url = listing.get("source_url", "")
    if not url:
        return listing
    try:
        soup = get_soup(url, timeout=20)
        if not soup:
            return listing
        text = soup.get_text(separator=" ", strip=True)

        # Financial extraction
        for field, patterns in {
            "sde": [r"(?:SDE|Seller.?s?\s*Disc)[:\s]*\$?([\d,]+)"],
            "ebitda": [r"EBITDA[:\s]*\$?([\d,]+)"],
            "ffe": [r"FF&?E[:\s]*\$?([\d,]+)"],
            "inventory": [r"[Ii]nventory[:\s]*\$?([\d,]+)"],
            "rent": [r"[Rr]ent[:\s]*\$?([\d,]+)\s*/?\s*(?:mo|month)?"],
        }.items():
            for pat in patterns:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    listing[field] = safe_int(m.group(1))
                    break

        # Revenue / cash flow if not already found
        if not listing.get("gross_revenue"):
            listing["gross_revenue"] = extract_rev(text)
        if not listing.get("cash_flow"):
            listing["cash_flow"] = extract_cf(text)
        if not listing.get("asking_price"):
            listing["asking_price"] = extract_price(text)

        # Employees
        m = re.search(r"(\d+)\s*(?:employees|staff|team\s*members)", text, re.IGNORECASE)
        if m: listing["employees"] = int(m.group(1))

        # Year established
        m = re.search(r"(?:est(?:ablished)?\.?|since|founded)\s*[:\-]?\s*(\d{4})", text, re.IGNORECASE)
        if m: listing["year_established"] = int(m.group(1))

        # City (if not found from card)
        if not listing.get("city"):
            listing["city"] = extract_city(text)

        # Description
        for sel in [".listing-description", ".description", "#description", ".details-description", 
                    ".entry-content", "article .content", "[class*='description']", ".business-description"]:
            el = soup.select_one(sel)
            if el and len(el.get_text(strip=True)) > 50:
                listing["description"] = el.get_text(strip=True)[:1200]
                break

        # Boolean flags
        tl = text.lower()
        listing["sba_prequalified"] = bool(re.search(r"sba.*(?:pre.?qual|loan|financ)", tl))
        listing["absentee"] = "absentee" in tl
        listing["real_estate_included"] = bool(re.search(r"real\s*estate\s*incl", tl))
        listing["home_based"] = bool(re.search(r"home.?based", tl))
        listing["recurring_revenue"] = "recurring" in tl

        # Industry detection
        industry_map = {
            "Restaurant": ["restaurant", "dining", "food service", "eatery", "café", "cafe", "pizz", "bar & grill"],
            "Retail": ["retail", "store", "shop", "boutique"],
            "Construction": ["construction", "contractor", "roofing", "excavat", "concrete"],
            "Healthcare": ["healthcare", "medical", "dental", "chiropractic", "veterinar", "pharmacy"],
            "Technology": ["technology", "software", "saas", "it service", "web design"],
            "Manufacturing": ["manufactur", "fabricat", "machine shop"],
            "Auto & Transport": ["auto", "car wash", "towing", "trucking", "fleet"],
            "Beauty & Personal": ["salon", "spa", "beauty", "barber"],
            "Fitness": ["fitness", "gym", "yoga", "martial art"],
            "Hotel & Lodging": ["hotel", "motel", "lodg", "inn ", "bed and breakfast"],
            "Gas Station": ["gas station", "convenience", "c-store"],
            "Cleaning": ["cleaning", "janitorial", "maid"],
            "Landscaping": ["landscap", "lawn", "tree service"],
            "Plumbing & HVAC": ["plumb", "hvac", "heating", "cooling"],
            "Real Estate": ["real estate", "property manage"],
            "Insurance": ["insurance", "agency"],
            "E-Commerce": ["e-commerce", "ecommerce", "online store", "amazon"],
            "Franchise": ["franchise"],
            "Agriculture": ["farm", "ranch", "agriculture", "livestock"],
            "Casino & Gaming": ["casino", "gaming", "poker", "keno"],
        }
        for industry, keywords in industry_map.items():
            if any(kw in tl for kw in keywords):
                listing["detected_industry"] = industry
                break

        delay()
    except Exception as e:
        print(f"      ⚠ Enrich error: {e}")
    return listing


# ═══════════════════════════════════════════════════════
# DEDUPLICATION
# ═══════════════════════════════════════════════════════
def deduplicate(listings):
    seen_urls = set()
    seen_titles = set()
    unique = []
    for l in listings:
        url = l.get("source_url", "")
        # Normalize URL
        url_clean = re.sub(r"[?#].*", "", url).rstrip("/").lower()
        if url_clean in seen_urls:
            continue
        title_key = re.sub(r"\s+", " ", l.get("title", "")).lower().strip()[:60]
        if title_key in seen_titles and len(title_key) > 15:
            continue
        seen_urls.add(url_clean)
        if title_key:
            seen_titles.add(title_key)
        unique.append(l)
    return unique


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════
def main():
    print("=" * 65)
    print("  DealFlow Montana — MEGA Scraper (15+ Sources)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    raw = scrape_all()
    print(f"\n📊 Raw listings scraped: {len(raw)}")

    unique = deduplicate(raw)
    print(f"📊 After dedup: {len(unique)}")

    # Enrich each listing
    print(f"\n🔬 Enriching {len(unique)} listings with detail pages...")
    enriched = []
    for i, l in enumerate(unique):
        short = l['title'][:55] + "..." if len(l['title']) > 55 else l['title']
        print(f"   [{i+1}/{len(unique)}] {short}")
        enriched.append(enrich(l))

    # Assign IDs
    for i, l in enumerate(enriched):
        l["id"] = f"DF-{i+1:04d}"

    # Build output
    source_counts = {}
    for l in enriched:
        s = l["source"]
        source_counts[s] = source_counts.get(s, 0) + 1

    output = {
        "scraped_at": datetime.utcnow().isoformat(),
        "total_listings": len(enriched),
        "sources": source_counts,
        "listings": enriched,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    # Summary
    print(f"\n{'=' * 65}")
    print("  RESULTS")
    print(f"{'=' * 65}")
    for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"  {source:30s} {count:>4d} listings")
    print(f"  {'─' * 40}")
    print(f"  {'TOTAL':30s} {len(enriched):>4d} unique MT listings")

    prices = [l["asking_price"] for l in enriched if l.get("asking_price", 0) > 0]
    if prices:
        print(f"\n  Price range: ${min(prices):,} — ${max(prices):,}")
        print(f"  Average: ${sum(prices)//len(prices):,}")
        print(f"  Median: ${sorted(prices)[len(prices)//2]:,}")

    cities = {}
    for l in enriched:
        c = l.get("city") or "Unknown"
        cities[c] = cities.get(c, 0) + 1
    if cities:
        print(f"\n  Top cities:")
        for city, count in sorted(cities.items(), key=lambda x: -x[1])[:20]:
            print(f"    {city}: {count}")

    with_cf = len([l for l in enriched if l.get("cash_flow", 0) > 0])
    with_rev = len([l for l in enriched if l.get("gross_revenue", 0) > 0])
    with_desc = len([l for l in enriched if l.get("description")])
    print(f"\n  Data quality:")
    print(f"    With cash flow: {with_cf}/{len(enriched)}")
    print(f"    With revenue: {with_rev}/{len(enriched)}")
    print(f"    With description: {with_desc}/{len(enriched)}")

    fsize = os.path.getsize(OUTPUT_FILE)
    print(f"\n  Output: {OUTPUT_FILE} ({fsize/1024:.1f} KB)")
    print(f"\n✅ Done. Every listing has a source_url linking to the real ad.")
    print(f"   Place listings.json next to index.html and open in Safari.")


if __name__ == "__main__":
    main()
