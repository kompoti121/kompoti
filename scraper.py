#!/usr/bin/env python3
"""
Kinoteka Scraper - Python version using cloudscraper to bypass Cloudflare.
"""

import os
import sys
import json
import re
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import cloudscraper
import requests
from bs4 import BeautifulSoup


# Constants
YTS_API_BASE = "https://yts.lt/api/v2"  # Will be updated dynamically
IMDB_API_BASE = "https://api.imdbapi.dev"
OUTPUT_PATH = "fulldatabase.json"
LATEST_PATH = "latest_movies.json"


def get_current_yts_domain() -> str:
    """Fetch the current official YTS domain from yifystatus.com."""
    try:
        scraper = cloudscraper.create_scraper()
        resp = scraper.get("https://yifystatus.com/")
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")
            # Look for "Current official domain" text
            target_text = soup.find(string=re.compile(r"Current official domain"))
            if target_text:
                # The next sibling or parent might contain the link
                # Structure is usually: <span>Current official domain: <a href="...">YTS.LT</a></span>
                parent = target_text.find_parent()
                link = parent.find("a") if parent else None
                if link and link.get("href"):
                    domain = link.get("href").rstrip("/")
                    print(f"  [INFO] Detected global YTS domain: {domain}")
                    return domain
    except Exception as e:
        print(f"  [WARN] Failed to fetch YTS domain from yifystatus.com: {e}")
    
    print("  [WARN] Fallback to default YTS domain: https://yts.lt")
    return "https://yts.lt"


def make_relative(url: str, base_url: str) -> str:
    """Convert an absolute URL to a relative one if it matches the base URL."""
    if not url:
        return url
    if url.startswith(base_url):
        return url[len(base_url):]
    return url


def clean_yts_data(data: Dict[str, Any], base_url: str) -> Dict[str, Any]:
    """Clean YTS data and convert URLs to relative paths."""
    # Convert main fields
    fields_to_relativize = [
        "url",
        "background_image", 
        "small_cover_image", 
        "medium_cover_image", 
        "large_cover_image"
    ]
    
    for field in fields_to_relativize:
        if data.get(field):
            data[field] = make_relative(data[field], base_url)
            
    # Convert screenshots
    for i in range(1, 4):
        key = f"large_screenshot_image{i}"
        if data.get(key):
            data[key] = make_relative(data[key], base_url)
        key = f"medium_screenshot_image{i}"
        if data.get(key):
            data[key] = make_relative(data[key], base_url)

    # Convert torrents
    if data.get("torrents"):
        for torrent in data["torrents"]:
            if torrent.get("url"):
                torrent["url"] = make_relative(torrent["url"], base_url)
                
    # Prune unnecessary fields
    data.pop("date_uploaded", None)
    data.pop("date_uploaded_unix", None)
    data.pop("background_image_original", None)
    
    # Prune seeds/peers from torrents
    if data.get("torrents"):
        for torrent in data["torrents"]:
            torrent.pop("seeds", None)
            torrent.pop("peers", None)
            torrent.pop("date_uploaded", None)
            torrent.pop("date_uploaded_unix", None)

    data["title"] = clean_text(data.get("title", ""))
    if data.get("description_full"):
        data["description_full"] = clean_text(data["description_full"])
        
    return data


def clean_text(text: str) -> str:
    """Clean whitespace from text."""
    return " ".join(text.replace("\n", " ").replace("\t", " ").split())


def fetch_yts_movie(imdb_id: str) -> Optional[Dict[str, Any]]:
    """Fetch movie details from YTS API."""
    try:
        # Step 1: Find movie ID
        list_url = f"{YTS_API_BASE}/list_movies.json?query_term={imdb_id}"
        resp = requests.get(list_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        movies = data.get("data", {}).get("movies", [])
        if not movies:
            return None
        
        movie_id = None
        for m in movies:
            if m.get("imdb_code") == imdb_id:
                movie_id = m.get("id")
                break
        
        if not movie_id:
            return None
        
        # Step 2: Get full details
        details_url = f"{YTS_API_BASE}/movie_details.json?movie_id={movie_id}&with_images=true&with_cast=true"
        resp = requests.get(details_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        return data.get("data", {}).get("movie")
    except Exception as e:
        print(f"  [ERROR] YTS API error: {e}")
        return None


def fetch_imdb_data(imdb_id: str) -> Optional[Dict[str, Any]]:
    """Fetch movie data from IMDb API."""
    try:
        url = f"{IMDB_API_BASE}/titles/{imdb_id}"
        print(f"  [INFO] Fetching IMDb data for {imdb_id}: {url}")
        resp = requests.get(url, headers={"accept": "application/json"}, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  [ERROR] IMDb API error: {e}")
        return None


def translate_with_gemini(text: str, api_key: str) -> Optional[str]:
    """Translate text to Albanian using Gemini API."""
    if not text or not text.strip():
        return None
    
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
        prompt = f"Translate the following movie synopsis into Albanian. Return only the translated text.\n\n{text}"
        
        payload = {
            "contents": [{
                "parts": [{"text": prompt}]
            }]
        }
        
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=30)
        
        if not resp.ok:
            print(f"  [ERROR] Gemini API Error: {resp.status_code} - {resp.text}")
            return None
        
        data = resp.json()
        candidates = data.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if parts:
                return parts[0].get("text", "").strip()
        return None
    except Exception as e:
        print(f"  [ERROR] Gemini translation error: {e}")
        return None


def parse_subtitles(html: str) -> list:
    """Parse subtitles from OpenSubtitles HTML page."""
    soup = BeautifulSoup(html, "lxml")
    subtitles = []
    
    # Check if it's a TV series page (skip those)
    if "http://schema.org/TVSeries" in html:
        print("  [DEBUG] Filtered: TV Series detected")
        return []
    
    for h in soup.select("h1, h2"):
        text = h.get_text()
        if "Season" in text or "Episode" in text or "TV Series" in text:
            print("  [DEBUG] Filtered: TV Series in header")
            return []
    
    table = soup.select_one("table#search_results")
    if not table:
        print("  [DEBUG] No table#search_results found")
        return []
    
    imdb_re = re.compile(r"tt(\d+)")
    
    for row in table.select("tr[id^='name']"):
        row_id_str = row.get("id", "name0").replace("name", "")
        try:
            sub_id = int(row_id_str)
        except ValueError:
            sub_id = 0
        
        # Extract IMDb ID
        imdb_id = None
        for a in row.select("a[href*='imdb.com/title/tt']"):
            href = a.get("href", "")
            match = imdb_re.search(href)
            if match:
                imdb_id = f"tt{match.group(1)}"
                break
        
        # Get movie name and check if it's a subtitle link
        main_link = row.select_one("a.bnone, a[href*='/subtitles/']")
        if not main_link:
            continue
        
        href = main_link.get("href", "")
        name = clean_text(main_link.get_text())
        
        if "/subtitles/" not in href:
            continue
        
        # Check for TV series patterns in row
        row_text = row.get_text()
        if "[S" in row_text and "E" in row_text:
            print(f"  [DEBUG] Row-level TV Series filter: {name}")
            continue
        
        # Extract filename
        filename = None
        td = row.select_one("td[id^='main']")
        if td:
            span = td.select_one("span[title]")
            if span:
                filename = span.get("title")
            if not filename:
                texts = [t.strip() for t in td.stripped_strings]
                if len(texts) > 1:
                    fallback = texts[1]
                    if fallback and fallback not in ("Watch online", "Download Subtitles Searcher") and "search results" not in fallback:
                        filename = fallback
        
        if not filename:
            filename = name
        
        subtitles.append({
            "id": sub_id,
            "movie": name,
            "filename": filename,
            "imdb_id": imdb_id,
            "download_link": f"https://dl.opensubtitles.org/en/download/sub/{sub_id}",
        })
    
    return subtitles


def main():
    global YTS_API_BASE
    
    args = sys.argv[1:]
    run_once = "--once" in args
    
    # 1. Fetch current YTS domian
    current_yts_url = get_current_yts_domain()
    YTS_API_BASE = f"{current_yts_url}/api/v2"
    
    print("=== Subtitle Monitor Daemon (Python) ===")
    print(f"Using YTS Domain: {current_yts_url}")
    
    if run_once:
        print("Mode: Single Run (GitHub Actions)")
    else:
        print("Mode: Daemon (Checking every 60 minutes...)")
    
    # Load existing data
    results: Dict[str, Any] = {}
    if os.path.exists(OUTPUT_PATH):
        print(f"Loading existing progress from {OUTPUT_PATH}...")
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                loaded_data = json.load(f)
                # Handle old vs new format
                if "database" in loaded_data:
                    results = loaded_data["database"]
                else:
                    results = loaded_data
        except json.JSONDecodeError:
            print("  [WARN] Failed to decode existing database. Starting fresh.")
    
    print(f"Loaded database with {len(results)} movies.")
    
    # Create cloudscraper session
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'darwin',
            'mobile': False
        }
    )
    
    while True:
        now = datetime.now()
        print(f"\\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] Checking for updates...")
        
        search_url = "https://www.opensubtitles.org/en/search/sublanguageid-alb/searchonlymovies-on/offset-0/sort-5/asc-0"
        
        try:
            resp = scraper.get(search_url)
            if resp.status_code != 200:
                print(f"  [ERROR] HTTP {resp.status_code} for URL: {search_url}")
                print(f"  [ERROR] Body Snippet: {resp.text[:500]}")
            else:
                subtitles = parse_subtitles(resp.text)
                new_count = 0
                
                for sub in subtitles:
                    imdb_id = sub.get("imdb_id")
                    if not imdb_id:
                        continue
                    
                    sub_item = {
                        "id": sub["id"],
                        "filename": sub["filename"],
                        "download_link": sub["download_link"],
                    }
                    
                    is_new_movie = False
                    is_new_sub = False
                    
                    if imdb_id in results:
                        existing_ids = [s["id"] for s in results[imdb_id].get("subtitle_list", [])]
                        if sub_item["id"] not in existing_ids:
                            results[imdb_id]["subtitle_list"].append(sub_item)
                            is_new_sub = True
                    else:
                        is_new_movie = True
                    
                    if is_new_movie:
                        cleaned_title = clean_text(sub["movie"])
                        print(f"  [*] New Movie found: {cleaned_title} ({imdb_id})")
                        
                        time.sleep(1)
                        yts_data = fetch_yts_movie(imdb_id)
                        
                        if yts_data:
                            # Clean and relativize
                            yts_data = clean_yts_data(yts_data, current_yts_url)
                            
                            time.sleep(0.5)
                            imdb_full_data = fetch_imdb_data(imdb_id)
                            plot_en = imdb_full_data.get("plot") if imdb_full_data else None
                            
                            if plot_en:
                                api_key = os.environ.get("GEMINI_API_KEY")
                                if api_key:
                                    print("  [INFO] Translating plot...")
                                    translated = translate_with_gemini(plot_en, api_key)
                                    if translated:
                                        print("  [INFO] Translation successful.")
                                        yts_data["description_full"] = translated
                                    else:
                                        print("  [WARN] Translation failed. Using English.")
                                        yts_data["description_full"] = plot_en
                                else:
                                    yts_data["description_full"] = plot_en
                            
                            entry = {
                                "title": cleaned_title,
                                "year": yts_data.get("year"),
                                "subtitle_list": [sub_item],
                                "date_uploaded": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            }
                            
                            # Featured logic
                            movie_year = yts_data.get("year")
                            if movie_year in [2025, 2026] and imdb_full_data:
                                vote_count = imdb_full_data.get("rating", {}).get("voteCount")
                                if vote_count and vote_count > 7500:
                                    entry["is_featured"] = True
                                    print(f"  [FEATURED] Movie {cleaned_title} is featured (Votes: {vote_count})")
                                    
                            entry["yts_data"] = yts_data
                            results[imdb_id] = entry
                            new_count += 1
                        else:
                            print(f"  [SKIP] Movie not found on YTS: {cleaned_title} ({imdb_id})")
                    elif is_new_sub:
                        print(f"  [+] New subtitle for existing movie: {imdb_id}")
                        new_count += 1
                
                if new_count > 0:
                    print(f"Found {new_count} new items. Saving database...")
                    
                    # Normalize all data to relative paths
                    for mid, entry in results.items():
                        if entry.get("yts_data"):
                            entry["yts_data"] = clean_yts_data(entry["yts_data"], current_yts_url)
                    
                    full_output = {
                        "yts_url": current_yts_url,
                        "database": results
                    }
                    
                    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
                        json.dump(full_output, f, indent=2, ensure_ascii=False)
                    
                    # Generate latest feed
                    print("Generating latest_movies.json...")
                    all_movies = list(results.values())
                    all_movies.sort(key=lambda x: x.get("date_uploaded", ""), reverse=True)
                    latest_list = all_movies[:50]
                    
                    latest_output = {
                        "yts_url": current_yts_url,
                        "movies": latest_list
                    }
                    
                    with open(LATEST_PATH, "w", encoding="utf-8") as f:
                        json.dump(latest_output, f, indent=2, ensure_ascii=False)
                else:
                    print("No new items found.")
        
        except Exception as e:
            print(f"Error fetching updates: {e}")
            import traceback
            traceback.print_exc()
        
        if run_once:
            print("Single run complete. Exiting.")
            break
        
        print("Sleeping for 60 minutes...")
        time.sleep(60 * 60)


if __name__ == "__main__":
    main()
