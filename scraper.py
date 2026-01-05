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
YTS_API_BASE = "https://yts.lt/api/v2"
IMDB_API_BASE = "https://api.imdbapi.dev"
OUTPUT_PATH = "fulldatabase.json"
LATEST_PATH = "latest_movies.json"


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


def fetch_imdb_plot(imdb_id: str) -> Optional[str]:
    """Fetch plot from IMDb API."""
    try:
        url = f"{IMDB_API_BASE}/titles/{imdb_id}"
        print(f"  [INFO] Fetching IMDb plot for {imdb_id}: {url}")
        resp = requests.get(url, headers={"accept": "application/json"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("plot")
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
    args = sys.argv[1:]
    run_once = "--once" in args
    
    print("=== Subtitle Monitor Daemon (Python) ===")
    if run_once:
        print("Mode: Single Run (GitHub Actions)")
    else:
        print("Mode: Daemon (Checking every 60 minutes...)")
    
    # Load existing data
    results: Dict[str, Any] = {}
    if os.path.exists(OUTPUT_PATH):
        print(f"Loading existing progress from {OUTPUT_PATH}...")
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            results = json.load(f)
    
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
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] Checking for updates...")
        
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
                            # Prune unnecessary fields to save space
                            yts_data.pop("date_uploaded", None)
                            yts_data.pop("date_uploaded_unix", None)
                            yts_data.pop("background_image_original", None)
                            
                            # Prune screenshots if they exist
                            for i in range(1, 4):
                                yts_data.pop(f"medium_screenshot_image{i}", None)
                                yts_data.pop(f"large_screenshot_image{i}", None)

                            if yts_data.get("torrents"):
                                for torrent in yts_data["torrents"]:
                                    torrent.pop("seeds", None)
                                    torrent.pop("peers", None)
                                    torrent.pop("date_uploaded", None)
                                    torrent.pop("date_uploaded_unix", None)

                            yts_data["title"] = clean_text(yts_data.get("title", ""))
                            if yts_data.get("description_full"):
                                yts_data["description_full"] = clean_text(yts_data["description_full"])
                            
                            time.sleep(0.5)
                            plot_en = fetch_imdb_plot(imdb_id)
                            
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
                                "yts_data": yts_data,
                            }
                            results[imdb_id] = entry
                            new_count += 1
                        else:
                            print(f"  [SKIP] Movie not found on YTS: {cleaned_title} ({imdb_id})")
                    elif is_new_sub:
                        print(f"  [+] New subtitle for existing movie: {imdb_id}")
                        new_count += 1
                
                if new_count > 0:
                    print(f"Found {new_count} new items. Saving database...")
                    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
                        json.dump(results, f, indent=2, ensure_ascii=False)
                    
                    # Generate latest feed
                    print("Generating latest_movies.json...")
                    all_movies = list(results.values())
                    all_movies.sort(key=lambda x: x.get("date_uploaded", ""), reverse=True)
                    latest = all_movies[:50]
                    with open(LATEST_PATH, "w", encoding="utf-8") as f:
                        json.dump(latest, f, indent=2, ensure_ascii=False)
                else:
                    print("No new items found.")
        
        except Exception as e:
            print(f"Error fetching updates: {e}")
        
        if run_once:
            print("Single run complete. Exiting.")
            break
        
        print("Sleeping for 60 minutes...")
        time.sleep(60 * 60)


if __name__ == "__main__":
    main()
