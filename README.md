# Kinoteka Director

This repository automates the fetching and publishing of movies.

## Components
1.  **Scraper (Python)**: Fetches new movies from YTS/IMDb and saves to `latest_movies.json`.
2.  **Publisher (Rust)**: Reads `latest_movies.json` and pushes them to the Iroh Network.

## Setup
1.  Push this to GitHub.
2.  Go to **Settings > Secrets**.
3.  Add `PUBLISHER_SECRET`: This must be the **Hex-encoded 32-byte private key** of your Director Identity.
    *   If you don't have one, run the publisher locally once, it generates `publisher_data/secret_key`.
    *   Convert that binary file to Hex (e.g. `xxd -p publisher_data/secret_key`).
    *   Paste that Hex string into GitHub Secrets.
