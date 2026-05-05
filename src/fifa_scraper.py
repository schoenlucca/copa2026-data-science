"""
src/fifa_scraper.py
-------------------
Fetches the current FIFA Men's World Ranking via Wikipedia.

Why Wikipedia instead of fifa.com?
    The official FIFA website renders data via JavaScript and blocks
    automated requests. Wikipedia's ranking table is updated regularly
    and returns clean static HTML — ideal for BeautifulSoup scraping.

Table structure on Wikipedia (index 0):
    Multi-row header with columns: Rank | Change | Team | Confederation | Points
    We skip the 'Change' column (index 1) and extract the other four.
"""

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path

URL         = "https://en.wikipedia.org/wiki/FIFA_World_Rankings"
OUTPUT_PATH = Path("data/raw/fifa_ranking_live.csv")
HEADERS     = {"User-Agent": "Mozilla/5.0 Chrome/124.0.0.0"}


def fetch_page() -> BeautifulSoup:
    print(f"Fetching: {URL}")
    r = requests.get(URL, headers=HEADERS, timeout=15)
    r.raise_for_status()
    print(f"  Status: {r.status_code} OK")
    return BeautifulSoup(r.text, "html.parser")


def parse_ranking_table(soup: BeautifulSoup) -> pd.DataFrame:
    """
    Parses table index 0 — the current top 20 ranking.

    Columns in HTML: Rank | Change(▲▼) | Team | Confederation | Points
    We skip Change (index 1) since it's not useful for our analysis.
    """
    tables = soup.find_all("table", {"class": "wikitable"})
    table  = tables[0]

    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue

        cell_texts = []
        for cell in cells:
            text = cell.get_text(separator=" ", strip=True)
            text = re.sub(r'\[.*?\]', '', text).strip()
            cell_texts.append(text)

        # Data rows: first cell is a number (the rank)
        if cell_texts and cell_texts[0].isdigit():
            rows.append(cell_texts)

    if not rows:
        raise ValueError("No data rows found. Wikipedia page structure may have changed.")

    clean_rows = []
    for row in rows:
        if len(row) >= 4:
            # Columns: rank | previous_rank | team | points
            # We skip previous_rank (index 1) — not needed for analysis
            clean_rows.append([row[0], row[2], row[3]])

    df = pd.DataFrame(clean_rows, columns=["rank", "team", "total_points"])

    # Clean and convert
    df["rank"]         = pd.to_numeric(df["rank"], errors="coerce")
    df["total_points"] = pd.to_numeric(
        df["total_points"].str.replace(",", ""), errors="coerce"
    )
    df = df.dropna(subset=["rank"]).copy()
    df["rank"] = df["rank"].astype(int)
    df = df.sort_values("rank").reset_index(drop=True)

    return df


def scrape_fifa_ranking() -> pd.DataFrame:
    """Main pipeline: fetch → parse → save."""
    print("=" * 50)
    print("FIFA World Ranking Scraper (via Wikipedia)")
    print("=" * 50)

    soup = fetch_page()
    df   = parse_ranking_table(soup)

    df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["source"]       = "Wikipedia — FIFA World Rankings"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"\n✓ Saved → {OUTPUT_PATH}")
    print(f"  {df.shape[0]} teams x {df.shape[1]} columns\n")
    print(df[["rank", "team", "total_points"]].to_string(index=False))

    return df


if __name__ == "__main__":
    df = scrape_fifa_ranking()