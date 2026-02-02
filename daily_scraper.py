#!/usr/bin/env python3
"""
TenderNed Daily Scraper
=======================
Checks for new publications above the current max ID in database.
Run via cron daily at 6 AM UTC.
"""

import os
import sys
import time
import logging
from datetime import datetime

import requests
import psycopg2

try:
    from pdf_parser import TenderNedPDFParser
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


class DailyScraper:
    """Scrapes new TenderNed publications since last run."""

    API_URL = "https://www.tenderned.nl/papi/tenderned-rs-tns/v2/publicaties"

    def __init__(self, db_config: dict):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Valan/1.0"})
        
        self.conn = psycopg2.connect(**db_config)
        self.conn.autocommit = False
        logger.info("Database connected")

        self.stats = {"checked": 0, "found": 0, "tenders": 0, "awards": 0, "pdf_enriched": 0}
        self._last_request = 0

    def _rate_limit(self, delay: float = 0.15):
        elapsed = time.time() - self._last_request
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.time()

    def get_max_id_in_db(self) -> int:
        """Get the highest publication ID we have."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT GREATEST(
                    COALESCE((SELECT MAX(source_id::int) FROM tenderned_tenders WHERE source = 'tenderned'), 0),
                    COALESCE((SELECT MAX(source_id::int) FROM tenderned_awards WHERE source = 'tenderned'), 0)
                )
            """)
            result = cur.fetchone()[0]
            return result or 392000

    def get_latest_api_id(self) -> int:
        """Get the latest publication ID from TenderNed API."""
        self._rate_limit()
        try:
            r = self.session.get(self.API_URL, params={"page": 0, "size": 1}, timeout=30)
            if r.status_code == 200:
                data = r.json()
                content = data.get("content", [])
                if content:
                    return content[0].get("publicatieId", 0)
        except Exception as e:
            logger.error(f"Error getting latest ID: {e}")
        return 0

    def is_award(self, pub: dict) -> bool:
        type_pub = pub.get("typePublicatie", {})
        type_str = type_pub.get("omschrijving", "") if isinstance(type_pub, dict) else str(type_pub)
        return any(k in type_str.lower() for k in ["gegund", "gunning", "award"])

    def enrich_pdf(self, pub_id: int) -> dict:
        if not PDF_AVAILABLE:
            return {}
        self._rate_limit(0.2)
        try:
            r = self.session.get(f"{self.API_URL}/{pub_id}/pdf", timeout=60)
            if r.status_code == 200:
                result = TenderNedPDFParser.parse_pdf_bytes(r.content)
                if result.get("extraction_success"):
                    self.stats["pdf_enriched"] += 1
                    return {
                        "supplier_name": result.get("supplier_name"),
                        "kvk_number": result.get("kvk_number"),
                        "award_value": result.get("award_value"),
                    }
        except:
            pass
        return {}

    def fetch_publication(self, pub_id: int) -> dict:
        self._rate_limit(0.15)
        try:
            r = self.session.get(f"{self.API_URL}/{pub_id}", timeout=30)
            if r.status_code == 200:
                return r.json()
        except:
            pass
        return None

    def insert_tender(self, pub: dict):
        pub_id = pub.get("publicatieId")
        buyer = pub.get("aanbestedendeDienst", {}) or {}
        buyer_name = buyer.get("naam", "") if isinstance(buyer, dict) else str(buyer)
        type_pub = pub.get("typePublicatie", {})
        notice_type = type_pub.get("omschrijving", "") if isinstance(type_pub, dict) else str(type_pub)

        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tenderned_tenders
                    (source, source_id, title, buyer_name, buyer_country, notice_type,
                     published_at, deadline, is_above_threshold, detail_url, fetched_at)
                    VALUES ('tenderned', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, source_id) DO NOTHING
                """, (
                    str(pub_id),
                    pub.get("aanbestedingNaam") or pub.get("titel") or "",
                    buyer_name, "NL", notice_type,
                    pub.get("publicatieDatum"), pub.get("sluitingsDatum"),
                    pub.get("europees", False),
                    f"https://www.tenderned.nl/aankondigingen/overzicht/{pub_id}",
                    datetime.now()
                ))
            self.conn.commit()
            self.stats["tenders"] += 1
        except Exception as e:
            self.conn.rollback()
            logger.debug(f"DB error tender {pub_id}: {e}")

    def insert_award(self, pub: dict, winner_data: dict = None):
        pub_id = pub.get("publicatieId")
        buyer = pub.get("aanbestedendeDienst", {}) or {}
        buyer_name = buyer.get("naam", "") if isinstance(buyer, dict) else str(buyer)
        winner_data = winner_data or {}

        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tenderned_awards
                    (source, source_id, title, buyer_name, buyer_country,
                     award_date, is_above_threshold, detail_url,
                     supplier_name, kvk_number, award_value, fetched_at)
                    VALUES ('tenderned', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, source_id) DO NOTHING
                """, (
                    str(pub_id),
                    pub.get("aanbestedingNaam") or pub.get("titel") or "",
                    buyer_name, "NL",
                    pub.get("publicatieDatum"), pub.get("europees", False),
                    f"https://www.tenderned.nl/aankondigingen/overzicht/{pub_id}",
                    winner_data.get("supplier_name"), winner_data.get("kvk_number"),
                    winner_data.get("award_value"), datetime.now()
                ))
            self.conn.commit()
            self.stats["awards"] += 1
        except Exception as e:
            self.conn.rollback()
            logger.debug(f"DB error award {pub_id}: {e}")

    def run(self):
        max_db_id = self.get_max_id_in_db()
        latest_api_id = self.get_latest_api_id()

        if latest_api_id <= max_db_id:
            logger.info(f"No new publications. DB max: {max_db_id}, API latest: {latest_api_id}")
            return

        logger.info(f"Scanning IDs {latest_api_id} down to {max_db_id + 1}")
        
        current_id = latest_api_id
        consecutive_404 = 0

        while current_id > max_db_id:
            self.stats["checked"] += 1
            pub = self.fetch_publication(current_id)

            if pub is None:
                consecutive_404 += 1
                if consecutive_404 >= 50:
                    logger.warning(f"50 consecutive 404s at {current_id}, stopping")
                    break
                current_id -= 1
                continue

            consecutive_404 = 0
            self.stats["found"] += 1

            is_award = self.is_award(pub)
            if is_award:
                winner_data = self.enrich_pdf(current_id)
                self.insert_award(pub, winner_data)
            else:
                self.insert_tender(pub)

            current_id -= 1

        logger.info(f"Daily scrape complete: {self.stats}")
        self.conn.close()


def main():
    # Handle empty string case for port (GitHub Actions sets empty secrets as "")
    port_str = os.environ.get("VALAN_DB_PORT", "") or "5432"

    db_config = {
        "host": os.environ.get("VALAN_DB_HOST", ""),
        "port": int(port_str),
        "dbname": os.environ.get("VALAN_DB_NAME", "postgres"),
        "user": os.environ.get("VALAN_DB_USER", "postgres"),
        "password": os.environ.get("VALAN_DB_PASSWORD", ""),
        "connect_timeout": 30,
    }

    # Validate required credentials
    if not db_config["host"] or not db_config["password"]:
        logger.error("Missing required database credentials. Set VALAN_DB_HOST and VALAN_DB_PASSWORD environment variables.")
        sys.exit(1)

    logger.info("=== TenderNed Daily Scraper ===")
    scraper = DailyScraper(db_config=db_config)
    scraper.run()


if __name__ == "__main__":
    main()
