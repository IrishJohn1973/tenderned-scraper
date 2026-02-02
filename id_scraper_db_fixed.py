#!/usr/bin/env python3
"""
TenderNed ID-based Scraper with Supabase - FIXED VERSION
=========================================================
Extracts ALL available fields from the TenderNed API.
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime

import requests
import psycopg2
from psycopg2.extras import Json

# PDF parser
try:
    from pdf_parser import TenderNedPDFParser
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class IDScraperDBFixed:
    """Scrapes TenderNed by publication ID with FULL field extraction."""

    API_URL = "https://www.tenderned.nl/papi/tenderned-rs-tns/v2/publicaties"

    def __init__(self, db_config: dict):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Valan/1.0'})

        self.conn = psycopg2.connect(**db_config)
        self.conn.autocommit = False
        logger.info("Database connected")

        self.stats = {
            'checked': 0,
            'found': 0,
            'tenders': 0,
            'awards': 0,
            'updated': 0,
            'pdf_enriched': 0,
            'not_found': 0,
            'errors': 0,
            'db_errors': 0,
        }
        self._last_request = 0

    def _rate_limit(self, delay: float = 0.15):
        elapsed = time.time() - self._last_request
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.time()

    def is_award(self, pub: dict) -> bool:
        """Determine if publication is an award notice."""
        type_pub = pub.get('typePublicatie', '')
        type_str = type_pub if isinstance(type_pub, str) else str(type_pub)
        award_keywords = ['gegund', 'gunning', 'award', 'resultaat', 'aanbesteding gegund']
        return any(k in type_str.lower() for k in award_keywords)

    def extract_cpv_codes(self, pub: dict) -> tuple:
        """Extract CPV codes array and primary code."""
        cpv_list = pub.get('cpvCodes', []) or []
        codes = []
        primary = None
        for cpv in cpv_list:
            code = cpv.get('code', '')
            if code:
                codes.append(code)
                if cpv.get('isHoofdOpdracht', False):
                    primary = code
        if codes and not primary:
            primary = codes[0]
        return codes, primary

    def extract_nuts_codes(self, pub: dict) -> list:
        """Extract NUTS codes."""
        nuts_list = pub.get('nutsCodes', []) or []
        return [n.get('code', '') for n in nuts_list if n.get('code')]

    def extract_buyer_name(self, pub: dict) -> str:
        """Extract buyer name from various fields."""
        # Try opdrachtgeverNaam first (most reliable)
        buyer = pub.get('opdrachtgeverNaam', '')
        if buyer:
            return buyer
        # Try aanbestedendeDienst
        ad = pub.get('aanbestedendeDienst', {})
        if isinstance(ad, dict):
            return ad.get('naam', '') or ad.get('name', '')
        elif isinstance(ad, str):
            return ad
        return ''

    def extract_procedure(self, pub: dict) -> str:
        """Extract procurement procedure/method."""
        proc = pub.get('procedureCode', {})
        if isinstance(proc, dict):
            return proc.get('omschrijving', '') or proc.get('code', '')
        return str(proc) if proc else ''

    def extract_contract_type(self, pub: dict) -> str:
        """Extract contract type (services, goods, works)."""
        type_code = pub.get('typeOpdrachtCode', {})
        if isinstance(type_code, dict):
            return type_code.get('omschrijving', '') or type_code.get('code', '')
        return str(type_code) if type_code else ''

    def enrich_pdf(self, pub_id: int) -> dict:
        """Get supplier info from PDF for awards."""
        if not PDF_AVAILABLE:
            return {}
        self._rate_limit(0.2)
        try:
            r = self.session.get(f"{self.API_URL}/{pub_id}/pdf", timeout=60)
            if r.status_code == 200:
                result = TenderNedPDFParser.parse_pdf_bytes(r.content)
                if result.get('extraction_success'):
                    self.stats['pdf_enriched'] += 1
                    return {
                        'supplier_name': result.get('supplier_name'),
                        'kvk_number': result.get('kvk_number'),
                        'award_value': result.get('award_value'),
                    }
        except Exception as e:
            logger.debug(f"PDF error {pub_id}: {e}")
        return {}

    def fetch_publication(self, pub_id: int) -> dict:
        """Fetch publication from API."""
        self._rate_limit(0.15)
        try:
            r = self.session.get(f"{self.API_URL}/{pub_id}", timeout=30)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                self.stats['not_found'] += 1
        except Exception as e:
            self.stats['errors'] += 1
            logger.debug(f"Fetch error {pub_id}: {e}")
        return None

    def upsert_tender(self, pub: dict):
        """Insert or update tender with ALL fields."""
        pub_id = pub.get('publicatieId')
        cpv_codes, cpv_primary = self.extract_cpv_codes(pub)
        nuts_codes = self.extract_nuts_codes(pub)
        buyer_name = self.extract_buyer_name(pub)
        procedure = self.extract_procedure(pub)
        contract_type = self.extract_contract_type(pub)

        # Notice type
        type_pub = pub.get('typePublicatie', '')
        notice_type = type_pub if isinstance(type_pub, str) else str(type_pub)

        # Build source_metadata
        metadata = {
            'kenmerk': pub.get('kenmerk'),
            'procedure': procedure,
            'tedNummer': pub.get('pbNummerTed'),
            'isDigitaal': pub.get('isDigitaalInschrijvenMogelijk'),
            'typeOpdracht': contract_type,
            'publicatieCode': pub.get('publicatieCode'),
            'referentieNummer': pub.get('referentieNummer'),
            'gerelateerdePublicaties': pub.get('gerelateerdePublicaties'),
        }

        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tenderned_tenders
                    (source, source_id, title, short_description, buyer_name, buyer_country,
                     cpv_codes, cpv_primary, nuts_codes, notice_type, procurement_method,
                     contract_type, published_at, deadline, is_above_threshold, 
                     ted_nummer, kenmerk, detail_url, source_metadata, fetched_at)
                    VALUES ('tenderned', %s, %s, %s, %s, 'NL', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, source_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        short_description = EXCLUDED.short_description,
                        buyer_name = EXCLUDED.buyer_name,
                        cpv_codes = EXCLUDED.cpv_codes,
                        cpv_primary = EXCLUDED.cpv_primary,
                        nuts_codes = EXCLUDED.nuts_codes,
                        notice_type = EXCLUDED.notice_type,
                        procurement_method = EXCLUDED.procurement_method,
                        contract_type = EXCLUDED.contract_type,
                        deadline = EXCLUDED.deadline,
                        is_above_threshold = EXCLUDED.is_above_threshold,
                        ted_nummer = EXCLUDED.ted_nummer,
                        kenmerk = EXCLUDED.kenmerk,
                        source_metadata = EXCLUDED.source_metadata,
                        updated_at = NOW()
                """, (
                    str(pub_id),
                    pub.get('aanbestedingNaam') or pub.get('titel') or '',
                    pub.get('opdrachtBeschrijving', '')[:2000] if pub.get('opdrachtBeschrijving') else None,
                    buyer_name,
                    cpv_codes if cpv_codes else None,
                    cpv_primary,
                    nuts_codes if nuts_codes else None,
                    notice_type,
                    procedure,
                    contract_type,
                    pub.get('publicatieDatum'),
                    pub.get('sluitingsDatum'),
                    pub.get('europees', False) or pub.get('nationaalOfEuropeesCode', {}).get('code') == 'EU',
                    pub.get('pbNummerTed'),
                    str(pub.get('kenmerk')) if pub.get('kenmerk') else None,
                    f"https://www.tenderned.nl/aankondigingen/overzicht/{pub_id}",
                    Json(metadata),
                    datetime.now()
                ))
            self.conn.commit()
            self.stats['tenders'] += 1
        except Exception as e:
            self.conn.rollback()
            self.stats['db_errors'] += 1
            logger.error(f"DB error tender {pub_id}: {e}")

    def upsert_award(self, pub: dict, winner_data: dict = None):
        """Insert or update award with ALL fields."""
        pub_id = pub.get('publicatieId')
        cpv_codes, cpv_primary = self.extract_cpv_codes(pub)
        nuts_codes = self.extract_nuts_codes(pub)
        buyer_name = self.extract_buyer_name(pub)
        procedure = self.extract_procedure(pub)
        contract_type = self.extract_contract_type(pub)
        winner_data = winner_data or {}

        # Notice type
        type_pub = pub.get('typePublicatie', '')
        notice_type = type_pub if isinstance(type_pub, str) else str(type_pub)

        metadata = {
            'kenmerk': pub.get('kenmerk'),
            'procedure': procedure,
            'tedNummer': pub.get('pbNummerTed'),
            'typeOpdracht': contract_type,
            'publicatieCode': pub.get('publicatieCode'),
            'gerelateerdePublicaties': pub.get('gerelateerdePublicaties'),
        }

        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tenderned_awards
                    (source, source_id, title, short_description, buyer_name, buyer_country,
                     cpv_codes, cpv_primary, procurement_method,
                     award_date, is_above_threshold, ted_nummer, kenmerk, detail_url,
                     supplier_name, kvk_number, award_value, source_metadata, fetched_at)
                    VALUES ('tenderned', %s, %s, %s, %s, 'NL', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, source_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        short_description = EXCLUDED.short_description,
                        buyer_name = EXCLUDED.buyer_name,
                        cpv_codes = EXCLUDED.cpv_codes,
                        cpv_primary = EXCLUDED.cpv_primary,
                        procurement_method = EXCLUDED.procurement_method,
                        is_above_threshold = EXCLUDED.is_above_threshold,
                        ted_nummer = EXCLUDED.ted_nummer,
                        kenmerk = EXCLUDED.kenmerk,
                        supplier_name = COALESCE(EXCLUDED.supplier_name, tenderned_awards.supplier_name),
                        kvk_number = COALESCE(EXCLUDED.kvk_number, tenderned_awards.kvk_number),
                        award_value = COALESCE(EXCLUDED.award_value, tenderned_awards.award_value),
                        source_metadata = EXCLUDED.source_metadata,
                        updated_at = NOW()
                """, (
                    str(pub_id),
                    pub.get('aanbestedingNaam') or pub.get('titel') or '',
                    pub.get('opdrachtBeschrijving', '')[:2000] if pub.get('opdrachtBeschrijving') else None,
                    buyer_name,
                    cpv_codes if cpv_codes else None,
                    cpv_primary,
                    procedure,
                    pub.get('publicatieDatum'),
                    pub.get('europees', False) or pub.get('nationaalOfEuropeesCode', {}).get('code') == 'EU',
                    pub.get('pbNummerTed'),
                    str(pub.get('kenmerk')) if pub.get('kenmerk') else None,
                    f"https://www.tenderned.nl/aankondigingen/overzicht/{pub_id}",
                    winner_data.get('supplier_name'),
                    winner_data.get('kvk_number'),
                    winner_data.get('award_value'),
                    Json(metadata),
                    datetime.now()
                ))
            self.conn.commit()
            self.stats['awards'] += 1
        except Exception as e:
            self.conn.rollback()
            self.stats['db_errors'] += 1
            logger.error(f"DB error award {pub_id}: {e}")

    def run(self, start_id: int, end_id: int, update_existing: bool = False):
        """Run the scraper."""
        logger.info(f"Scraping IDs {start_id} down to {end_id}")
        logger.info(f"Update existing: {update_existing}")

        start_time = datetime.now()
        current_id = start_id
        consecutive_404 = 0

        try:
            while current_id >= end_id:
                self.stats['checked'] += 1

                # Check if exists (skip if not updating)
                if not update_existing:
                    try:
                        with self.conn.cursor() as cur:
                            cur.execute(
                                "SELECT 1 FROM tenderned_tenders WHERE source_id = %s "
                                "UNION SELECT 1 FROM tenderned_awards WHERE source_id = %s",
                                (str(current_id), str(current_id))
                            )
                            if cur.fetchone():
                                current_id -= 1
                                consecutive_404 = 0
                                continue
                    except:
                        pass

                pub = self.fetch_publication(current_id)

                if pub is None:
                    consecutive_404 += 1
                    if consecutive_404 >= 1000:
                        logger.warning(f"200 consecutive 404s at ID {current_id}, stopping")
                        break
                    current_id -= 1
                    continue

                consecutive_404 = 0
                self.stats['found'] += 1

                is_award = self.is_award(pub)
                if is_award:
                    winner_data = self.enrich_pdf(current_id)
                    self.upsert_award(pub, winner_data)
                else:
                    self.upsert_tender(pub)

                # Progress every 500
                if self.stats['found'] % 500 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds() / 60
                    rate = self.stats['found'] / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"ID {current_id} | Found:{self.stats['found']:,} | "
                        f"T:{self.stats['tenders']} A:{self.stats['awards']} | "
                        f"PDF:{self.stats['pdf_enriched']} | {rate:.0f}/min"
                    )

                current_id -= 1

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.conn.close()

        elapsed = (datetime.now() - start_time).total_seconds() / 60
        logger.info(f"Completed in {elapsed:.1f} minutes")
        self.print_stats()

    def print_stats(self):
        print("\n" + "=" * 60)
        print("TENDERNED SCRAPER - FIXED VERSION")
        print("=" * 60)
        print(f"  IDs checked:   {self.stats['checked']:,}")
        print(f"  Found:         {self.stats['found']:,}")
        print(f"  Tenders:       {self.stats['tenders']:,}")
        print(f"  Awards:        {self.stats['awards']:,}")
        print(f"  PDF enriched:  {self.stats['pdf_enriched']:,}")
        print(f"  DB errors:     {self.stats['db_errors']:,}")
        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='TenderNed ID Scraper - FIXED')
    parser.add_argument('--start', type=int, default=420000, help='Start ID')
    parser.add_argument('--end', type=int, default=100000, help='End ID')
    parser.add_argument('--update', action='store_true', help='Update existing records')
    args = parser.parse_args()

    # Handle empty string case for port (GitHub Actions sets empty secrets as "")
    port_str = os.environ.get('VALAN_DB_PORT', '') or '5432'

    db_config = {
        'host': os.environ.get('VALAN_DB_HOST', ''),
        'port': int(port_str),
        'dbname': os.environ.get('VALAN_DB_NAME', 'postgres'),
        'user': os.environ.get('VALAN_DB_USER', 'postgres'),
        'password': os.environ.get('VALAN_DB_PASSWORD', ''),
        'connect_timeout': 30,
    }

    # Validate required credentials
    if not db_config['host'] or not db_config['password']:
        logger.error("Missing required database credentials. Set VALAN_DB_HOST and VALAN_DB_PASSWORD environment variables.")
        sys.exit(1)

    scraper = IDScraperDBFixed(db_config=db_config)
    scraper.run(start_id=args.start, end_id=args.end, update_existing=args.update)


if __name__ == '__main__':
    main()
