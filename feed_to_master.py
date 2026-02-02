#!/usr/bin/env python3
"""
TenderNed Feed to Master Tables
================================
Push tenderned_tenders and tenderned_awards to master tables.

Usage:
    python feed_to_master.py              # Feed all
    python feed_to_master.py --tenders    # Feed only tenders
    python feed_to_master.py --awards     # Feed only awards
    python feed_to_master.py --dry-run    # Show counts without feeding
"""

import os
import sys
import argparse
import logging
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_db_config():
    # Handle empty string case for port (GitHub Actions sets empty secrets as "")
    port_str = os.environ.get('VALAN_DB_PORT', '') or '5432'
    return {
        'host': os.environ.get('VALAN_DB_HOST'),
        'port': int(port_str),
        'dbname': os.environ.get('VALAN_DB_NAME'),
        'user': os.environ.get('VALAN_DB_USER'),
        'password': os.environ.get('VALAN_DB_PASSWORD'),
    }


def feed_to_master(tenders=True, awards=True, dry_run=False):
    """Feed TenderNed data to master tables."""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    print('=' * 60)
    print('TENDERNED - FEED TO MASTER TABLES')
    print('=' * 60)

    # Get counts before
    cur.execute("SELECT COUNT(*) FROM master_tenders WHERE source = 'tenderned'")
    before_tenders = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM master_awards WHERE source = 'tenderned'")
    before_awards = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) FROM tenderned_tenders')
    source_tenders = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM tenderned_awards')
    source_awards = cur.fetchone()[0]

    print(f'\nSource tables:')
    print(f'  tenderned_tenders: {source_tenders:,}')
    print(f'  tenderned_awards:  {source_awards:,}')
    print(f'\nMaster tables (before):')
    print(f'  master_tenders (NL): {before_tenders:,}')
    print(f'  master_awards (NL):  {before_awards:,}')

    if dry_run:
        print('\n[DRY RUN] No changes made.')
        conn.close()
        return

    results = {}

    if tenders:
        print('\n📥 Feeding tenders to master...')
        cur.execute('SELECT feed_tenderned_tenders_to_master()')
        results['tenders'] = cur.fetchone()[0]
        print(f'   Tenders fed: {results["tenders"]:,}')

    if awards:
        print('\n📥 Feeding awards to master...')
        cur.execute('SELECT feed_tenderned_awards_to_master()')
        results['awards'] = cur.fetchone()[0]
        print(f'   Awards fed: {results["awards"]:,}')

    conn.commit()

    # Get counts after
    cur.execute("SELECT COUNT(*) FROM master_tenders WHERE source = 'tenderned'")
    after_tenders = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM master_awards WHERE source = 'tenderned'")
    after_awards = cur.fetchone()[0]

    print('\n' + '=' * 60)
    print('RESULTS')
    print('=' * 60)
    print(f'Master tenders (NL): {before_tenders:,} → {after_tenders:,} (+{after_tenders - before_tenders:,})')
    print(f'Master awards (NL):  {before_awards:,} → {after_awards:,} (+{after_awards - before_awards:,})')

    # Total master counts
    cur.execute('SELECT COUNT(*) FROM master_tenders')
    total_tenders = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM master_awards')
    total_awards = cur.fetchone()[0]
    print(f'\nTotal in master_tenders: {total_tenders:,}')
    print(f'Total in master_awards:  {total_awards:,}')

    conn.close()
    print('\n✅ Feed complete!')


def main():
    parser = argparse.ArgumentParser(description='Feed TenderNed data to master tables')
    parser.add_argument('--tenders', action='store_true', help='Feed only tenders')
    parser.add_argument('--awards', action='store_true', help='Feed only awards')
    parser.add_argument('--dry-run', action='store_true', help='Show counts without feeding')
    args = parser.parse_args()

    # If neither specified, do both
    if not args.tenders and not args.awards:
        args.tenders = True
        args.awards = True

    feed_to_master(
        tenders=args.tenders,
        awards=args.awards,
        dry_run=args.dry_run
    )


if __name__ == '__main__':
    main()
