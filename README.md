# TenderNed Scraper (Netherlands)

Production-ready scraper for Dutch public procurement data from TenderNed.

## Features

- **No authentication required** - Uses public API
- **ID-based scraping** - Comprehensive coverage by iterating through publication IDs
- **PDF parsing** - Extracts supplier name, KVK number, award value from PDF documents
- **Supabase integration** - Direct database storage
- **Feed to master tables** - Push data to normalized master schema

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export VALAN_DB_HOST='your-db-host'
export VALAN_DB_PORT='5432'
export VALAN_DB_NAME='postgres'
export VALAN_DB_USER='postgres'
export VALAN_DB_PASSWORD='your-password'

# Run the scraper (scrape IDs from 420000 down to 100000)
python id_scraper_db_fixed.py --start 420000 --end 100000

# Or update existing records
python id_scraper_db_fixed.py --start 420000 --end 100000 --update

# Daily incremental scrape
python daily_scraper.py

# Feed data to master tables
python feed_to_master.py
python feed_to_master.py --dry-run  # Preview only
```

## Files

| File | Description |
|------|-------------|
| `id_scraper_db_fixed.py` | Main scraper - iterates through publication IDs |
| `pdf_parser.py` | Extracts supplier/KVK/value from award PDFs |
| `daily_scraper.py` | Daily incremental scraper |
| `feed_to_master.py` | Push data to master tables |
| `watchdog.sh` | Auto-restart wrapper script |
| `migrations/001_create_tenderned_tables.sql` | Database schema |

## Database Tables

- `tenderned_tenders` - Contract notices, prior information notices
- `tenderned_awards` - Contract award notices with supplier info

## Coverage

Typical field coverage achieved:

**Tenders:**
- title: 100%
- buyer_name: 99%+
- cpv_primary: 99%+
- published_at: 100%
- procurement_method: 99%+
- deadline: 76%

**Awards:**
- supplier_name: 36% (from PDF parsing)
- kvk_number: 36%
- award_value: 20%

## Deployment (Hetzner)

```bash
# Copy to server
scp -r . root@your-server:/root/tenderned_scraper/

# SSH and setup
ssh root@your-server
cd /root/tenderned_scraper
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run with watchdog
./watchdog.sh
```

## API Endpoints Used

- `https://www.tenderned.nl/papi/tenderned-rs-tns/v2/publicaties/{id}` - Publication details
- `https://www.tenderned.nl/papi/tenderned-rs-tns/v2/publicaties/{id}/pdf` - PDF document

No authentication required for these public endpoints.
