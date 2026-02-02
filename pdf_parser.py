#!/usr/local/opt/python@3.11/bin/python3.11
"""
TenderNed PDF Parser
====================
Extracts winner/supplier information from TenderNed award PDF documents.

The JSON API does NOT include winner data - it's only in the PDFs.
This module parses award PDFs to extract:
- Winner company name (supplier_name)
- KVK registration number
- Award value (if available)
"""

import re
import logging
from typing import Dict, Any, Optional, List
import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


class TenderNedPDFParser:
    """Parser for TenderNed award PDF documents."""

    # Patterns for winner extraction - Dutch language
    # TenderNed PDFs use: "Winnaar:\nOfficiële naam: \n[Company Name]"
    # Note: Has ligature ﬃ (U+FB03) and punctuation space \u2008
    WINNER_PATTERNS = [
        # PRIMARY: Simple pattern that works with special chars
        # "Winnaar:" followed by any "naam:" line, then the company name
        r'Winnaar:\s*\n.{0,30}naam:.{0,5}\n([^\n]+)',

        # Informatie over winnaars section
        r'Informatie over winnaars\s*\nWinnaar:\s*\n.{0,30}naam:.{0,5}\n([^\n]+)',

        # Just look for "naam:" after Winnaar and grab next line
        r'Winnaar:.*?naam:.{0,5}\n([^\n]+)',

        # Contractant format (older documents)
        r'Contractant[^\n]*\n.{0,30}naam:.{0,5}\n([^\n]+)',
        r'Contractant:?\s*\n([^\n]+(?:B\.?V\.?|N\.?V\.?))',

        # Direct mentions with company suffix
        r'Naam onderneming[^\n]*:\s*\n?([^\n]+)',
        r'Winnende inschrijver[^\n]*:\s*\n?([^\n]+)',
        r'(?:gegund aan|opdracht aan)[:\s]+([^\n]+(?:B\.?V\.?|N\.?V\.?))',
    ]

    # KVK (Chamber of Commerce) number patterns
    KVK_PATTERNS = [
        r'KVK[- ]?(?:nummer)?[:\s]*([0-9]{8})',
        r'Registratienummer[:\s]*([0-9]{8})',
        r'Handelsregister[:\s]*([0-9]{8})',
        r'Chamber of Commerce[:\s]*([0-9]{8})',
    ]

    # Award value patterns
    # TenderNed uses "5 000 000 Euro" format with spaces in numbers
    # Also uses special punctuation space \u2008
    VALUE_PATTERNS = [
        # Framework/maximum value patterns (common in TenderNed)
        r'Maximumwaarde.{0,40}:\s*\n?([0-9][0-9 \u2008]+)\s*Euro',
        r'raamovereenkomst.{0,20}:\s*\n?([0-9][0-9 \u2008]+)\s*Euro',

        # Standard value patterns
        r'Waarde van de\s*\n?aanbesteding.{0,10}:\s*\n?([0-9][0-9 \u2008.,]+)\s*Euro',
        r'Waarde van het\s*\n?contract.{0,10}:\s*\n?([0-9][0-9 \u2008.,]+)\s*(?:Euro|EUR)',
        r'Totale waarde.{0,20}:\s*\n?([0-9][0-9 \u2008.,]+)\s*(?:Euro|EUR)',
        r'Geraamde waarde.{0,20}:\s*\n?([0-9][0-9 \u2008.,]+)\s*(?:Euro|EUR)',
        r'Geraamde totale.{0,20}:\s*\n?([0-9][0-9 \u2008.,]+)\s*(?:Euro|EUR)',
        r'Raming van de totale\s*\n?waarde.{0,10}:\s*\n?([0-9][0-9 \u2008.,]+)\s*(?:Euro|EUR)',

        # General pattern: number with spaces followed by Euro (catches most formats)
        # Must be > 3 digits to avoid matching "1 Euro" placeholders
        r'([0-9]{1,3}(?:[\s\u2008][0-9]{3})+)\s*Euro',

        # Euro symbol patterns
        r'(?:Waarde|Totale waarde|Geraamde waarde)[^\n]*?€\s*([0-9][0-9 \u2008.,]+)',
        r'(?:Waarde|Totale waarde|Geraamde waarde)[^\n]*?EUR\s*([0-9][0-9 \u2008.,]+)',
    ]

    # Email patterns
    EMAIL_PATTERNS = [
        r'E-?mail[:\s]*([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)',
        r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)',
    ]

    # Address patterns
    ADDRESS_PATTERNS = [
        r'(?:Adres|Postadres)[:\s]*\n([^\n]+)\n([0-9]{4}\s*[A-Z]{2})\s+([^\n]+)',
    ]

    @classmethod
    def parse_pdf_bytes(cls, pdf_bytes: bytes) -> Dict[str, Any]:
        """
        Parse PDF from bytes and extract winner information.

        Args:
            pdf_bytes: Raw PDF content

        Returns:
            Dict with extracted fields: supplier_name, kvk_number, award_value, email, etc.
        """
        result = {
            'supplier_name': None,
            'kvk_number': None,
            'award_value': None,
            'supplier_email': None,
            'supplier_address': None,
            'supplier_city': None,
            'supplier_postal_code': None,
            'extraction_success': False,
            'raw_text_sample': None,
        }

        try:
            # Open PDF from bytes
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")

            # Extract all text
            full_text = ''
            for page in doc:
                full_text += page.get_text() + '\n'
            doc.close()

            # Store sample for debugging
            result['raw_text_sample'] = full_text[:500]

            # Extract winner name
            result['supplier_name'] = cls._extract_winner_name(full_text)

            # Extract KVK number
            result['kvk_number'] = cls._extract_kvk(full_text)

            # Extract award value
            result['award_value'] = cls._extract_value(full_text)

            # Extract email
            result['supplier_email'] = cls._extract_email(full_text)

            # Extract address
            address_info = cls._extract_address(full_text)
            if address_info:
                result.update(address_info)

            # Mark success if we got at least the company name
            if result['supplier_name']:
                result['extraction_success'] = True

        except Exception as e:
            logger.error(f"PDF parsing error: {e}")
            result['error'] = str(e)

        return result

    @classmethod
    def parse_pdf_file(cls, file_path: str) -> Dict[str, Any]:
        """
        Parse PDF from file path.

        Args:
            file_path: Path to PDF file

        Returns:
            Dict with extracted fields
        """
        with open(file_path, 'rb') as f:
            return cls.parse_pdf_bytes(f.read())

    @classmethod
    def _extract_winner_name(cls, text: str) -> Optional[str]:
        """Extract winner company name from text."""
        for pattern in cls.WINNER_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                name = match.group(1).strip()
                # Clean up the name
                name = cls._clean_company_name(name)
                if name and len(name) > 3 and 'geen' not in name.lower():
                    return name[:200]  # Limit length
        return None

    @classmethod
    def _extract_kvk(cls, text: str) -> Optional[str]:
        """Extract KVK registration number."""
        for pattern in cls.KVK_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                kvk = match.group(1).strip()
                # KVK numbers are exactly 8 digits
                if len(kvk) == 8 and kvk.isdigit():
                    return kvk
        return None

    @classmethod
    def _extract_value(cls, text: str) -> Optional[float]:
        """Extract award value in EUR."""
        for pattern in cls.VALUE_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value_str = match.group(1).strip()
                try:
                    # Remove spaces (TenderNed uses "5 000 000" format)
                    value_str = value_str.replace(' ', '').replace('\u2008', '')

                    # Handle Dutch number format (1.234,56 or 1,234.56)
                    # Check if comma is decimal separator
                    if ',' in value_str and '.' in value_str:
                        if value_str.rindex(',') > value_str.rindex('.'):
                            # Dutch format: 1.234,56
                            value_str = value_str.replace('.', '').replace(',', '.')
                        else:
                            # US format: 1,234.56
                            value_str = value_str.replace(',', '')
                    elif ',' in value_str:
                        # Could be Dutch decimal or US thousands
                        if len(value_str.split(',')[-1]) == 2:
                            # Likely decimal: 1234,56
                            value_str = value_str.replace(',', '.')
                        else:
                            # Likely thousands: 1,234
                            value_str = value_str.replace(',', '')

                    value = float(value_str)
                    # Sanity check - values should be reasonable
                    if 100 < value < 1_000_000_000:
                        return value
                except (ValueError, IndexError):
                    continue
        return None

    @classmethod
    def _extract_email(cls, text: str) -> Optional[str]:
        """Extract email address."""
        # Look near winner section first
        winner_section = re.search(r'(Winnaar|Contractant|Opdrachtnemer).{0,500}', text, re.IGNORECASE | re.DOTALL)
        search_text = winner_section.group(0) if winner_section else text

        for pattern in cls.EMAIL_PATTERNS:
            match = re.search(pattern, search_text, re.IGNORECASE)
            if match:
                email = match.group(1).lower().strip()
                # Basic email validation
                if '@' in email and '.' in email.split('@')[1]:
                    return email[:100]
        return None

    @classmethod
    def _extract_address(cls, text: str) -> Optional[Dict[str, str]]:
        """Extract address information."""
        for pattern in cls.ADDRESS_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return {
                    'supplier_address': match.group(1).strip()[:200],
                    'supplier_postal_code': match.group(2).strip(),
                    'supplier_city': match.group(3).strip()[:100],
                }
        return None

    @classmethod
    def _clean_company_name(cls, name: str) -> str:
        """Clean up extracted company name."""
        # Remove common prefixes/suffixes
        name = re.sub(r'^(Officiële naam:|Naam:|De winnaar is:?)\s*', '', name, flags=re.IGNORECASE)

        # Remove trailing punctuation except company suffixes
        name = re.sub(r'[,;:\.\s]+$', '', name)

        # Remove page artifacts
        name = re.sub(r'\d+\s*van\s*\d+$', '', name)  # "5 van 10"
        name = re.sub(r'^\d+\s*', '', name)  # Leading page numbers

        # Normalize whitespace
        name = ' '.join(name.split())

        return name.strip()


def extract_winners_from_awards(db_connection=None, limit: int = None) -> Dict[str, int]:
    """
    Process all awards without supplier data and extract from PDFs.

    Args:
        db_connection: Database connection (optional)
        limit: Maximum awards to process

    Returns:
        Stats dict with counts
    """
    import requests

    stats = {
        'processed': 0,
        'success': 0,
        'no_pdf': 0,
        'parse_failed': 0,
        'already_has_data': 0,
    }

    # API client for fetching PDFs
    session = requests.Session()
    session.headers.update({'User-Agent': 'Valan/1.0'})

    base_url = 'https://www.tenderned.nl/papi/tenderned-rs-tns/v2/publicaties'

    # For now, process from API directly
    # TODO: Integrate with database to update existing records

    page = 0
    processed = 0

    while True:
        if limit and processed >= limit:
            break

        params = {'page': page, 'size': 100}
        r = session.get(base_url, params=params, timeout=30)
        data = r.json()

        for pub in data.get('content', []):
            if limit and processed >= limit:
                break

            type_pub = pub.get('typePublicatie', {})
            type_str = type_pub.get('omschrijving', '') if isinstance(type_pub, dict) else str(type_pub)

            # Only process award notices
            if not ('gegund' in type_str.lower() or 'gunning' in type_str.lower()):
                continue

            pub_id = pub.get('publicatieId')
            stats['processed'] += 1
            processed += 1

            try:
                # Fetch PDF
                pdf_url = f'{base_url}/{pub_id}/pdf'
                pdf_r = session.get(pdf_url, timeout=60)

                if pdf_r.status_code != 200:
                    stats['no_pdf'] += 1
                    continue

                # Parse PDF
                result = TenderNedPDFParser.parse_pdf_bytes(pdf_r.content)

                if result['extraction_success']:
                    stats['success'] += 1
                    logger.info(f"{pub_id}: {result['supplier_name']} (KVK: {result['kvk_number']})")

                    # TODO: Update database record
                    # UPDATE tenderned_awards SET
                    #   supplier_name = result['supplier_name'],
                    #   kvk_number = result['kvk_number'],
                    #   award_value = result['award_value'],
                    #   ...
                    # WHERE source_id = pub_id
                else:
                    stats['parse_failed'] += 1

            except Exception as e:
                logger.error(f"Error processing {pub_id}: {e}")
                stats['parse_failed'] += 1

        # Check for more pages
        if not data.get('content'):
            break

        page += 1

        if page % 10 == 0:
            logger.info(f"Page {page}: {stats}")

    return stats


if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    print("TenderNed PDF Parser Test")
    print("=" * 60)

    # Test with a few award PDFs
    stats = extract_winners_from_awards(limit=20)

    print()
    print("=" * 60)
    print("Results:")
    print(f"  Processed: {stats['processed']}")
    print(f"  Success:   {stats['success']}")
    print(f"  No PDF:    {stats['no_pdf']}")
    print(f"  Failed:    {stats['parse_failed']}")
    print("=" * 60)
