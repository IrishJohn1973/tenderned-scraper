-- TenderNed Source Tables Migration
-- Run this ONCE to create the source tables
-- These feed into master_tenders, master_awards, master_organizations_clean

-- ============================================================================
-- 1. TENDERNED_TENDERS - Source table for Dutch tenders
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.tenderned_tenders (
    id BIGSERIAL PRIMARY KEY,
    
    -- Source identification
    source TEXT NOT NULL DEFAULT 'tenderned',
    source_id TEXT NOT NULL,
    internal_ref TEXT,
    
    -- Title & Description (keep in Dutch)
    title TEXT NOT NULL,
    title_translated TEXT,
    short_description TEXT,
    short_description_translated TEXT,
    full_description TEXT,
    language TEXT DEFAULT 'nl',
    
    -- Buyer information
    buyer_name TEXT,
    buyer_country TEXT DEFAULT 'NL',
    buyer_city TEXT,
    buyer_region TEXT,
    buyer_postcode TEXT,
    buyer_address TEXT,
    buyer_organization_type TEXT,
    buyer_sector TEXT,
    
    -- Classification
    tbc_code TEXT,
    tbc_category TEXT,
    cpv_codes TEXT[],
    cpv_primary TEXT,
    naics_codes TEXT[],
    unspsc_codes TEXT[],
    nuts_codes TEXT[],
    
    -- Dates
    published_at TIMESTAMPTZ,
    deadline TIMESTAMPTZ,
    tender_period_start TIMESTAMPTZ,
    tender_period_end TIMESTAMPTZ,
    contract_start_date TIMESTAMPTZ,
    contract_end_date TIMESTAMPTZ,
    
    -- Value
    currency TEXT DEFAULT 'EUR',
    value_min NUMERIC(15,2),
    value_max NUMERIC(15,2),
    estimated_value NUMERIC(15,2),
    value_text TEXT,
    contract_duration_months INTEGER,
    
    -- Contract details
    contract_type TEXT,
    lot_structure TEXT,
    number_of_lots INTEGER,
    framework_agreement BOOLEAN DEFAULT FALSE,
    renewal_option BOOLEAN DEFAULT FALSE,
    procurement_method TEXT,
    
    -- SME flags
    sme_suitable BOOLEAN,
    reserved_for_sme BOOLEAN,
    reserved_for_vcse BOOLEAN,
    allows_consortia BOOLEAN DEFAULT TRUE,
    allows_subcontracting BOOLEAN DEFAULT TRUE,
    
    -- Requirements
    required_capabilities TEXT[],
    required_certifications TEXT[],
    required_turnover_min NUMERIC(15,2),
    required_experience_years INTEGER,
    security_clearance_required TEXT,
    geographic_restriction TEXT,
    
    -- Contact
    contact_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    contact_address TEXT,
    contact_organization TEXT,
    
    -- URLs
    detail_url TEXT,
    external_portal_url TEXT,
    document_urls TEXT[],
    
    -- Status
    tender_status TEXT DEFAULT 'active',
    notice_type TEXT,
    
    -- TenderNed specific
    kenmerk TEXT,                    -- Dutch reference number
    ted_nummer TEXT,                 -- TED publication number (if above threshold)
    is_above_threshold BOOLEAN,      -- TRUE = also on TED, FALSE = our value!
    is_framework BOOLEAN DEFAULT FALSE,
    is_dynamic_purchasing_system BOOLEAN DEFAULT FALSE,
    
    -- Raw data
    source_metadata JSONB,
    raw_xml TEXT,                    -- Store original XML
    
    -- Timestamps
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Feed tracking
    fed_to_master BOOLEAN DEFAULT FALSE,
    fed_to_master_at TIMESTAMPTZ,
    
    CONSTRAINT tenderned_tenders_source_id_unique UNIQUE (source, source_id)
);

-- Indexes for tenderned_tenders
CREATE INDEX IF NOT EXISTS idx_tenderned_tenders_source_id ON public.tenderned_tenders(source_id);
CREATE INDEX IF NOT EXISTS idx_tenderned_tenders_deadline ON public.tenderned_tenders(deadline) WHERE deadline IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tenderned_tenders_published ON public.tenderned_tenders(published_at);
CREATE INDEX IF NOT EXISTS idx_tenderned_tenders_cpv ON public.tenderned_tenders USING GIN(cpv_codes);
CREATE INDEX IF NOT EXISTS idx_tenderned_tenders_buyer ON public.tenderned_tenders(buyer_name);
CREATE INDEX IF NOT EXISTS idx_tenderned_tenders_status ON public.tenderned_tenders(tender_status);
CREATE INDEX IF NOT EXISTS idx_tenderned_tenders_threshold ON public.tenderned_tenders(is_above_threshold);
CREATE INDEX IF NOT EXISTS idx_tenderned_tenders_not_fed ON public.tenderned_tenders(fed_to_master) WHERE fed_to_master = FALSE;
CREATE INDEX IF NOT EXISTS idx_tenderned_tenders_kenmerk ON public.tenderned_tenders(kenmerk) WHERE kenmerk IS NOT NULL;


-- ============================================================================
-- 2. TENDERNED_AWARDS - Source table for Dutch contract awards
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.tenderned_awards (
    id BIGSERIAL PRIMARY KEY,
    
    -- Source identification
    source TEXT NOT NULL DEFAULT 'tenderned',
    source_id TEXT NOT NULL,
    internal_ref TEXT,
    tender_internal_ref TEXT,        -- Link to original tender
    
    -- Title & Description
    title TEXT NOT NULL,
    title_translated TEXT,
    short_description TEXT,
    short_description_translated TEXT,
    full_description TEXT,
    language TEXT DEFAULT 'nl',
    
    -- Buyer information
    buyer_name TEXT,
    buyer_country TEXT DEFAULT 'NL',
    buyer_city TEXT,
    buyer_region TEXT,
    buyer_organization_type TEXT,
    buyer_sector TEXT,
    buyer_email TEXT,
    buyer_phone TEXT,
    
    -- Supplier/Winner information - CRITICAL
    supplier_name TEXT,
    supplier_name_normalized TEXT,
    supplier_country TEXT DEFAULT 'NL',
    supplier_city TEXT,
    supplier_id TEXT,
    supplier_size TEXT,
    supplier_is_consortium BOOLEAN DEFAULT FALSE,
    consortium_members TEXT[],
    lead_contractor TEXT,
    supplier_email TEXT,             -- PRIMARY VALUE (may be dummy)
    supplier_phone TEXT,             -- May be dummy +31 600000000
    
    -- Dutch business identifiers - CRITICAL FOR ENRICHMENT
    kvk_number TEXT,                 -- Chamber of Commerce number (8 digits)
    btw_number TEXT,                 -- Dutch VAT number
    
    -- Award details
    award_date TIMESTAMPTZ,
    award_value NUMERIC(15,2),
    currency TEXT DEFAULT 'EUR',
    original_estimate NUMERIC(15,2),
    variance_percentage NUMERIC(8,2),
    award_status TEXT,
    number_of_bidders INTEGER,
    
    -- Classification
    tbc_code TEXT,
    tbc_category TEXT,
    cpv_codes TEXT[],
    cpv_primary TEXT,
    naics_codes TEXT[],
    unspsc_codes TEXT[],
    
    -- Contract details
    contract_start_date TIMESTAMPTZ,
    contract_end_date TIMESTAMPTZ,
    contract_duration_months INTEGER,
    contract_signed_date TIMESTAMPTZ,
    procurement_method TEXT,
    sme_suitable BOOLEAN,
    
    -- URLs
    detail_url TEXT,
    award_notice_url TEXT,
    
    -- TenderNed specific
    kenmerk TEXT,
    ted_nummer TEXT,
    is_above_threshold BOOLEAN,
    
    -- Data quality flags
    email_is_dummy BOOLEAN DEFAULT FALSE,
    phone_is_dummy BOOLEAN DEFAULT FALSE,
    needs_enrichment BOOLEAN DEFAULT FALSE,
    
    -- Raw data
    source_metadata JSONB,
    raw_xml TEXT,
    
    -- Timestamps
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Feed tracking
    fed_to_master BOOLEAN DEFAULT FALSE,
    fed_to_master_at TIMESTAMPTZ,
    fed_to_organizations BOOLEAN DEFAULT FALSE,
    fed_to_organizations_at TIMESTAMPTZ,
    
    CONSTRAINT tenderned_awards_source_id_unique UNIQUE (source, source_id)
);

-- Indexes for tenderned_awards
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_source_id ON public.tenderned_awards(source_id);
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_award_date ON public.tenderned_awards(award_date);
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_supplier ON public.tenderned_awards(supplier_name);
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_supplier_lower ON public.tenderned_awards(LOWER(TRIM(supplier_name))) WHERE supplier_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_cpv ON public.tenderned_awards USING GIN(cpv_codes);
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_value ON public.tenderned_awards(award_value) WHERE award_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_kvk ON public.tenderned_awards(kvk_number) WHERE kvk_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_email ON public.tenderned_awards(supplier_email) WHERE supplier_email IS NOT NULL AND email_is_dummy = FALSE;
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_not_fed ON public.tenderned_awards(fed_to_master) WHERE fed_to_master = FALSE;
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_needs_enrichment ON public.tenderned_awards(needs_enrichment) WHERE needs_enrichment = TRUE;
CREATE INDEX IF NOT EXISTS idx_tenderned_awards_kenmerk ON public.tenderned_awards(kenmerk) WHERE kenmerk IS NOT NULL;


-- ============================================================================
-- 3. TENDERNED_ORGANIZATIONS - Extracted winners from awards
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.tenderned_organizations (
    id BIGSERIAL PRIMARY KEY,
    
    -- Source tracking
    source TEXT NOT NULL DEFAULT 'tenderned',
    first_award_source_id TEXT,      -- First award we saw this org
    
    -- Core identification
    canonical_name TEXT,
    name_variations TEXT[],
    legal_name TEXT,
    
    -- Dutch identifiers - CRITICAL
    kvk_number TEXT,                 -- Chamber of Commerce (8 digits)
    btw_number TEXT,                 -- VAT number
    
    -- Other identifiers
    companies_house_number TEXT,
    vat_number TEXT,
    duns_number TEXT,
    national_id TEXT,
    
    -- Company info
    company_type TEXT,
    company_status TEXT,
    is_sme BOOLEAN,
    company_scale TEXT,
    
    -- Location
    headquarters_address TEXT,
    headquarters_city TEXT,
    headquarters_region TEXT,
    headquarters_postcode TEXT,
    headquarters_country TEXT DEFAULT 'NL',
    nuts_codes TEXT[],
    
    -- Contact - PRIMARY VALUE
    primary_email TEXT,
    secondary_email TEXT,
    primary_phone TEXT,
    secondary_phone TEXT,
    website_url TEXT,
    
    -- Classification
    cpv_codes_won TEXT[],
    cpv_primary TEXT,
    primary_sector TEXT,
    industry_tags TEXT[],
    
    -- Award statistics
    total_awards_won INTEGER DEFAULT 0,
    total_contract_value NUMERIC DEFAULT 0,
    average_contract_value NUMERIC,
    largest_contract_value NUMERIC,
    first_award_date DATE,
    last_award_date DATE,
    
    -- Buyer relationships
    frequent_buyers TEXT[],
    buyer_count INTEGER DEFAULT 0,
    
    -- Data quality
    email_verified BOOLEAN DEFAULT FALSE,
    email_is_dummy BOOLEAN DEFAULT FALSE,
    needs_enrichment BOOLEAN DEFAULT FALSE,
    enrichment_attempted_at TIMESTAMPTZ,
    enrichment_source TEXT,
    enrichment_success BOOLEAN,
    data_completeness_score NUMERIC,
    
    -- Source refs (all awards for this org)
    award_source_ids TEXT[],
    all_source_ids TEXT[],
    
    -- Timestamps
    first_seen_date TIMESTAMPTZ DEFAULT NOW(),
    last_activity_date TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Feed tracking
    fed_to_master BOOLEAN DEFAULT FALSE,
    fed_to_master_at TIMESTAMPTZ,
    master_org_id BIGINT,            -- ID in master_organizations_clean after feed
    
    CONSTRAINT tenderned_organizations_kvk_unique UNIQUE (kvk_number)
);

-- Indexes for tenderned_organizations
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_name ON public.tenderned_organizations(canonical_name);
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_name_lower ON public.tenderned_organizations(LOWER(TRIM(canonical_name)));
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_kvk ON public.tenderned_organizations(kvk_number) WHERE kvk_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_email ON public.tenderned_organizations(primary_email) WHERE primary_email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_country ON public.tenderned_organizations(headquarters_country);
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_cpv ON public.tenderned_organizations USING GIN(cpv_codes_won);
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_awards ON public.tenderned_organizations(total_awards_won DESC);
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_value ON public.tenderned_organizations(total_contract_value DESC);
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_not_fed ON public.tenderned_organizations(fed_to_master) WHERE fed_to_master = FALSE;
CREATE INDEX IF NOT EXISTS idx_tenderned_orgs_needs_enrichment ON public.tenderned_organizations(needs_enrichment) WHERE needs_enrichment = TRUE;


-- ============================================================================
-- 4. FEED FUNCTIONS - Move data from tenderned_* to master_* tables
-- ============================================================================

-- Feed tenderned_tenders → master_tenders
CREATE OR REPLACE FUNCTION feed_tenderned_tenders_to_master()
RETURNS INTEGER AS $$
DECLARE
    rows_fed INTEGER := 0;
BEGIN
    INSERT INTO public.master_tenders (
        source, source_id, internal_ref,
        title, title_translated, short_description, short_description_translated,
        full_description, language,
        buyer_name, buyer_country, buyer_city, buyer_region, buyer_postcode,
        buyer_address, buyer_organization_type, buyer_sector,
        tbc_code, tbc_category, cpv_codes, cpv_primary,
        naics_codes, unspsc_codes, nuts_codes,
        published_at, deadline, tender_period_start, tender_period_end,
        contract_start_date, contract_end_date,
        currency, value_min, value_max, estimated_value, value_text,
        contract_duration_months, contract_type, lot_structure, number_of_lots,
        framework_agreement, renewal_option, procurement_method,
        sme_suitable, reserved_for_sme, reserved_for_vcse,
        allows_consortia, allows_subcontracting,
        required_capabilities, required_certifications,
        contact_name, contact_email, contact_phone, contact_address, contact_organization,
        detail_url, external_portal_url,
        tender_status, notice_type,
        is_framework, is_dynamic_purchasing_system,
        source_metadata,
        inserted_at, updated_at
    )
    SELECT
        source, source_id, internal_ref,
        title, title_translated, short_description, short_description_translated,
        full_description, language,
        buyer_name, buyer_country, buyer_city, buyer_region, buyer_postcode,
        buyer_address, buyer_organization_type, buyer_sector,
        tbc_code, tbc_category, cpv_codes, cpv_primary,
        naics_codes, unspsc_codes, nuts_codes,
        published_at, deadline, tender_period_start, tender_period_end,
        contract_start_date, contract_end_date,
        currency, value_min, value_max, estimated_value, value_text,
        contract_duration_months, contract_type, lot_structure, number_of_lots,
        framework_agreement, renewal_option, procurement_method,
        sme_suitable, reserved_for_sme, reserved_for_vcse,
        allows_consortia, allows_subcontracting,
        required_capabilities, required_certifications,
        contact_name, contact_email, contact_phone, contact_address, contact_organization,
        detail_url, external_portal_url,
        tender_status, notice_type,
        is_framework, is_dynamic_purchasing_system,
        source_metadata,
        NOW(), NOW()
    FROM public.tenderned_tenders
    WHERE fed_to_master = FALSE
    ON CONFLICT (source, source_id) DO UPDATE SET
        title = EXCLUDED.title,
        short_description = EXCLUDED.short_description,
        full_description = EXCLUDED.full_description,
        deadline = EXCLUDED.deadline,
        estimated_value = EXCLUDED.estimated_value,
        tender_status = EXCLUDED.tender_status,
        source_metadata = EXCLUDED.source_metadata,
        updated_at = NOW();
    
    GET DIAGNOSTICS rows_fed = ROW_COUNT;
    
    -- Mark as fed
    UPDATE public.tenderned_tenders
    SET fed_to_master = TRUE, fed_to_master_at = NOW()
    WHERE fed_to_master = FALSE;
    
    RETURN rows_fed;
END;
$$ LANGUAGE plpgsql;


-- Feed tenderned_awards → master_awards
CREATE OR REPLACE FUNCTION feed_tenderned_awards_to_master()
RETURNS INTEGER AS $$
DECLARE
    rows_fed INTEGER := 0;
BEGIN
    INSERT INTO public.master_awards (
        source, source_id, internal_ref, tender_internal_ref,
        title, title_translated, short_description, short_description_translated,
        full_description, language,
        buyer_name, buyer_country, buyer_city, buyer_region,
        buyer_organization_type, buyer_sector, buyer_email, buyer_phone,
        supplier_name, supplier_name_normalized, supplier_country, supplier_city,
        supplier_id, supplier_size, supplier_is_consortium,
        consortium_members, lead_contractor,
        supplier_email, supplier_phone,
        award_date, award_value, currency, original_estimate, variance_percentage,
        award_status, number_of_bidders,
        tbc_code, tbc_category, cpv_codes, cpv_primary,
        naics_codes, unspsc_codes,
        contract_start_date, contract_end_date, contract_duration_months,
        contract_signed_date, procurement_method, sme_suitable,
        detail_url, award_notice_url,
        source_metadata,
        inserted_at, updated_at
    )
    SELECT
        source, source_id, internal_ref, tender_internal_ref,
        title, title_translated, short_description, short_description_translated,
        full_description, language,
        buyer_name, buyer_country, buyer_city, buyer_region,
        buyer_organization_type, buyer_sector, buyer_email, buyer_phone,
        supplier_name, supplier_name_normalized, supplier_country, supplier_city,
        supplier_id, supplier_size, supplier_is_consortium,
        consortium_members, lead_contractor,
        CASE WHEN email_is_dummy THEN NULL ELSE supplier_email END,
        CASE WHEN phone_is_dummy THEN NULL ELSE supplier_phone END,
        award_date, award_value, currency, original_estimate, variance_percentage,
        award_status, number_of_bidders,
        tbc_code, tbc_category, cpv_codes, cpv_primary,
        naics_codes, unspsc_codes,
        contract_start_date, contract_end_date, contract_duration_months,
        contract_signed_date, procurement_method, sme_suitable,
        detail_url, award_notice_url,
        source_metadata,
        NOW(), NOW()
    FROM public.tenderned_awards
    WHERE fed_to_master = FALSE
    ON CONFLICT (source, source_id) DO UPDATE SET
        supplier_name = EXCLUDED.supplier_name,
        supplier_email = COALESCE(EXCLUDED.supplier_email, master_awards.supplier_email),
        supplier_phone = COALESCE(EXCLUDED.supplier_phone, master_awards.supplier_phone),
        award_value = EXCLUDED.award_value,
        source_metadata = EXCLUDED.source_metadata,
        updated_at = NOW();
    
    GET DIAGNOSTICS rows_fed = ROW_COUNT;
    
    -- Mark as fed
    UPDATE public.tenderned_awards
    SET fed_to_master = TRUE, fed_to_master_at = NOW()
    WHERE fed_to_master = FALSE;
    
    RETURN rows_fed;
END;
$$ LANGUAGE plpgsql;


-- Feed tenderned_organizations → master_organizations_clean
CREATE OR REPLACE FUNCTION feed_tenderned_organizations_to_master()
RETURNS INTEGER AS $$
DECLARE
    rows_fed INTEGER := 0;
    org_record RECORD;
    master_id BIGINT;
BEGIN
    FOR org_record IN 
        SELECT * FROM public.tenderned_organizations
        WHERE fed_to_master = FALSE
    LOOP
        INSERT INTO public.master_organizations_clean (
            org_ref,
            canonical_name,
            canonical_name_normalized,
            name_variations,
            legal_name,
            companies_house_number,
            vat_number,
            national_id,
            company_type,
            company_status,
            is_sme,
            company_scale,
            headquarters_address,
            headquarters_city,
            headquarters_region,
            headquarters_postcode,
            headquarters_country,
            nuts_codes,
            primary_email,
            secondary_email,
            primary_phone,
            secondary_phone,
            website_url,
            cpv_codes_won,
            cpv_primary,
            primary_sector,
            industry_tags,
            total_awards_won,
            total_contract_value,
            average_contract_value,
            largest_contract_value,
            first_award_date,
            last_award_date,
            frequent_buyers,
            buyer_count,
            needs_enrichment,
            enrichment_attempted_at,
            enrichment_source,
            enrichment_success,
            data_completeness_score,
            all_source_ids,
            primary_source,
            first_seen_date,
            last_activity_date,
            inserted_at,
            updated_at
        )
        VALUES (
            'tenderned_' || org_record.kvk_number,
            org_record.canonical_name,
            LOWER(TRIM(org_record.canonical_name)),
            org_record.name_variations,
            org_record.legal_name,
            org_record.kvk_number,  -- Dutch KVK goes to companies_house_number
            org_record.btw_number,
            org_record.kvk_number,
            org_record.company_type,
            org_record.company_status,
            org_record.is_sme,
            org_record.company_scale,
            org_record.headquarters_address,
            org_record.headquarters_city,
            org_record.headquarters_region,
            org_record.headquarters_postcode,
            org_record.headquarters_country,
            org_record.nuts_codes,
            org_record.primary_email,
            org_record.secondary_email,
            org_record.primary_phone,
            org_record.secondary_phone,
            org_record.website_url,
            org_record.cpv_codes_won,
            org_record.cpv_primary,
            org_record.primary_sector,
            org_record.industry_tags,
            org_record.total_awards_won,
            org_record.total_contract_value,
            org_record.average_contract_value,
            org_record.largest_contract_value,
            org_record.first_award_date,
            org_record.last_award_date,
            org_record.frequent_buyers,
            org_record.buyer_count,
            org_record.needs_enrichment,
            org_record.enrichment_attempted_at,
            org_record.enrichment_source,
            org_record.enrichment_success,
            org_record.data_completeness_score,
            org_record.all_source_ids,
            'tenderned',
            org_record.first_seen_date,
            org_record.last_activity_date,
            NOW(),
            NOW()
        )
        ON CONFLICT (canonical_name_normalized) 
        WHERE canonical_name_normalized IS NOT NULL
        DO UPDATE SET
            primary_email = COALESCE(EXCLUDED.primary_email, master_organizations_clean.primary_email),
            primary_phone = COALESCE(EXCLUDED.primary_phone, master_organizations_clean.primary_phone),
            total_awards_won = master_organizations_clean.total_awards_won + EXCLUDED.total_awards_won,
            total_contract_value = master_organizations_clean.total_contract_value + EXCLUDED.total_contract_value,
            last_award_date = GREATEST(master_organizations_clean.last_award_date, EXCLUDED.last_award_date),
            last_activity_date = GREATEST(master_organizations_clean.last_activity_date, EXCLUDED.last_activity_date),
            updated_at = NOW()
        RETURNING id INTO master_id;
        
        -- Update tenderned_organizations with master ID
        UPDATE public.tenderned_organizations
        SET 
            fed_to_master = TRUE,
            fed_to_master_at = NOW(),
            master_org_id = master_id
        WHERE id = org_record.id;
        
        rows_fed := rows_fed + 1;
    END LOOP;
    
    RETURN rows_fed;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- 5. CONVENIENCE FUNCTION - Feed all TenderNed data to masters
-- ============================================================================
CREATE OR REPLACE FUNCTION feed_all_tenderned_to_masters()
RETURNS TABLE(tenders_fed INTEGER, awards_fed INTEGER, orgs_fed INTEGER) AS $$
DECLARE
    t_fed INTEGER;
    a_fed INTEGER;
    o_fed INTEGER;
BEGIN
    t_fed := feed_tenderned_tenders_to_master();
    a_fed := feed_tenderned_awards_to_master();
    o_fed := feed_tenderned_organizations_to_master();
    
    RETURN QUERY SELECT t_fed, a_fed, o_fed;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- 6. HELPER FUNCTION - Extract organizations from awards
-- ============================================================================
CREATE OR REPLACE FUNCTION extract_tenderned_organizations()
RETURNS INTEGER AS $$
DECLARE
    rows_extracted INTEGER := 0;
BEGIN
    -- Insert/update organizations from awards
    INSERT INTO public.tenderned_organizations (
        source,
        first_award_source_id,
        canonical_name,
        kvk_number,
        btw_number,
        is_sme,
        headquarters_city,
        headquarters_country,
        primary_email,
        primary_phone,
        cpv_codes_won,
        cpv_primary,
        total_awards_won,
        total_contract_value,
        average_contract_value,
        largest_contract_value,
        first_award_date,
        last_award_date,
        award_source_ids,
        email_is_dummy,
        needs_enrichment,
        first_seen_date,
        last_activity_date
    )
    SELECT
        'tenderned',
        MIN(source_id),
        supplier_name,
        kvk_number,
        MAX(btw_number),
        BOOL_OR(supplier_size = 'SME'),
        MAX(supplier_city),
        'NL',
        MAX(CASE WHEN NOT email_is_dummy THEN supplier_email END),
        MAX(CASE WHEN NOT phone_is_dummy THEN supplier_phone END),
        ARRAY_AGG(DISTINCT unnest_cpv) FILTER (WHERE unnest_cpv IS NOT NULL),
        MAX(cpv_primary),
        COUNT(*),
        SUM(COALESCE(award_value, 0)),
        AVG(award_value),
        MAX(award_value),
        MIN(award_date::DATE),
        MAX(award_date::DATE),
        ARRAY_AGG(source_id),
        BOOL_AND(email_is_dummy),
        BOOL_AND(email_is_dummy) AND MAX(kvk_number) IS NOT NULL,
        MIN(inserted_at),
        MAX(award_date)
    FROM public.tenderned_awards
    LEFT JOIN LATERAL UNNEST(cpv_codes) AS unnest_cpv ON TRUE
    WHERE supplier_name IS NOT NULL
      AND fed_to_organizations = FALSE
    GROUP BY supplier_name, kvk_number
    ON CONFLICT (kvk_number) 
    WHERE kvk_number IS NOT NULL
    DO UPDATE SET
        primary_email = COALESCE(EXCLUDED.primary_email, tenderned_organizations.primary_email),
        primary_phone = COALESCE(EXCLUDED.primary_phone, tenderned_organizations.primary_phone),
        total_awards_won = tenderned_organizations.total_awards_won + EXCLUDED.total_awards_won,
        total_contract_value = tenderned_organizations.total_contract_value + EXCLUDED.total_contract_value,
        largest_contract_value = GREATEST(tenderned_organizations.largest_contract_value, EXCLUDED.largest_contract_value),
        last_award_date = GREATEST(tenderned_organizations.last_award_date, EXCLUDED.last_award_date),
        last_activity_date = GREATEST(tenderned_organizations.last_activity_date, EXCLUDED.last_activity_date),
        award_source_ids = tenderned_organizations.award_source_ids || EXCLUDED.award_source_ids,
        updated_at = NOW();
    
    GET DIAGNOSTICS rows_extracted = ROW_COUNT;
    
    -- Mark awards as processed
    UPDATE public.tenderned_awards
    SET fed_to_organizations = TRUE, fed_to_organizations_at = NOW()
    WHERE supplier_name IS NOT NULL AND fed_to_organizations = FALSE;
    
    RETURN rows_extracted;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- USAGE:
-- 
-- After running scraper:
--   SELECT extract_tenderned_organizations();  -- Extract orgs from awards
--   SELECT * FROM feed_all_tenderned_to_masters();  -- Feed to all master tables
--
-- Or individually:
--   SELECT feed_tenderned_tenders_to_master();
--   SELECT feed_tenderned_awards_to_master();
--   SELECT feed_tenderned_organizations_to_master();
-- ============================================================================
