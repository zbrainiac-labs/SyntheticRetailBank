#!/usr/bin/env python3
import re
import os
import glob

STRUCTURE_DIR = "/Users/mdaeppen/workspace/AAA_synthetic_bank/structure"
OUTPUT_DIR = "/Users/mdaeppen/workspace/AAA_synthetic_bank/sources/definitions"
POST_DEPLOY_FILE = "/Users/mdaeppen/workspace/AAA_synthetic_bank/post_deploy.sql"
DB = "AAA_DEV_SYNTHETIC_BANK"

SCHEMA_MAP = {
    "CRM_RAW_001": f"{DB}.CRM_RAW_001",
    "CRM_AGG_001": f"{DB}.CRM_AGG_001",
    "EQT_RAW_001": f"{DB}.EQT_RAW_001",
    "EQT_AGG_001": f"{DB}.EQT_AGG_001",
    "FII_RAW_001": f"{DB}.FII_RAW_001",
    "FII_AGG_001": f"{DB}.FII_AGG_001",
    "CMD_RAW_001": f"{DB}.CMD_RAW_001",
    "CMD_AGG_001": f"{DB}.CMD_AGG_001",
    "PAY_RAW_001": f"{DB}.PAY_RAW_001",
    "PAY_AGG_001": f"{DB}.PAY_AGG_001",
    "REF_RAW_001": f"{DB}.REF_RAW_001",
    "REF_AGG_001": f"{DB}.REF_AGG_001",
    "REP_RAW_001": f"{DB}.REP_RAW_001",
    "REP_AGG_001": f"{DB}.REP_AGG_001",
    "LOA_RAW_001": f"{DB}.LOA_RAW_001",
    "LOA_AGG_001": f"{DB}.LOA_AGG_001",
    "PUBLIC": f"{DB}.PUBLIC",
}

FILE_MAPPING = {
    "010_CRMI_customer_master.sql": "010_CRMI_ingestion.sql",
    "011_ACCI_accounts.sql": "011_ACCI_ingestion.sql",
    "015_EMPI_employees.sql": "015_EMPI_ingestion.sql",
    "020_REFI_fx_rates.sql": "020_REFI_ingestion.sql",
    "030_PAYI_transactions.sql": "030_PAYI_ingestion.sql",
    "035_ICGI_swift_messages.sql": "035_ICGI_ingestion.sql",
    "040_EQTI_equity_trades.sql": "040_EQTI_ingestion.sql",
    "050_FIII_fixed_income.sql": "050_FIII_ingestion.sql",
    "055_CMDI_commodities.sql": "055_CMDI_ingestion.sql",
    "060_LIQI_LiquidityCoverageRatio.sql": "060_LIQI_ingestion.sql",
    "065_LOAI_loans_documents.sql": "065_LOAI_ingestion.sql",
    "302_CRMA_sanctions_screening.sql": "302_CRMA_analytics.sql",
    "311_ACCA_accounts_agg.sql": "311_ACCA_analytics.sql",
    "312_CRMA_LIFECYCLE.sql": "312_CRMA_lifecycle.sql",
    "320_REFA_fx_analytics.sql": "320_REFA_analytics.sql",
    "330_PAYA_anomaly_detection.sql": "330_PAYA_analytics.sql",
    "335_ICGA_swift_lifecycle.sql": "335_ICGA_analytics.sql",
    "340_EQTA_equity_analytics.sql": "340_EQTA_analytics.sql",
    "350_FIIA_fixed_income_analytics.sql": "350_FIIA_analytics.sql",
    "355_CMDA_commodity_analytics.sql": "355_CMDA_analytics.sql",
    "360_LIQA_CalculateHQLAandNetCashOutflows.sql": "360_LIQA_analytics.sql",
    "361_LIQA_BusinessReporting_FINMA_LCR.sql": "361_LIQA_reporting.sql",
    "410_CRMA_customer_360.sql": "410_CRMA_customer360.sql",
    "415_EMPA_employee_analytics.sql": "415_EMPA_analytics.sql",
    "465_LOAA_loans_applications.sql": "465_LOAA_analytics.sql",
    "500_REPP_core_reporting.sql": "500_REPP_reporting.sql",
    "510_REPP_equity_reporting.sql": "510_REPP_equity.sql",
    "520_REPP_CREDIT_RISK.sql": "520_REPP_credit_risk.sql",
    "525_REPP_frtb_market_risk.sql": "525_REPP_frtb.sql",
    "540_REPP_bcbs239_compliance.sql": "540_REPP_bcbs239.sql",
    "565_LOAR_loans_portfolio_reporting.sql": "565_LOAR_reporting.sql",
    "600_REPP_portfolio_performance.sql": "600_REPP_portfolio.sql",
}

UNSUPPORTED_FILES = [
    "000_database_setup.sql",
    "001_get_listings.sql",
    "002_SANC_sanction_data.sql",
    "710_CRMA_SV_CUSTOMER_360.sql",
    "715_EMPA_SV_EMPLOYEE_ADVISOR.sql",
    "720_PAYA_SV_COMPLIANCE_MONITORING.sql",
    "730_REPA_SV_WEALTH_MANAGEMENT.sql",
    "740_REPA_SV_RISK_REPORTING.sql",
    "750_LCRS_SV_LCR_SEMANTIC_MODELS.sql",
    "765_LOAS_SV_LOAN_PORTFOLIO_SEMANTIC_MODELS.sql",
    "810_CRM_INTELLIGENCE_AGENT.sql",
    "811_COMPLIANCE_MONITORING_AGENT.sql",
    "820_RISK_REGULATORY_AGENT.sql",
    "830_WEALTH_ADVISOR_AGENT.sql",
    "850_LIQUIDITY_RISK_AGENT.sql",
    "865_LOAN_PORTFOLIO_AGENT.sql",
]


def get_current_schema(content):
    m = re.search(r'USE\s+SCHEMA\s+(\w+)\s*;', content, re.IGNORECASE)
    return m.group(1) if m else None


def remove_comments(text):
    lines = text.split('\n')
    result = []
    in_block_comment = False
    in_dollar_quote = False
    for line in lines:
        if in_dollar_quote:
            result.append(line)
            if '$$' in line:
                in_dollar_quote = False
            continue
        if in_block_comment:
            if '*/' in line:
                in_block_comment = False
                rest = line[line.index('*/') + 2:]
                if rest.strip():
                    result.append(rest)
            continue
        if '$$' in line:
            result.append(line)
            occurrences = line.count('$$')
            if occurrences == 1:
                in_dollar_quote = True
            continue
        stripped = line.strip()
        if stripped.startswith('--'):
            continue
        if ' --' in line:
            comment_pos = line.index(' --')
            in_string = False
            quote_char = None
            for i, c in enumerate(line[:comment_pos]):
                if c in ("'", '"') and not in_string:
                    in_string = True
                    quote_char = c
                elif c == quote_char and in_string:
                    in_string = False
            if not in_string:
                line = line[:comment_pos]
        if '/*' in line:
            slash_pos = line.index('/*')
            in_str = False
            qc = None
            for i, c in enumerate(line[:slash_pos]):
                if c in ("'", '"') and not in_str:
                    in_str = True
                    qc = c
                elif c == qc and in_str:
                    in_str = False
            if not in_str:
                before = line[:slash_pos]
                if '*/' not in line[slash_pos:]:
                    in_block_comment = True
                    if before.strip():
                        result.append(before)
                    continue
                else:
                    end_pos = line.index('*/', slash_pos) + 2
                    line = before + line[end_pos:]
        result.append(line)
    return '\n'.join(result)


def remove_blank_lines(text):
    lines = text.split('\n')
    result = []
    prev_blank = False
    for line in lines:
        if line.strip() == '':
            if not prev_blank:
                result.append('')
            prev_blank = True
        else:
            prev_blank = False
            result.append(line)
    while result and result[0].strip() == '':
        result.pop(0)
    while result and result[-1].strip() == '':
        result.pop()
    return '\n'.join(result)


def extract_objects(content, schema):
    fqn_prefix = SCHEMA_MAP.get(schema, f"{DB}.{schema}") + "."
    
    dcm_objects = []
    post_deploy_objects = []
    
    content_clean = remove_comments(content)
    
    content_no_use = re.sub(r'USE\s+DATABASE\s+\w+\s*;', '', content_clean, flags=re.IGNORECASE)
    content_no_use = re.sub(r'USE\s+SCHEMA\s+\w+\s*;', '', content_no_use, flags=re.IGNORECASE)
    
    stmts = split_statements(content_no_use)
    
    for stmt in stmts:
        stripped = stmt.strip()
        if not stripped:
            continue
            
        upper = stripped.upper()
        
        if upper.startswith('ALTER TASK') and 'RESUME' in upper:
            post_deploy_objects.append(("task_resume", stmt))
            continue
            
        if upper.startswith('ALTER TABLE') and 'SET TAG' in upper:
            post_deploy_objects.append(("tags", stmt))
            continue
            
        if upper.startswith('INSERT INTO'):
            post_deploy_objects.append(("inserts", stmt))
            continue

        if upper.startswith('SELECT'):
            continue
            
        if upper.startswith('CREATE OR REPLACE STREAM') or upper.startswith('CREATE STREAM'):
            post_deploy_objects.append(("streams", stmt))
            continue
            
        if upper.startswith('CREATE OR REPLACE FILE FORMAT') or upper.startswith('CREATE FILE FORMAT'):
            post_deploy_objects.append(("file_formats", stmt))
            continue
            
        if upper.startswith('CREATE OR REPLACE PROCEDURE') or upper.startswith('CREATE PROCEDURE'):
            post_deploy_objects.append(("procedures", stmt))
            continue
            
        if re.match(r'CREATE\s+(OR\s+REPLACE\s+)?SNOWFLAKE\s*\.\s*ML', upper):
            post_deploy_objects.append(("dynamic", stmt))
            continue

        if 'EXECUTE IMMEDIATE' in upper and upper.startswith('SET') or upper.startswith('EXECUTE'):
            post_deploy_objects.append(("dynamic", stmt))
            continue
            
        if upper.startswith('GRANT '):
            post_deploy_objects.append(("grants", stmt))
            continue
            
        if upper.startswith('CREATE OR REPLACE TABLE') or upper.startswith('CREATE TABLE'):
            name = extract_object_name(stripped, r'CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?')
            if name:
                new_stmt = re.sub(
                    r'CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?' + re.escape(name),
                    f'DEFINE TABLE {fqn_prefix}{name}',
                    stripped, count=1, flags=re.IGNORECASE
                )
                dcm_objects.append(("TABLE", name, new_stmt))
            continue
            
        if upper.startswith('CREATE OR REPLACE DYNAMIC TABLE') or upper.startswith('CREATE DYNAMIC TABLE'):
            name = extract_dt_name(stripped)
            if name:
                new_stmt = re.sub(
                    r'CREATE\s+(OR\s+REPLACE\s+)?DYNAMIC\s+TABLE\s+' + re.escape(name),
                    f'DEFINE DYNAMIC TABLE {fqn_prefix}{name}',
                    stripped, count=1, flags=re.IGNORECASE
                )
                dcm_objects.append(("DYNAMIC TABLE", name, new_stmt))
            continue
            
        if upper.startswith('CREATE OR REPLACE VIEW') or upper.startswith('CREATE VIEW'):
            name = extract_object_name(stripped, r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+')
            if name:
                new_stmt = re.sub(
                    r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+' + re.escape(name),
                    f'DEFINE VIEW {fqn_prefix}{name}',
                    stripped, count=1, flags=re.IGNORECASE
                )
                dcm_objects.append(("VIEW", name, new_stmt))
            continue
            
        if upper.startswith('CREATE STAGE IF NOT EXISTS') or upper.startswith('CREATE OR REPLACE STAGE') or upper.startswith('CREATE STAGE'):
            name = extract_object_name(stripped, r'CREATE\s+(OR\s+REPLACE\s+)?STAGE\s+(IF\s+NOT\s+EXISTS\s+)?')
            if name:
                new_stmt = re.sub(
                    r'CREATE\s+(OR\s+REPLACE\s+)?STAGE\s+(IF\s+NOT\s+EXISTS\s+)?' + re.escape(name),
                    f'DEFINE STAGE {fqn_prefix}{name}',
                    stripped, count=1, flags=re.IGNORECASE
                )
                dcm_objects.append(("STAGE", name, new_stmt))
            continue
            
        if upper.startswith('CREATE OR REPLACE TASK') or upper.startswith('CREATE TASK'):
            name = extract_object_name(stripped, r'CREATE\s+(OR\s+REPLACE\s+)?TASK\s+')
            if name:
                new_stmt = convert_task(stripped, name, fqn_prefix, schema)
                dcm_objects.append(("TASK", name, new_stmt))
            continue

    return dcm_objects, post_deploy_objects


def extract_object_name(stmt, pattern):
    m = re.match(pattern + r'(\S+)', stmt, re.IGNORECASE)
    if m:
        name = m.group(m.lastindex)
        name = name.strip('(').strip()
        return name
    return None


def extract_dt_name(stmt):
    m = re.match(r'CREATE\s+(OR\s+REPLACE\s+)?DYNAMIC\s+TABLE\s+(\S+)', stmt, re.IGNORECASE)
    if m:
        name = m.group(2).rstrip('(').strip()
        return name
    return None


def convert_task(stmt, name, fqn_prefix, schema):
    new_stmt = re.sub(
        r'CREATE\s+(OR\s+REPLACE\s+)?TASK\s+' + re.escape(name),
        f'DEFINE TASK {fqn_prefix}{name}',
        stmt, count=1, flags=re.IGNORECASE
    )
    
    stream_pattern = r"SYSTEM\$STREAM_HAS_DATA\('(\w+)'\)"
    def replace_stream_ref(m):
        stream_name = m.group(1)
        return f"SYSTEM$STREAM_HAS_DATA('{fqn_prefix}{stream_name}')"
    new_stmt = re.sub(stream_pattern, replace_stream_ref, new_stmt)
    
    after_pattern = r'AFTER\s+(\w+)'
    def replace_after_ref(m):
        task_ref = m.group(1)
        return f'AFTER {fqn_prefix}{task_ref}'
    new_stmt = re.sub(after_pattern, replace_after_ref, new_stmt, flags=re.IGNORECASE)
    
    return new_stmt


def split_statements(content):
    statements = []
    current = []
    in_dollar = False
    in_string = False
    lines = content.split('\n')
    
    i = 0
    full_text = content
    result = []
    pos = 0
    in_dollar_quote = False
    
    chars = list(full_text)
    stmt_start = 0
    i = 0
    while i < len(chars):
        if not in_dollar_quote and i + 1 < len(chars) and chars[i] == '$' and chars[i+1] == '$':
            in_dollar_quote = True
            i += 2
            continue
        elif in_dollar_quote and i + 1 < len(chars) and chars[i] == '$' and chars[i+1] == '$':
            in_dollar_quote = False
            i += 2
            continue
        
        if not in_dollar_quote and chars[i] == ';':
            stmt = full_text[stmt_start:i+1].strip()
            if stmt and stmt != ';':
                result.append(stmt)
            stmt_start = i + 1
        i += 1
    
    remaining = full_text[stmt_start:].strip()
    if remaining and remaining != ';':
        result.append(remaining)
    
    return result


def process_file(src_filename, schema=None):
    filepath = os.path.join(STRUCTURE_DIR, src_filename)
    if not os.path.exists(filepath):
        print(f"  SKIP: {src_filename} not found")
        return [], [], None
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    if schema is None:
        schema = get_current_schema(content)
    
    if not schema:
        print(f"  WARN: No schema found in {src_filename}")
        return [], [], None
    
    dcm_objects, post_deploy_objects = extract_objects(content, schema)
    return dcm_objects, post_deploy_objects, schema


def write_dcm_file(output_filename, dcm_objects):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, output_filename)
    
    lines = []
    for obj_type, obj_name, stmt in dcm_objects:
        cleaned = remove_blank_lines(stmt.rstrip().rstrip(';'))
        lines.append(cleaned + ';')
        lines.append('')
    
    content = '\n'.join(lines).strip() + '\n'
    
    with open(filepath, 'w') as f:
        f.write(content)
    
    return filepath


def write_post_deploy(all_post_deploy, schema_context_map):
    lines = []
    
    categories = {}
    for src_file, schema, objects in all_post_deploy:
        for cat, stmt in objects:
            if cat not in categories:
                categories[cat] = []
            categories[cat].append((src_file, schema, stmt))
    
    order = ["file_formats", "streams", "procedures", "inserts", "tags", "task_resume", "grants", "dynamic"]
    labels = {
        "file_formats": "FILE FORMATS",
        "streams": "STREAMS",
        "procedures": "STORED PROCEDURES",
        "inserts": "INSERT STATEMENTS (Reference Data)",
        "tags": "ALTER TABLE SET TAG",
        "task_resume": "ALTER TASK RESUME",
        "grants": "GRANTS",
        "dynamic": "DYNAMIC SQL / EXECUTE IMMEDIATE",
    }
    
    for cat in order:
        if cat not in categories:
            continue
        items = categories[cat]
        lines.append(f"-- ============================================================")
        lines.append(f"-- {labels.get(cat, cat.upper())}")
        lines.append(f"-- ============================================================")
        lines.append("")
        
        current_schema = None
        for src_file, schema, stmt in items:
            if schema != current_schema:
                lines.append(f"USE DATABASE {DB};")
                lines.append(f"USE SCHEMA {schema};")
                lines.append("")
                current_schema = schema
            lines.append(stmt.rstrip().rstrip(';') + ';')
            lines.append("")
    
    content = '\n'.join(lines).strip() + '\n'
    with open(POST_DEPLOY_FILE, 'w') as f:
        f.write(content)
    return POST_DEPLOY_FILE


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_post_deploy = []
    summary = []
    
    for src_file, out_file in sorted(FILE_MAPPING.items()):
        print(f"Processing: {src_file} -> {out_file}")
        dcm_objects, post_objects, schema = process_file(src_file)
        
        if dcm_objects:
            path = write_dcm_file(out_file, dcm_objects)
            obj_counts = {}
            for obj_type, obj_name, _ in dcm_objects:
                obj_counts[obj_type] = obj_counts.get(obj_type, 0) + 1
            count_str = ", ".join(f"{v} {k}{'s' if v > 1 else ''}" for k, v in sorted(obj_counts.items()))
            summary.append((out_file, len(dcm_objects), count_str))
            print(f"  -> {out_file}: {len(dcm_objects)} objects ({count_str})")
        else:
            summary.append((out_file, 0, "no DCM objects"))
            print(f"  -> {out_file}: no DCM objects found")
        
        if post_objects:
            all_post_deploy.append((src_file, schema, post_objects))
            post_counts = {}
            for cat, _ in post_objects:
                post_counts[cat] = post_counts.get(cat, 0) + 1
            post_str = ", ".join(f"{v} {k}" for k, v in sorted(post_counts.items()))
            print(f"  -> post_deploy: {len(post_objects)} items ({post_str})")
    
    for unsup_file in UNSUPPORTED_FILES:
        filepath = os.path.join(STRUCTURE_DIR, unsup_file)
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                content = f.read()
            schema = get_current_schema(content) or "PUBLIC"
            cleaned = remove_comments(content)
            cleaned = re.sub(r'USE\s+DATABASE\s+\w+\s*;', '', cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r'USE\s+SCHEMA\s+\w+\s*;', '', cleaned, flags=re.IGNORECASE)
            stmts = split_statements(cleaned)
            post_items = []
            for stmt in stmts:
                stripped = stmt.strip()
                if not stripped or stripped.upper().startswith('SELECT'):
                    continue
                upper = stripped.upper()
                if 'SEMANTIC' in upper and 'VIEW' in upper:
                    post_items.append(("semantic_views", stmt))
                elif 'AGENT' in upper or 'CORTEX' in upper:
                    post_items.append(("agents", stmt))
                elif 'LISTING' in upper or 'SHARE' in upper:
                    post_items.append(("listings", stmt))
                elif upper.startswith('GRANT'):
                    post_items.append(("grants", stmt))
                elif upper.startswith('CREATE') or upper.startswith('ALTER') or upper.startswith('SET') or upper.startswith('EXECUTE'):
                    post_items.append(("dynamic", stmt))
                else:
                    post_items.append(("dynamic", stmt))
            if post_items:
                all_post_deploy.append((unsup_file, schema, post_items))
                print(f"Unsupported: {unsup_file} -> post_deploy.sql ({len(post_items)} items)")
    
    if all_post_deploy:
        pd_path = write_post_deploy(all_post_deploy, {})
        total_post = sum(len(objs) for _, _, objs in all_post_deploy)
        print(f"\npost_deploy.sql: {total_post} total items")
    
    print("\n" + "=" * 70)
    print("SUMMARY OF DEFINITION FILES")
    print("=" * 70)
    total_objects = 0
    for out_file, count, detail in summary:
        total_objects += count
        print(f"  {out_file:45s} {count:3d} objects  ({detail})")
    print(f"\n  {'TOTAL':45s} {total_objects:3d} objects")
    print(f"\n  post_deploy.sql written to: {POST_DEPLOY_FILE}")
    print(f"  Definition files written to: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
