DEFINE STAGE {{ db }}.{{ pay_raw }}.ICGI_RAW_ST_SWIFT_INBOUND
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for SWIFT ISO20022 XML message files. Expected pattern: *.xml with PACS.008 and PACS.002 message types for interbank clearing operations';

DEFINE TABLE {{ db }}.{{ pay_raw }}.ICGI_RAW_TB_SWIFT_MESSAGES (
    FILE_NAME   STRING COMMENT 'Original source file name for audit trail, correlation with external systems, and operational troubleshooting. Enables traceability back to source systems and message routing verification.',
    LOAD_TS     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP COMMENT 'System ingestion timestamp for data lineage tracking, SLA monitoring, and processing performance analysis. Critical for operational dashboards and regulatory reporting timelines.',
    RAW_XML     VARIANT COMMENT 'Complete SWIFT ISO20022 XML message content preserved as VARIANT for flexible schema evolution, compliance archival, and comprehensive downstream parsing. Supports all current and future ISO20022 message types with full fidelity preservation.'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Master repository for raw SWIFT ISO20022 XML messages supporting interbank clearing and settlement operations. Stores PACS.008 customer credit transfer instructions, PACS.002 payment status reports, and future message types in native XML format. Provides foundation for downstream business logic processing, regulatory compliance analysis, operational monitoring, and audit trail maintenance. Optimized for high-volume message ingestion with comprehensive metadata capture.';

DEFINE TASK {{ db }}.{{ pay_raw }}.ICGI_RAW_TK_LOAD_SWIFT_MESSAGES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ pay_raw }}.ICGI_RAW_SM_SWIFT_FILES')
AS
    COPY INTO {{ pay_raw }}.ICGI_RAW_TB_SWIFT_MESSAGES (FILE_NAME, RAW_XML)
    FROM (
        SELECT 
            METADATA$FILENAME AS FILE_NAME,         
            PARSE_XML($1) AS RAW_XML                
        FROM @{{ pay_raw }}.ICGI_RAW_ST_SWIFT_INBOUND
    )
    PATTERN = '.*\.xml'                             
    FILE_FORMAT = ICGI_RAW_FF_XML              
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ pay_raw }}.ICGI_RAW_TK_CLEANUP_AFTER_LOAD_SWIFT_MESSAGES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER SWIFT message data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ pay_raw }}.ICGI_RAW_TK_LOAD_SWIFT_MESSAGES
AS
    CALL PAYI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ pay_raw }}.ICGI_RAW_ST_SWIFT_INBOUND', 5);
