/*
 * 335_ICGA_analytics.sql
 * SWIFT/ICG analytics: message parsing and enrichment
 */
DEFINE DYNAMIC TABLE {{ db }}.{{ pay_agg }}.ICGA_AGG_DT_SWIFT_PACS008(
    SOURCE_FILENAME VARCHAR(200) COMMENT 'Original XML file name for audit trail and message correlation',
    SOURCE_LOAD_TIMESTAMP TIMESTAMP_NTZ COMMENT 'System ingestion timestamp for data lineage and processing tracking',
    MESSAGE_ID VARCHAR(50) COMMENT 'Unique SWIFT message identifier for deduplication and correlation',
    CREATION_DATETIME TIMESTAMP_NTZ COMMENT 'Message creation timestamp for SLA monitoring and processing analysis',
    NUMBER_OF_TRANSACTIONS NUMBER(10,0) COMMENT 'Number of transactions in message batch for volume analysis',
    GROUP_SETTLEMENT_CURRENCY VARCHAR(3) COMMENT 'Settlement currency for the entire message group',
    GROUP_SETTLEMENT_AMOUNT DECIMAL(28,2) COMMENT 'Total settlement amount for liquidity management',
    SETTLEMENT_METHOD VARCHAR(20) COMMENT 'Settlement method code for routing and clearing decisions',
    CLEARING_SYSTEM_CODE VARCHAR(20) COMMENT 'Clearing system identifier (TARGET2/SEPA/etc.) for operational routing',
    INSTRUCTION_ID VARCHAR(50) COMMENT 'Bank internal instruction identifier for operational tracking',
    END_TO_END_ID VARCHAR(50) COMMENT 'Customer end-to-end reference for reconciliation and customer service',
    TRANSACTION_ID VARCHAR(50) COMMENT 'SWIFT transaction identifier for inquiry and investigation',
    INSTRUCTION_PRIORITY VARCHAR(20) COMMENT 'Payment priority level for processing sequence and SLA',
    SERVICE_LEVEL_CODE VARCHAR(20) COMMENT 'Service level agreement code for processing rules',
    LOCAL_INSTRUMENT_CODE VARCHAR(20) COMMENT 'Local payment instrument code for domestic routing',
    TRANSACTION_CURRENCY VARCHAR(3) COMMENT 'Payment currency for FX and treasury management',
    TRANSACTION_AMOUNT DECIMAL(28,2) COMMENT 'Payment amount for limit monitoring and settlement',
    INTERBANK_SETTLEMENT_DATE DATE COMMENT 'Requested settlement date for liquidity planning',
    CHARGES_BEARER VARCHAR(10) COMMENT 'Charges allocation (OUR/BEN/SHA) for fee management',
    INSTRUCTING_AGENT_BIC VARCHAR(20) COMMENT 'BIC of instructing bank for routing and correspondence',
    INSTRUCTED_AGENT_BIC VARCHAR(20) COMMENT 'BIC of instructed bank for processing and settlement',
    DEBTOR_AGENT_BIC VARCHAR(20) COMMENT 'BIC of debtor bank for correspondent banking',
    CREDITOR_AGENT_BIC VARCHAR(20) COMMENT 'BIC of creditor bank for beneficiary settlement',
    DEBTOR_NAME VARCHAR(200) COMMENT 'Payer name for compliance screening and customer identification',
    DEBTOR_STREET VARCHAR(200) COMMENT 'Payer street address for compliance and verification',
    DEBTOR_POSTAL_CODE VARCHAR(20) COMMENT 'Payer postal code for geographic analysis',
    DEBTOR_CITY VARCHAR(100) COMMENT 'Payer city for compliance and risk assessment',
    DEBTOR_COUNTRY VARCHAR(50) COMMENT 'Payer country for sanctions screening and regulatory compliance',
    DEBTOR_IBAN VARCHAR(50) COMMENT 'Payer IBAN for account identification and validation',
    CREDITOR_NAME VARCHAR(200) COMMENT 'Beneficiary name for compliance screening and delivery confirmation',
    CREDITOR_STREET VARCHAR(200) COMMENT 'Beneficiary street address for compliance verification',
    CREDITOR_POSTAL_CODE VARCHAR(20) COMMENT 'Beneficiary postal code for geographic analysis',
    CREDITOR_CITY VARCHAR(100) COMMENT 'Beneficiary city for compliance and risk assessment',
    CREDITOR_COUNTRY VARCHAR(50) COMMENT 'Beneficiary country for sanctions screening and regulatory compliance',
    CREDITOR_IBAN VARCHAR(50) COMMENT 'Beneficiary IBAN for account identification and settlement',
    REMITTANCE_INFORMATION VARCHAR(500) COMMENT 'Payment purpose and reference information for compliance',
    IS_HIGH_VALUE_PAYMENT BOOLEAN COMMENT 'Boolean flag for payments >= 100k requiring enhanced monitoring',
    IS_TARGET2_PAYMENT BOOLEAN COMMENT 'Boolean flag for TARGET2 RTGS payments requiring special handling',
    PAYMENT_CORRIDOR VARCHAR(50) COMMENT 'Geographic payment flow (Country -> Country) for correspondent analysis',
    PAYMENT_TYPE_CLASSIFICATION VARCHAR(15) COMMENT 'Payment classification (DOMESTIC/CROSS_BORDER) for regulatory reporting',
    PARSED_AT TIMESTAMP_NTZ COMMENT 'Timestamp when XML parsing was completed for processing tracking',
    XML_SIZE_BYTES NUMBER(10,0) COMMENT 'Size of original XML message for performance analysis'
) COMMENT = 'SWIFT PACS.008 Customer Credit Transfer messages parsed and structured for business analysis. Includes payment instructions, routing information, compliance data, and derived analytics for operational monitoring, risk management, and regulatory reporting.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT 
    FILE_NAME as source_filename,
    LOAD_TS as source_load_timestamp,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[0]."$"')::STRING AS message_id,
    TRY_CAST(GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[1]."$"')::STRING AS TIMESTAMP_NTZ) AS creation_datetime,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[2]."$"')::INTEGER AS number_of_transactions,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[3]."@Ccy"')::STRING AS group_settlement_currency,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[3]."$"')::DECIMAL(28,2) AS group_settlement_amount,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[4]."$"[0]."$"')::STRING AS settlement_method,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[4]."$"[1]."$"."$"')::STRING AS clearing_system_code,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[0]."$"[0]."$"')::STRING AS instruction_id,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[0]."$"[1]."$"')::STRING AS end_to_end_id,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[0]."$"[2]."$"')::STRING AS transaction_id,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[1]."$"[0]."$"')::STRING AS instruction_priority,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[1]."$"[1]."$"."$"')::STRING AS service_level_code,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[1]."$"[2]."$"."$"')::STRING AS local_instrument_code,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[2]."@Ccy"')::STRING AS transaction_currency,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[2]."$"')::DECIMAL(28,2) AS transaction_amount,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[3]."$"')::DATE AS interbank_settlement_date,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[4]."$"')::STRING AS charges_bearer,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[5]."$"."$"."$"')::STRING AS instructing_agent_bic,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[6]."$"."$"."$"')::STRING AS instructed_agent_bic,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[7]."$"."$"."$"')::STRING AS debtor_agent_bic,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[8]."$"."$"."$"')::STRING AS creditor_agent_bic,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[9]."$"[0]."$"')::STRING AS debtor_name,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[9]."$"[1]."$"[0]."$"')::STRING AS debtor_street,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[9]."$"[1]."$"[1]."$"')::STRING AS debtor_postal_code,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[9]."$"[1]."$"[2]."$"')::STRING AS debtor_city,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[9]."$"[1]."$"[3]."$"')::STRING AS debtor_country,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[10]."$"."$"."$"')::STRING AS debtor_iban,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[11]."$"[0]."$"')::STRING AS creditor_name,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[11]."$"[1]."$"[0]."$"')::STRING AS creditor_street,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[11]."$"[1]."$"[1]."$"')::STRING AS creditor_postal_code,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[11]."$"[1]."$"[2]."$"')::STRING AS creditor_city,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[11]."$"[1]."$"[3]."$"')::STRING AS creditor_country,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[12]."$"."$"."$"')::STRING AS creditor_iban,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[13]."$"."$"')::STRING AS remittance_information,

    CASE 
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[2]."$"')::DECIMAL(28,2) >= 100000 THEN TRUE
        ELSE FALSE
    END AS is_high_value_payment,

    CASE 
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[4]."$"[1]."$"."$"')::STRING = 'TARGET2' THEN TRUE
        ELSE FALSE
    END AS is_target2_payment,

    CONCAT(
        COALESCE(GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[9]."$"[1]."$"[3]."$"')::STRING, 'UNKNOWN'),
        ' -> ',
        COALESCE(GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[11]."$"[1]."$"[3]."$"')::STRING, 'UNKNOWN')
    ) AS payment_corridor,

    CASE 
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[9]."$"[1]."$"[3]."$"')::STRING = 
             GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[11]."$"[1]."$"[3]."$"')::STRING THEN 'DOMESTIC'
        ELSE 'CROSS_BORDER'
    END AS payment_type_classification,

    CURRENT_TIMESTAMP() AS parsed_at,
    LENGTH(RAW_XML::STRING) AS xml_size_bytes

FROM {{ db }}.{{ pay_raw }}.ICGI_RAW_TB_SWIFT_MESSAGES
WHERE RAW_XML IS NOT NULL
  AND (FILE_NAME ILIKE '%pacs008%' OR RAW_XML::STRING ILIKE '%FIToFICstmrCdtTrf%');

DEFINE DYNAMIC TABLE {{ db }}.{{ pay_agg }}.ICGA_AGG_DT_SWIFT_PACS002(
    SOURCE_FILENAME VARCHAR(200) COMMENT 'Original XML file name for audit trail and message correlation',
    SOURCE_LOAD_TIMESTAMP TIMESTAMP_NTZ COMMENT 'System ingestion timestamp for data lineage and processing tracking',
    MESSAGE_ID VARCHAR(50) COMMENT 'Unique status report message identifier for deduplication',
    CREATION_DATETIME TIMESTAMP_NTZ COMMENT 'Status report creation timestamp for SLA measurement and response time analysis',
    INSTRUCTING_AGENT_BIC VARCHAR(20) COMMENT 'BIC of bank sending status report for operational contact and routing',
    INSTRUCTED_AGENT_BIC VARCHAR(20) COMMENT 'BIC of bank receiving status report for message routing',
    ORIGINAL_MESSAGE_ID VARCHAR(50) COMMENT 'Reference to original PACS.008 message for correlation and reconciliation',
    ORIGINAL_MESSAGE_NAME_ID VARCHAR(50) COMMENT 'Confirmation of original message type being acknowledged',
    ORIGINAL_CREATION_DATETIME TIMESTAMP_NTZ COMMENT 'Original instruction timestamp for SLA tracking and processing time calculation',
    GROUP_STATUS VARCHAR(10) COMMENT 'Overall status of payment batch (ACCP/RJCT/PDNG) for bulk processing analysis',
    ORIGINAL_END_TO_END_ID VARCHAR(50) COMMENT 'Customer reference from original instruction for notification and reconciliation',
    TRANSACTION_STATUS VARCHAR(10) COMMENT 'Individual payment status code (ACCP/RJCT/PDNG/ACSC/ACSP) for operational decisions',
    STATUS_REASON VARCHAR(200) COMMENT 'Detailed reason code for rejection or delay for customer service and investigation',
    ORIGINAL_INSTRUCTION_ID VARCHAR(50) COMMENT 'Bank internal reference from original instruction for operational tracking',
    ORIGINAL_TRANSACTION_ID VARCHAR(50) COMMENT 'SWIFT tracking reference from original instruction for inquiry handling',
    ACCEPTANCE_DATETIME TIMESTAMP_NTZ COMMENT 'Timestamp when payment was actually processed for settlement timing analysis',
    TRANSACTION_STATUS_DESCRIPTION VARCHAR(50) COMMENT 'Human-readable transaction status for dashboards and customer notifications',
    GROUP_STATUS_DESCRIPTION VARCHAR(50) COMMENT 'Human-readable batch status for operational monitoring and reporting',
    IS_POSITIVE_RESPONSE BOOLEAN COMMENT 'Boolean flag for successful payment processing (SLA reporting and customer communication)',
    IS_REJECTION BOOLEAN COMMENT 'Boolean flag for failed payments requiring exception handling and customer service escalation',
    IS_PACS008_RESPONSE BOOLEAN COMMENT 'Boolean flag confirming this status relates to payment instruction (not other message types)',
    ORIGINAL_MESSAGE_DATE VARCHAR(10) COMMENT 'Business date extracted from original message for time-based analytics and archiving',
    PARSED_AT TIMESTAMP_NTZ COMMENT 'Timestamp when XML parsing was completed for processing tracking',
    XML_SIZE_BYTES NUMBER(10,0) COMMENT 'Size of original XML message for performance analysis'
) COMMENT = 'SWIFT PACS.002 Payment Status Reports parsed and structured for operational monitoring. Includes status confirmations, rejection reasons, processing timestamps, and derived analytics for SLA tracking, exception handling, and customer communication workflows.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT 
    FILE_NAME as source_filename,                                         
    LOAD_TS as source_load_timestamp,                                    

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[0]."$"')::STRING AS message_id,                  
    TRY_CAST(GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[1]."$"')::STRING AS TIMESTAMP_NTZ) AS creation_datetime, 

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[2]."$"."$"."$"')::STRING AS instructing_agent_bic,-- Bank sending status report (operational contact)
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[0]."$"[3]."$"."$"."$"')::STRING AS instructed_agent_bic,

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[0]."$"')::STRING AS original_message_id,         
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[1]."$"')::STRING AS original_message_name_id,    
    TRY_CAST(GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[2]."$"')::STRING AS TIMESTAMP_NTZ) AS original_creation_datetime,
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[3]."$"')::STRING AS group_status,               

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[0]."$"')::STRING AS original_end_to_end_id,      
    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[1]."$"')::STRING AS transaction_status,          

    GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[2]."$"."$"')::STRING AS status_reason,           

    TRY_CAST(GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[3]."$"')::STRING AS STRING) AS original_instruction_id, 
    TRY_CAST(GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[4]."$"')::STRING AS STRING) AS original_transaction_id, 
    TRY_CAST(GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[5]."$"')::STRING AS TIMESTAMP_NTZ) AS acceptance_datetime,-- When payment was actually processed (settlement timing)

    CASE 
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[1]."$"')::STRING = 'ACCP' THEN 'ACCEPTED'
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[1]."$"')::STRING = 'RJCT' THEN 'REJECTED'
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[1]."$"')::STRING = 'PDNG' THEN 'PENDING'
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[1]."$"')::STRING = 'ACSC' THEN 'ACCEPTED_SETTLEMENT_COMPLETED'
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[1]."$"')::STRING = 'ACSP' THEN 'ACCEPTED_SETTLEMENT_IN_PROCESS'
        ELSE GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[1]."$"')::STRING
    END AS transaction_status_description,                                 

    CASE 
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[3]."$"')::STRING = 'ACCP' THEN 'ACCEPTED'
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[3]."$"')::STRING = 'RJCT' THEN 'REJECTED'
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[3]."$"')::STRING = 'PDNG' THEN 'PENDING'
        ELSE GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[3]."$"')::STRING
    END AS group_status_description,                                       

    CASE 
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[1]."$"')::STRING IN ('ACCP', 'ACSC', 'ACSP') THEN TRUE
        ELSE FALSE
    END AS is_positive_response,                                           

    CASE 
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[2]."$"[1]."$"')::STRING = 'RJCT' THEN TRUE
        ELSE FALSE
    END AS is_rejection,                                                   

    CASE 
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[1]."$"')::STRING = 'pacs.008.001.08' THEN TRUE
        ELSE FALSE
    END AS is_pacs008_response,                                            

    CASE 
        WHEN GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[0]."$"')::STRING LIKE '20%-%-%' THEN
            SUBSTR(GET_PATH(PARSE_XML(RAW_XML::STRING), '$[1]."$"[0]."$"')::STRING, 1, 8)
        ELSE NULL
    END AS original_message_date,                                          

    CURRENT_TIMESTAMP() AS parsed_at,                                      
    LENGTH(RAW_XML::STRING) AS xml_size_bytes                             

FROM {{ db }}.{{ pay_raw }}.ICGI_RAW_TB_SWIFT_MESSAGES
WHERE RAW_XML IS NOT NULL
  AND (FILE_NAME ILIKE '%pacs002%' OR RAW_XML::STRING ILIKE '%FIToFIPmtStsRpt%');

DEFINE DYNAMIC TABLE {{ db }}.{{ pay_agg }}.ICGA_AGG_DT_SWIFT_PAYMENT_LIFECYCLE(
    PACS008_MESSAGE_ID VARCHAR(50) COMMENT 'Original payment instruction message ID for tracking and correlation',
    PACS002_ORIGINAL_MESSAGE_ID VARCHAR(50) COMMENT 'Status report reference back to original instruction for reconciliation',
    PACS008_END_TO_END_ID VARCHAR(50) COMMENT 'Customer payment reference from original instruction for reconciliation',
    PACS002_ORIGINAL_END_TO_END_ID VARCHAR(50) COMMENT 'Customer reference confirmed in status report for validation',
    TRANSACTION_STATUS VARCHAR(10) COMMENT 'Final payment status code (ACCP/RJCT/PDNG/ACSC/ACSP) for operational decisions',
    TRANSACTION_STATUS_DESCRIPTION VARCHAR(50) COMMENT 'Human-readable payment status for customer communication and dashboards',
    GROUP_STATUS VARCHAR(10) COMMENT 'Batch-level payment outcome for bulk processing analysis',
    GROUP_STATUS_DESCRIPTION VARCHAR(50) COMMENT 'Human-readable batch status for operational dashboards and monitoring',
    STATUS_REASON VARCHAR(200) COMMENT 'Detailed reason code for investigation, customer service, and process improvement',
    IS_REJECTION BOOLEAN COMMENT 'Boolean flag for failed payments requiring exception handling workflows',
    IS_POSITIVE_RESPONSE BOOLEAN COMMENT 'Boolean flag for successful payments (SLA and performance reporting)',
    TRANSACTION_CURRENCY VARCHAR(3) COMMENT 'Payment currency for FX exposure analysis and treasury management',
    TRANSACTION_AMOUNT DECIMAL(28,2) COMMENT 'Payment value for limit monitoring, settlement planning, and risk assessment',
    DEBTOR_NAME VARCHAR(200) COMMENT 'Payer identification for compliance screening and customer service',
    CREDITOR_NAME VARCHAR(200) COMMENT 'Beneficiary identification for delivery confirmation and compliance',
    PAYMENT_CORRIDOR VARCHAR(50) COMMENT 'Geographic payment flow (Country -> Country) for correspondent banking analysis',
    PAYMENT_TYPE_CLASSIFICATION VARCHAR(15) COMMENT 'Payment classification (DOMESTIC/CROSS_BORDER) for regulatory reporting',
    IS_HIGH_VALUE_PAYMENT BOOLEAN COMMENT 'Boolean flag for large payments requiring enhanced monitoring and approval processes',
    IS_TARGET2_PAYMENT BOOLEAN COMMENT 'Boolean flag for RTGS payments requiring special liquidity and settlement planning',
    PACS008_FILE VARCHAR(200) COMMENT 'Original instruction file name for audit trail and data lineage',
    PACS002_FILE VARCHAR(200) COMMENT 'Status report file name for correlation verification and audit trail',
    PACS008_LOAD_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Instruction ingestion timestamp for timing analysis and data quality',
    PACS002_LOAD_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Status report ingestion timestamp for response time measurement',
    ACK_TIME NUMBER(10,0) COMMENT 'Processing time in minutes from instruction to status for SLA monitoring and performance optimization',
    JOINED_AT TIMESTAMP_NTZ COMMENT 'Join processing timestamp for data quality tracking and refresh monitoring'
) COMMENT = 'Complete SWIFT payment lifecycle view joining PACS.008 instructions with PACS.002 status reports. Provides end-to-end payment tracking, SLA monitoring, settlement analysis, and comprehensive business intelligence for treasury management, compliance reporting, and operational excellence.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    p008.message_id                AS pacs008_message_id,              
    p002.original_message_id       AS pacs002_original_message_id,    

    p008.end_to_end_id             AS pacs008_end_to_end_id,          
    p002.original_end_to_end_id    AS pacs002_original_end_to_end_id, 

    p002.transaction_status,                                          
    p002.transaction_status_description,                              
    p002.group_status,                                                
    p002.group_status_description,                                    
    p002.status_reason,                                               
    p002.is_rejection,                                                
    p002.is_positive_response,                                        

    p008.transaction_currency,                                        
    p008.transaction_amount,                                          
    p008.debtor_name,                                                 
    p008.creditor_name,                                               
    p008.payment_corridor,                                            
    p008.payment_type_classification,                                 
    p008.is_high_value_payment,                                       
    p008.is_target2_payment,                                          

    p008.source_filename   AS pacs008_file,                          
    p002.source_filename   AS pacs002_file,                          
    p008.source_load_timestamp AS pacs008_load_timestamp,            
    p002.source_load_timestamp AS pacs002_load_timestamp,            
    DATEDIFF('minutes', p002.ORIGINAL_CREATION_DATETIME, p002.CREATION_DATETIME) AS ack_time,
    CURRENT_TIMESTAMP() AS joined_at                                 

FROM {{ db }}.{{ pay_agg }}.ICGA_AGG_DT_SWIFT_PACS008 p008
LEFT JOIN {{ db }}.{{ pay_agg }}.ICGA_AGG_DT_SWIFT_PACS002 p002
    ON p002.original_message_id = p008.message_id
   AND (
        p002.original_end_to_end_id = p008.end_to_end_id
        OR p002.original_transaction_id = p008.transaction_id
   );
