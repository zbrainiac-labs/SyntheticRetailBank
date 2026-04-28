DEFINE DYNAMIC TABLE {{ db }}.{{ pay_agg }}.ICGA_AGG_DT_SWIFT_PACS008(
 SOURCE_FILENAME VARCHAR(200),
 SOURCE_LOAD_TIMESTAMP TIMESTAMP_NTZ,
 MESSAGE_ID VARCHAR(50),
 CREATION_DATETIME TIMESTAMP_NTZ,
 NUMBER_OF_TRANSACTIONS NUMBER(10,0),
 GROUP_SETTLEMENT_CURRENCY VARCHAR(3),
 GROUP_SETTLEMENT_AMOUNT DECIMAL(28,2),
 SETTLEMENT_METHOD VARCHAR(20),
 CLEARING_SYSTEM_CODE VARCHAR(20),
 INSTRUCTION_ID VARCHAR(50),
 END_TO_END_ID VARCHAR(50),
 TRANSACTION_ID VARCHAR(50),
 INSTRUCTION_PRIORITY VARCHAR(20),
 SERVICE_LEVEL_CODE VARCHAR(20),
 LOCAL_INSTRUMENT_CODE VARCHAR(20),
 TRANSACTION_CURRENCY VARCHAR(3),
 TRANSACTION_AMOUNT DECIMAL(28,2),
 INTERBANK_SETTLEMENT_DATE DATE,
 CHARGES_BEARER VARCHAR(10),
 INSTRUCTING_AGENT_BIC VARCHAR(20),
 INSTRUCTED_AGENT_BIC VARCHAR(20),
 DEBTOR_AGENT_BIC VARCHAR(20),
 CREDITOR_AGENT_BIC VARCHAR(20),
 DEBTOR_NAME VARCHAR(200),
 DEBTOR_STREET VARCHAR(200),
 DEBTOR_POSTAL_CODE VARCHAR(20),
 DEBTOR_CITY VARCHAR(100),
 DEBTOR_COUNTRY VARCHAR(50),
 DEBTOR_IBAN VARCHAR(50),
 CREDITOR_NAME VARCHAR(200),
 CREDITOR_STREET VARCHAR(200),
 CREDITOR_POSTAL_CODE VARCHAR(20),
 CREDITOR_CITY VARCHAR(100),
 CREDITOR_COUNTRY VARCHAR(50),
 CREDITOR_IBAN VARCHAR(50),
 REMITTANCE_INFORMATION VARCHAR(500),
 IS_HIGH_VALUE_PAYMENT BOOLEAN,
 IS_TARGET2_PAYMENT BOOLEAN,
 PAYMENT_CORRIDOR VARCHAR(50),
 PAYMENT_TYPE_CLASSIFICATION VARCHAR(15),
 PARSED_AT TIMESTAMP_NTZ,
 XML_SIZE_BYTES NUMBER(10,0)
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

FROM {{ pay_raw }}.ICGI_RAW_TB_SWIFT_MESSAGES
WHERE RAW_XML IS NOT NULL
 AND (FILE_NAME ILIKE '%pacs008%' OR RAW_XML::STRING ILIKE '%FIToFICstmrCdtTrf%');

DEFINE DYNAMIC TABLE {{ db }}.{{ pay_agg }}.ICGA_AGG_DT_SWIFT_PACS002(
 SOURCE_FILENAME VARCHAR(200),
 SOURCE_LOAD_TIMESTAMP TIMESTAMP_NTZ,
 MESSAGE_ID VARCHAR(50),
 CREATION_DATETIME TIMESTAMP_NTZ,
 INSTRUCTING_AGENT_BIC VARCHAR(20),
 INSTRUCTED_AGENT_BIC VARCHAR(20),
 ORIGINAL_MESSAGE_ID VARCHAR(50),
 ORIGINAL_MESSAGE_NAME_ID VARCHAR(50),
 ORIGINAL_CREATION_DATETIME TIMESTAMP_NTZ,
 GROUP_STATUS VARCHAR(10),
 ORIGINAL_END_TO_END_ID VARCHAR(50),
 TRANSACTION_STATUS VARCHAR(10),
 STATUS_REASON VARCHAR(200),
 ORIGINAL_INSTRUCTION_ID VARCHAR(50),
 ORIGINAL_TRANSACTION_ID VARCHAR(50),
 ACCEPTANCE_DATETIME TIMESTAMP_NTZ,
 TRANSACTION_STATUS_DESCRIPTION VARCHAR(50),
 GROUP_STATUS_DESCRIPTION VARCHAR(50),
 IS_POSITIVE_RESPONSE BOOLEAN,
 IS_REJECTION BOOLEAN,
 IS_PACS008_RESPONSE BOOLEAN,
 ORIGINAL_MESSAGE_DATE VARCHAR(10),
 PARSED_AT TIMESTAMP_NTZ,
 XML_SIZE_BYTES NUMBER(10,0)
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

FROM {{ pay_raw }}.ICGI_RAW_TB_SWIFT_MESSAGES
WHERE RAW_XML IS NOT NULL
 AND (FILE_NAME ILIKE '%pacs002%' OR RAW_XML::STRING ILIKE '%FIToFIPmtStsRpt%');

DEFINE DYNAMIC TABLE {{ db }}.{{ pay_agg }}.ICGA_AGG_DT_SWIFT_PAYMENT_LIFECYCLE(
 PACS008_MESSAGE_ID VARCHAR(50),
 PACS002_ORIGINAL_MESSAGE_ID VARCHAR(50),
 PACS008_END_TO_END_ID VARCHAR(50),
 PACS002_ORIGINAL_END_TO_END_ID VARCHAR(50),
 TRANSACTION_STATUS VARCHAR(10),
 TRANSACTION_STATUS_DESCRIPTION VARCHAR(50),
 GROUP_STATUS VARCHAR(10),
 GROUP_STATUS_DESCRIPTION VARCHAR(50),
 STATUS_REASON VARCHAR(200),
 IS_REJECTION BOOLEAN,
 IS_POSITIVE_RESPONSE BOOLEAN,
 TRANSACTION_CURRENCY VARCHAR(3),
 TRANSACTION_AMOUNT DECIMAL(28,2),
 DEBTOR_NAME VARCHAR(200),
 CREDITOR_NAME VARCHAR(200),
 PAYMENT_CORRIDOR VARCHAR(50),
 PAYMENT_TYPE_CLASSIFICATION VARCHAR(15),
 IS_HIGH_VALUE_PAYMENT BOOLEAN,
 IS_TARGET2_PAYMENT BOOLEAN,
 PACS008_FILE VARCHAR(200),
 PACS002_FILE VARCHAR(200),
 PACS008_LOAD_TIMESTAMP TIMESTAMP_NTZ,
 PACS002_LOAD_TIMESTAMP TIMESTAMP_NTZ,
 ACK_TIME NUMBER(10,0),
 JOINED_AT TIMESTAMP_NTZ
) COMMENT = 'Complete SWIFT payment lifecycle view joining PACS.008 instructions with PACS.002 status reports. Provides end-to-end payment tracking, SLA monitoring, settlement analysis, and comprehensive business intelligence for treasury management, compliance reporting, and operational excellence.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
 p008.message_id AS pacs008_message_id,
 p002.original_message_id AS pacs002_original_message_id,

 p008.end_to_end_id AS pacs008_end_to_end_id,
 p002.original_end_to_end_id AS pacs002_original_end_to_end_id,

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

 p008.source_filename AS pacs008_file,
 p002.source_filename AS pacs002_file,
 p008.source_load_timestamp AS pacs008_load_timestamp,
 p002.source_load_timestamp AS pacs002_load_timestamp,
 DATEDIFF('minutes', p002.ORIGINAL_CREATION_DATETIME, p002.CREATION_DATETIME) AS ack_time,
 CURRENT_TIMESTAMP() AS joined_at

FROM {{ pay_agg }}.ICGA_AGG_DT_SWIFT_PACS008 p008
LEFT JOIN {{ pay_agg }}.ICGA_AGG_DT_SWIFT_PACS002 p002
 ON p002.original_message_id = p008.message_id
 AND (
 p002.original_end_to_end_id = p008.end_to_end_id
 OR p002.original_transaction_id = p008.transaction_id
 );
