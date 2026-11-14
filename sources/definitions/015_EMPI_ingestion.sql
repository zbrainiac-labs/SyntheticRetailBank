/*
 * 015_EMPI_ingestion.sql
 * Employee raw ingestion: employees, client assignments, stages, tasks
 */
DEFINE STAGE {{ db }}.{{ crm_raw }}.EMPI_RAW_ST_EMPLOYEES
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for employee master data CSV files. Expected pattern: *employees*.csv';

DEFINE STAGE {{ db }}.{{ crm_raw }}.EMPI_RAW_ST_CLIENT_ASSIGNMENTS
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for client-advisor assignment CSV files. Expected pattern: *client_assignments*.csv';

DEFINE TABLE {{ db }}.{{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE (
    EMPLOYEE_ID VARCHAR(20) NOT NULL,                   
    FIRST_NAME VARCHAR(100),                            
    FAMILY_NAME VARCHAR(100),                           
    EMAIL VARCHAR(200),                                 
    PHONE VARCHAR(50),                                  

    DATE_OF_BIRTH DATE,                                 
    HIRE_DATE DATE,                                     
    EMPLOYMENT_STATUS VARCHAR(20),                      

    COUNTRY VARCHAR(50),                                
    OFFICE_LOCATION VARCHAR(200),                       
    REGION VARCHAR(50),                                 

    POSITION_LEVEL VARCHAR(30),                         
    MANAGER_EMPLOYEE_ID VARCHAR(20),                    

    PERFORMANCE_RATING DECIMAL(3,2),                    
    LANGUAGES_SPOKEN VARCHAR(200),                      
    CERTIFICATIONS VARCHAR(500),                        

    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ,                 

    PRIMARY KEY (EMPLOYEE_ID),
    CONSTRAINT FK_EMPI_RAW_TB_EMPLOYEE__EMPI_RAW_TB_EMPLOYEE FOREIGN KEY (MANAGER_EMPLOYEE_ID) REFERENCES {{ db }}.{{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE(EMPLOYEE_ID)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Employee master data with 3-tier hierarchy (advisors, team leaders, super team leaders). Scales dynamically based on customer distribution.';

DEFINE TABLE {{ db }}.{{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT (
    ASSIGNMENT_ID VARCHAR(30) NOT NULL,                 
    CUSTOMER_ID VARCHAR(30) NOT NULL,                   
    ADVISOR_EMPLOYEE_ID VARCHAR(20) NOT NULL,           

    ASSIGNMENT_START_DATE DATE,                         
    ASSIGNMENT_END_DATE DATE,                           
    IS_CURRENT BOOLEAN DEFAULT TRUE,                    

    ASSIGNMENT_REASON VARCHAR(50),                      

    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ,                 

    PRIMARY KEY (ASSIGNMENT_ID),

    FOREIGN KEY (ADVISOR_EMPLOYEE_ID) REFERENCES {{ db }}.{{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE(EMPLOYEE_ID)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Client-advisor assignment tracking with history support (SCD Type 2 ready). Tracks customer-advisor relationships over time. CUSTOMER_ID references {{ db }}.{{ crm_raw }}.CRMI_RAW_TB_CUSTOMER but FK not enforced due to SCD Type 2 composite key.';

DEFINE TASK {{ db }}.{{ crm_raw }}.EMPI_RAW_TK_LOAD_EMPLOYEES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ crm_raw }}.EMPI_RAW_SM_EMPLOYEE_FILES')
    STARTED
AS
    EXECUTE IMMEDIATE $$
    BEGIN
        COPY INTO {{ db }}.{{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE
        FROM @{{ db }}.{{ crm_raw }}.EMPI_RAW_ST_EMPLOYEES
        FILE_FORMAT = (FORMAT_NAME = 'EMPI_RAW_FF_EMPLOYEE_CSV')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE';
        -- consume the file stream (offset only): it is empty again and does not go stale
        CREATE OR REPLACE TEMPORARY TABLE STREAM_ADVANCE AS SELECT RELATIVE_PATH FROM {{ db }}.{{ crm_raw }}.EMPI_RAW_SM_EMPLOYEE_FILES WHERE 0 = 1;
    END;
    $$;

DEFINE TASK {{ db }}.{{ crm_raw }}.EMPI_RAW_TK_LOAD_ASSIGNMENTS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ crm_raw }}.EMPI_RAW_SM_ASSIGNMENT_FILES')
    STARTED
AS
    EXECUTE IMMEDIATE $$
    BEGIN
        COPY INTO {{ db }}.{{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT
        FROM @{{ db }}.{{ crm_raw }}.EMPI_RAW_ST_CLIENT_ASSIGNMENTS
        FILE_FORMAT = (FORMAT_NAME = 'EMPI_RAW_FF_ASSIGNMENT_CSV')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE';
        -- consume the file stream (offset only): it is empty again and does not go stale
        CREATE OR REPLACE TEMPORARY TABLE STREAM_ADVANCE AS SELECT RELATIVE_PATH FROM {{ db }}.{{ crm_raw }}.EMPI_RAW_SM_ASSIGNMENT_FILES WHERE 0 = 1;
    END;
    $$;

DEFINE TASK {{ db }}.{{ crm_raw }}.EMPI_RAW_TK_CLEANUP_AFTER_LOAD_EMPLOYEES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER employee data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ crm_raw }}.EMPI_RAW_TK_LOAD_EMPLOYEES
    STARTED
AS
    CALL CRMI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ crm_raw }}.EMPI_RAW_ST_EMPLOYEES', 5);

DEFINE TASK {{ db }}.{{ crm_raw }}.EMPI_RAW_TK_CLEANUP_AFTER_LOAD_ASSIGNMENTS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER client assignment data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ crm_raw }}.EMPI_RAW_TK_LOAD_ASSIGNMENTS
    STARTED
AS
    CALL CRMI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ crm_raw }}.EMPI_RAW_ST_CLIENT_ASSIGNMENTS', 5);

-- ============================================================
-- FILE FORMATS (moved from post_deploy.sql)
-- ============================================================

DEFINE FILE FORMAT {{ db }}.{{ crm_raw }}.EMPI_RAW_FF_EMPLOYEE_CSV
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"'
    COMMENT = 'CSV format for employee master data with EMEA localization - PARSE_HEADER handles header row';

DEFINE FILE FORMAT {{ db }}.{{ crm_raw }}.EMPI_RAW_FF_ASSIGNMENT_CSV
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"'
    COMMENT = 'CSV format for client-advisor assignments with SCD Type 2 support - PARSE_HEADER handles header row';

-- ============================================================
-- STREAMS (moved from post_deploy.sql)
-- ============================================================

DEFINE STREAM {{ db }}.{{ crm_raw }}.EMPI_RAW_SM_EMPLOYEE_FILES
    ON STAGE {{ db }}.{{ crm_raw }}.EMPI_RAW_ST_EMPLOYEES
    COMMENT = 'Monitors EMPI_RAW_ST_EMPLOYEES stage for new employee data files';

DEFINE STREAM {{ db }}.{{ crm_raw }}.EMPI_RAW_SM_ASSIGNMENT_FILES
    ON STAGE {{ db }}.{{ crm_raw }}.EMPI_RAW_ST_CLIENT_ASSIGNMENTS
    COMMENT = 'Monitors EMPI_RAW_ST_CLIENT_ASSIGNMENTS stage for new assignment files';
