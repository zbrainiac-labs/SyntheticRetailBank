DEFINE VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_EMPLOYEE_HIERARCHY
COMMENT = 'Recursive employee hierarchy showing full organizational structure with paths and levels'
AS
WITH RECURSIVE hierarchy AS (
    SELECT 
        EMPLOYEE_ID,                                                   
        FIRST_NAME,                                                    
        FAMILY_NAME,                                                   
        FIRST_NAME || ' ' || FAMILY_NAME as FULL_NAME,               
        POSITION_LEVEL,                                                
        MANAGER_EMPLOYEE_ID,                                           
        COUNTRY,                                                       
        REGION,                                                        
        EMPLOYMENT_STATUS,                                             
        HIRE_DATE,                                                     
        PERFORMANCE_RATING,                                            
        1 as HIERARCHY_LEVEL,                                         
        EMPLOYEE_ID as ROOT_SUPER_LEADER_ID,                          
        NULL as ROOT_SUPER_LEADER_NAME,                               
        NULL as TEAM_LEADER_ID,                                       
        NULL as TEAM_LEADER_NAME,                                     
        CAST(EMPLOYEE_ID AS VARCHAR(1000)) as HIERARCHY_PATH,        
        CAST(FIRST_NAME || ' ' || FAMILY_NAME AS VARCHAR(1000)) as HIERARCHY_PATH_NAMES 
    FROM {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE
    WHERE POSITION_LEVEL = 'SUPER_TEAM_LEADER'

    UNION ALL

    SELECT 
        e.EMPLOYEE_ID,                                                
        e.FIRST_NAME,                                                 
        e.FAMILY_NAME,                                                
        e.FIRST_NAME || ' ' || e.FAMILY_NAME,                        
        e.POSITION_LEVEL,                                             
        e.MANAGER_EMPLOYEE_ID,                                        
        e.COUNTRY,                                                    
        e.REGION,                                                     
        e.EMPLOYMENT_STATUS,                                          
        e.HIRE_DATE,                                                  
        e.PERFORMANCE_RATING,                                         
        h.HIERARCHY_LEVEL + 1,                                       
        h.ROOT_SUPER_LEADER_ID,                                      
        h.ROOT_SUPER_LEADER_NAME,                                    
        CASE 
            WHEN e.POSITION_LEVEL = 'CLIENT_ADVISOR' THEN e.MANAGER_EMPLOYEE_ID
            ELSE h.TEAM_LEADER_ID
        END,                                                         
        CASE 
            WHEN e.POSITION_LEVEL = 'CLIENT_ADVISOR' THEN 
                (SELECT FIRST_NAME || ' ' || FAMILY_NAME 
                 FROM {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE 
                 WHERE EMPLOYEE_ID = e.MANAGER_EMPLOYEE_ID)
            ELSE h.TEAM_LEADER_NAME
        END,                                                         
        h.HIERARCHY_PATH || ' > ' || e.EMPLOYEE_ID,                 
        h.HIERARCHY_PATH_NAMES || ' > ' || e.FIRST_NAME || ' ' || e.FAMILY_NAME 
    FROM {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE e
    INNER JOIN hierarchy h ON e.MANAGER_EMPLOYEE_ID = h.EMPLOYEE_ID
)
SELECT * FROM hierarchy;

DEFINE VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_ORGANIZATIONAL_CHART
COMMENT = 'Flat organizational chart with manager relationships and direct report counts'
AS
SELECT 
    e.EMPLOYEE_ID,                                                    
    e.FIRST_NAME || ' ' || e.FAMILY_NAME as EMPLOYEE_NAME,           
    e.POSITION_LEVEL,                                                 
    e.COUNTRY,                                                        
    e.REGION,                                                         
    e.EMPLOYMENT_STATUS,                                              
    e.HIRE_DATE,                                                      
    DATEDIFF(day, e.HIRE_DATE, CURRENT_DATE()) as TENURE_DAYS,      
    ROUND(DATEDIFF(day, e.HIRE_DATE, CURRENT_DATE()) / 365.25, 1) as TENURE_YEARS, 
    e.PERFORMANCE_RATING,                                             
    e.LANGUAGES_SPOKEN,                                               
    e.MANAGER_EMPLOYEE_ID,                                            
    m.FIRST_NAME || ' ' || m.FAMILY_NAME as MANAGER_NAME,            
    m.POSITION_LEVEL as MANAGER_POSITION,                            
    (SELECT COUNT(*) 
     FROM {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE 
     WHERE MANAGER_EMPLOYEE_ID = e.EMPLOYEE_ID) as DIRECT_REPORTS,  
    (SELECT COUNT(DISTINCT CUSTOMER_ID) 
     FROM {{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT 
     WHERE ADVISOR_EMPLOYEE_ID = e.EMPLOYEE_ID 
     AND IS_CURRENT = TRUE) as CLIENT_COUNT                          
FROM {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE e
LEFT JOIN {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE m ON e.MANAGER_EMPLOYEE_ID = m.EMPLOYEE_ID;

DEFINE DYNAMIC TABLE {{ db }}.{{ crm_agg }}.EMPA_AGG_DT_ADVISOR_PERFORMANCE(
    EMPLOYEE_ID VARCHAR(20) COMMENT 'Advisor identifier for performance tracking and compensation',
    ADVISOR_NAME VARCHAR(201) COMMENT 'Full name for leaderboards and recognition programs',
    COUNTRY VARCHAR(50) COMMENT 'Location for regional performance benchmarking',
    REGION VARCHAR(50) COMMENT 'Broader region for cross-market comparison',
    HIRE_DATE DATE COMMENT 'Start date for experience-weighted metrics',
    TENURE_DAYS NUMBER(10,0) COMMENT 'Days of service for ramp-up performance expectations',
    EMPLOYMENT_STATUS VARCHAR(20) COMMENT 'Status for active advisor filtering',
    PERFORMANCE_RATING DECIMAL(3,2) COMMENT 'HR rating for correlation with client outcomes',
    LANGUAGES_SPOKEN VARCHAR(200) COMMENT 'Language skills for multicultural client service quality',

    TEAM_LEADER_ID VARCHAR(20) COMMENT 'Team leader for escalation and support',
    TEAM_LEADER_NAME VARCHAR(201) COMMENT 'Team leader name for coaching accountability',

    TOTAL_CLIENTS NUMBER(10,0) COMMENT 'Current client count for workload and revenue calculations',
    HIGH_RISK_CLIENTS NUMBER(10,0) COMMENT 'High-risk clients requiring enhanced monitoring',
    HIGH_RISK_PERCENTAGE NUMBER(10,2) COMMENT 'Risk concentration for compliance oversight',

    TOTAL_PORTFOLIO_VALUE NUMBER(18,2) COMMENT 'Total AUM for revenue forecasting and incentive compensation',
    AVG_CLIENT_BALANCE NUMBER(18,2) COMMENT 'Average client value for service tier optimization',
    MAX_CLIENT_BALANCE NUMBER(18,2) COMMENT 'Largest client for key account management',

    TOTAL_CLIENT_ACCOUNTS NUMBER(10,0) COMMENT 'Total accounts for cross-sell success measurement',
    AVG_ACCOUNTS_PER_CLIENT NUMBER(10,2) COMMENT 'Products per client for relationship depth KPI',

    TOTAL_TRANSACTIONS NUMBER(10,0) COMMENT 'Total transactions for activity-based portfolio quality',
    AVG_TRANSACTIONS_PER_CLIENT NUMBER(10,2) COMMENT 'Transaction frequency for engagement health score',

    CRITICAL_RISK_CLIENTS NUMBER(10,0) COMMENT 'Critical risk clients requiring immediate attention',
    HIGH_RISK_CLIENTS_CLASSIFICATION NUMBER(10,0) COMMENT 'High risk classification for portfolio quality',

    MOST_RECENT_ASSIGNMENT DATE COMMENT 'Latest assignment date for onboarding activity tracking',
    FIRST_ASSIGNMENT DATE COMMENT 'First assignment for advisor seniority with book',

    CAPACITY_UTILIZATION_PCT NUMBER(10,2) COMMENT 'Utilization percentage for workload balancing decisions',
    AVAILABLE_CAPACITY NUMBER(10,0) COMMENT 'Remaining capacity for new client assignments',

    WORKLOAD_STATUS VARCHAR(20) COMMENT 'Status flag for hiring needs and rebalancing priorities'
)
COMMENT = 'Client advisor performance metrics including portfolio value, client counts, and workload status. Refreshed hourly for management dashboards.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT 
    e.EMPLOYEE_ID,                                                    
    e.FIRST_NAME || ' ' || e.FAMILY_NAME as ADVISOR_NAME,            
    e.COUNTRY,                                                        
    e.REGION,                                                         
    e.HIRE_DATE,                                                      
    DATEDIFF(day, e.HIRE_DATE, CURRENT_DATE()) as TENURE_DAYS,      
    e.EMPLOYMENT_STATUS,                                              
    e.PERFORMANCE_RATING,                                             
    e.LANGUAGES_SPOKEN,                                               

    e.MANAGER_EMPLOYEE_ID as TEAM_LEADER_ID,                         
    m.FIRST_NAME || ' ' || m.FAMILY_NAME as TEAM_LEADER_NAME,        

    COUNT(DISTINCT a.CUSTOMER_ID) as TOTAL_CLIENTS,                  
    COUNT(DISTINCT CASE WHEN c.HAS_ANOMALY THEN a.CUSTOMER_ID END) as HIGH_RISK_CLIENTS, 
    ROUND(COUNT(DISTINCT CASE WHEN c.HAS_ANOMALY THEN a.CUSTOMER_ID END) * 100.0 / 
          NULLIF(COUNT(DISTINCT a.CUSTOMER_ID), 0), 2) as HIGH_RISK_PERCENTAGE, 

    COALESCE(SUM(c360.TOTAL_BALANCE), 0) as TOTAL_PORTFOLIO_VALUE,  
    COALESCE(AVG(c360.TOTAL_BALANCE), 0) as AVG_CLIENT_BALANCE,     
    COALESCE(MAX(c360.TOTAL_BALANCE), 0) as MAX_CLIENT_BALANCE,     

    COALESCE(SUM(c360.TOTAL_ACCOUNTS), 0) as TOTAL_CLIENT_ACCOUNTS, 
    COALESCE(AVG(c360.TOTAL_ACCOUNTS), 0) as AVG_ACCOUNTS_PER_CLIENT, 

    COALESCE(SUM(c360.TOTAL_TRANSACTIONS), 0) as TOTAL_TRANSACTIONS, 
    COALESCE(AVG(c360.TOTAL_TRANSACTIONS), 0) as AVG_TRANSACTIONS_PER_CLIENT, 

    COUNT(DISTINCT CASE WHEN c360.RISK_CLASSIFICATION = 'CRITICAL' THEN a.CUSTOMER_ID END) as CRITICAL_RISK_CLIENTS, 
    COUNT(DISTINCT CASE WHEN c360.RISK_CLASSIFICATION = 'HIGH' THEN a.CUSTOMER_ID END) as HIGH_RISK_CLIENTS_CLASSIFICATION, 

    MAX(a.ASSIGNMENT_START_DATE) as MOST_RECENT_ASSIGNMENT,         
    MIN(a.ASSIGNMENT_START_DATE) as FIRST_ASSIGNMENT,               

    ROUND(COUNT(DISTINCT a.CUSTOMER_ID) / 200.0 * 100, 2) as CAPACITY_UTILIZATION_PCT, 
    200 - COUNT(DISTINCT a.CUSTOMER_ID) as AVAILABLE_CAPACITY,      

    CASE 
        WHEN COUNT(DISTINCT a.CUSTOMER_ID) >= 180 THEN 'AT_CAPACITY'
        WHEN COUNT(DISTINCT a.CUSTOMER_ID) >= 150 THEN 'HIGH_LOAD'
        WHEN COUNT(DISTINCT a.CUSTOMER_ID) >= 100 THEN 'BALANCED'
        ELSE 'LOW_LOAD'
    END as WORKLOAD_STATUS                                           

FROM {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE e
LEFT JOIN {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE m ON e.MANAGER_EMPLOYEE_ID = m.EMPLOYEE_ID
LEFT JOIN {{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT a 
    ON e.EMPLOYEE_ID = a.ADVISOR_EMPLOYEE_ID 
    AND a.IS_CURRENT = TRUE
LEFT JOIN {{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_CURRENT c 
    ON a.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN {{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360 c360 
    ON a.CUSTOMER_ID = c360.CUSTOMER_ID
WHERE e.POSITION_LEVEL = 'CLIENT_ADVISOR'
GROUP BY 
    e.EMPLOYEE_ID, e.FIRST_NAME, e.FAMILY_NAME, e.COUNTRY, e.REGION, 
    e.HIRE_DATE, e.EMPLOYMENT_STATUS, e.PERFORMANCE_RATING, e.LANGUAGES_SPOKEN,
    e.MANAGER_EMPLOYEE_ID, m.FIRST_NAME, m.FAMILY_NAME;

DEFINE DYNAMIC TABLE {{ db }}.{{ crm_agg }}.EMPA_AGG_DT_PORTFOLIO_BY_ADVISOR(
    ADVISOR_EMPLOYEE_ID VARCHAR(20) COMMENT 'Advisor ID for portfolio performance attribution',
    ADVISOR_NAME VARCHAR(201) COMMENT 'Full name for wealth management reporting',
    COUNTRY VARCHAR(50) COMMENT 'Location for local market AUM tracking',
    REGION VARCHAR(50) COMMENT 'Region for divisional AUM aggregation',

    TOTAL_CLIENTS NUMBER(10,0) COMMENT 'Client count for relationship-based compensation models',
    TOTAL_AUM NUMBER(18,2) COMMENT 'Total assets under management for advisor ranking',
    AVG_AUM_PER_CLIENT NUMBER(18,2) COMMENT 'Average client value for service model segmentation',

    AUM_USD NUMBER(18,2) COMMENT 'USD exposure for FX risk management',
    AUM_EUR NUMBER(18,2) COMMENT 'EUR exposure for euro-zone strategy',
    AUM_GBP NUMBER(18,2) COMMENT 'GBP exposure for UK market tracking',
    AUM_CHF NUMBER(18,2) COMMENT 'CHF exposure for Swiss wealth management',
    AUM_OTHER NUMBER(18,2) COMMENT 'Other currencies (NOK, SEK, DKK, JPY, etc.) for diversified portfolio tracking',

    PREMIUM_CLIENTS NUMBER(10,0) COMMENT 'Premium tier count for white-glove service allocation',
    PLATINUM_CLIENTS NUMBER(10,0) COMMENT 'Platinum tier for VIP relationship management',
    GOLD_CLIENTS NUMBER(10,0) COMMENT 'Gold tier for enhanced service level tracking',

    ESTIMATED_ANNUAL_REVENUE NUMBER(18,2) COMMENT 'Annual fee estimate (1% AUM) for incentive compensation and budget planning'
)
COMMENT = 'Portfolio valuation aggregated by client advisor for AUM tracking and revenue estimation. Refreshed hourly for financial reporting.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT 
    a.ADVISOR_EMPLOYEE_ID,                                            
    e.FIRST_NAME || ' ' || e.FAMILY_NAME as ADVISOR_NAME,            
    e.COUNTRY,                                                        
    e.REGION,                                                         

    COUNT(DISTINCT a.CUSTOMER_ID) as TOTAL_CLIENTS,                  
    COALESCE(SUM(c360.TOTAL_BALANCE), 0) as TOTAL_AUM,              
    COALESCE(AVG(c360.TOTAL_BALANCE), 0) as AVG_AUM_PER_CLIENT,     

    COALESCE(SUM(CASE WHEN c360.REPORTING_CURRENCY = 'USD' THEN c360.TOTAL_BALANCE ELSE 0 END), 0) as AUM_USD, 
    COALESCE(SUM(CASE WHEN c360.REPORTING_CURRENCY = 'EUR' THEN c360.TOTAL_BALANCE ELSE 0 END), 0) as AUM_EUR, 
    COALESCE(SUM(CASE WHEN c360.REPORTING_CURRENCY = 'GBP' THEN c360.TOTAL_BALANCE ELSE 0 END), 0) as AUM_GBP, 
    COALESCE(SUM(CASE WHEN c360.REPORTING_CURRENCY = 'CHF' THEN c360.TOTAL_BALANCE ELSE 0 END), 0) as AUM_CHF, 
    COALESCE(SUM(CASE WHEN c360.REPORTING_CURRENCY NOT IN ('USD', 'EUR', 'GBP', 'CHF') OR c360.REPORTING_CURRENCY IS NULL THEN c360.TOTAL_BALANCE ELSE 0 END), 0) as AUM_OTHER, 

    COUNT(DISTINCT CASE WHEN c360.ACCOUNT_TIER = 'PREMIUM' THEN a.CUSTOMER_ID END) as PREMIUM_CLIENTS,  
    COUNT(DISTINCT CASE WHEN c360.ACCOUNT_TIER = 'PLATINUM' THEN a.CUSTOMER_ID END) as PLATINUM_CLIENTS,
    COUNT(DISTINCT CASE WHEN c360.ACCOUNT_TIER = 'GOLD' THEN a.CUSTOMER_ID END) as GOLD_CLIENTS,        

    COALESCE(SUM(c360.TOTAL_BALANCE), 0) * 0.01 as ESTIMATED_ANNUAL_REVENUE 

FROM {{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT a
JOIN {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE e ON a.ADVISOR_EMPLOYEE_ID = e.EMPLOYEE_ID
JOIN {{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360 c360 ON a.CUSTOMER_ID = c360.CUSTOMER_ID
WHERE a.IS_CURRENT = TRUE
GROUP BY a.ADVISOR_EMPLOYEE_ID, e.FIRST_NAME, e.FAMILY_NAME, e.COUNTRY, e.REGION;

DEFINE DYNAMIC TABLE {{ db }}.{{ crm_agg }}.EMPA_AGG_DT_TEAM_LEADER_DASHBOARD(
    TEAM_LEADER_ID VARCHAR(20) COMMENT 'Team leader ID for management reporting',
    TEAM_LEADER_NAME VARCHAR(201) COMMENT 'Full name for leadership dashboards',
    REGION VARCHAR(50) COMMENT 'Geographic region for divisional P and L',
    HIRE_DATE DATE COMMENT 'Start date for leadership tenure tracking',
    TL_PERFORMANCE_RATING DECIMAL(3,2) COMMENT 'Leader own rating for correlation with team outcomes',

    SUPER_LEADER_ID VARCHAR(20) COMMENT 'Super leader for executive roll-ups',
    SUPER_LEADER_NAME VARCHAR(201) COMMENT 'Super leader name for organizational reporting',

    TOTAL_ADVISORS NUMBER(10,0) COMMENT 'Total advisors for span-of-control analysis',
    ACTIVE_ADVISORS NUMBER(10,0) COMMENT 'Active headcount for capacity planning',

    AVG_ADVISOR_PERFORMANCE NUMBER(5,2) COMMENT 'Team average for leader effectiveness measurement',
    MIN_ADVISOR_PERFORMANCE DECIMAL(3,2) COMMENT 'Lowest performer for coaching focus',
    MAX_ADVISOR_PERFORMANCE DECIMAL(3,2) COMMENT 'Top performer for best practice sharing',

    TOTAL_CLIENTS NUMBER(10,0) COMMENT 'Total clients under team management',
    HIGH_RISK_CLIENTS NUMBER(10,0) COMMENT 'High-risk client concentration for oversight',

    TOTAL_TEAM_AUM NUMBER(18,2) COMMENT 'Total team portfolio for revenue attribution',
    AVG_CLIENT_BALANCE NUMBER(18,2) COMMENT 'Average client value for quality assessment',

    AVG_CLIENTS_PER_ADVISOR NUMBER(10,1) COMMENT 'Average workload for balance assessment',

    TEAM_CAPACITY_UTILIZATION_PCT NUMBER(10,2) COMMENT 'Team utilization for hiring decisions',
    TEAM_AVAILABLE_CAPACITY NUMBER(10,0) COMMENT 'Remaining capacity for growth planning',

    COUNTRIES_COVERED NUMBER(10,0) COMMENT 'Number of countries for international reach',
    COUNTRY_LIST VARCHAR(2000) COMMENT 'Country list for market coverage validation'
)
COMMENT = 'Team leader dashboard with aggregated team performance, workload, and portfolio metrics. Refreshed hourly for management reporting.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT 
    tl.EMPLOYEE_ID as TEAM_LEADER_ID,                                 
    tl.FIRST_NAME || ' ' || tl.FAMILY_NAME as TEAM_LEADER_NAME,      
    tl.REGION,                                                        
    tl.HIRE_DATE,                                                     
    tl.PERFORMANCE_RATING as TL_PERFORMANCE_RATING,                   

    tl.MANAGER_EMPLOYEE_ID as SUPER_LEADER_ID,                       
    stl.FIRST_NAME || ' ' || stl.FAMILY_NAME as SUPER_LEADER_NAME,  

    COUNT(DISTINCT adv.EMPLOYEE_ID) as TOTAL_ADVISORS,               
    COUNT(DISTINCT CASE WHEN adv.EMPLOYMENT_STATUS = 'ACTIVE' THEN adv.EMPLOYEE_ID END) as ACTIVE_ADVISORS, 

    AVG(adv.PERFORMANCE_RATING) as AVG_ADVISOR_PERFORMANCE,          
    MIN(adv.PERFORMANCE_RATING) as MIN_ADVISOR_PERFORMANCE,          
    MAX(adv.PERFORMANCE_RATING) as MAX_ADVISOR_PERFORMANCE,          

    COUNT(DISTINCT a.CUSTOMER_ID) as TOTAL_CLIENTS,                  
    COUNT(DISTINCT CASE WHEN c.HAS_ANOMALY THEN a.CUSTOMER_ID END) as HIGH_RISK_CLIENTS, 

    COALESCE(SUM(c360.TOTAL_BALANCE), 0) as TOTAL_TEAM_AUM,         
    COALESCE(AVG(c360.TOTAL_BALANCE), 0) as AVG_CLIENT_BALANCE,     

    ROUND(AVG(
        (SELECT COUNT(*) 
         FROM {{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT 
         WHERE ADVISOR_EMPLOYEE_ID = adv.EMPLOYEE_ID AND IS_CURRENT = TRUE)
    ), 1) as AVG_CLIENTS_PER_ADVISOR,                                

    ROUND(COUNT(DISTINCT a.CUSTOMER_ID) / (COUNT(DISTINCT adv.EMPLOYEE_ID) * 200.0) * 100, 2) as TEAM_CAPACITY_UTILIZATION_PCT, 
    (COUNT(DISTINCT adv.EMPLOYEE_ID) * 200) - COUNT(DISTINCT a.CUSTOMER_ID) as TEAM_AVAILABLE_CAPACITY, 

    COUNT(DISTINCT adv.COUNTRY) as COUNTRIES_COVERED,                
    LISTAGG(DISTINCT adv.COUNTRY, ', ') WITHIN GROUP (ORDER BY adv.COUNTRY) as COUNTRY_LIST 

FROM {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE tl
LEFT JOIN {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE stl ON tl.MANAGER_EMPLOYEE_ID = stl.EMPLOYEE_ID
LEFT JOIN {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE adv ON tl.EMPLOYEE_ID = adv.MANAGER_EMPLOYEE_ID
LEFT JOIN {{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT a 
    ON adv.EMPLOYEE_ID = a.ADVISOR_EMPLOYEE_ID 
    AND a.IS_CURRENT = TRUE
LEFT JOIN {{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_CURRENT c ON a.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN {{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360 c360 ON a.CUSTOMER_ID = c360.CUSTOMER_ID
WHERE tl.POSITION_LEVEL = 'TEAM_LEADER'
GROUP BY 
    tl.EMPLOYEE_ID, tl.FIRST_NAME, tl.FAMILY_NAME, tl.REGION, tl.HIRE_DATE, 
    tl.PERFORMANCE_RATING, tl.MANAGER_EMPLOYEE_ID, stl.FIRST_NAME, stl.FAMILY_NAME;

DEFINE VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_CURRENT_ASSIGNMENTS
COMMENT = 'Current active client-advisor assignments with customer and advisor details'
AS
SELECT 
    a.ASSIGNMENT_ID,                                                  

    a.CUSTOMER_ID,                                                    
    c.FIRST_NAME || ' ' || c.FAMILY_NAME as CUSTOMER_NAME,           
    c360.COUNTRY as CUSTOMER_COUNTRY,                                 
    c.HAS_ANOMALY as IS_HIGH_RISK_CUSTOMER,                          

    a.ADVISOR_EMPLOYEE_ID,                                            
    e.FIRST_NAME || ' ' || e.FAMILY_NAME as ADVISOR_NAME,            
    e.COUNTRY as ADVISOR_COUNTRY,                                     
    e.REGION as ADVISOR_REGION,                                       
    e.PERFORMANCE_RATING as ADVISOR_RATING,                           

    e.MANAGER_EMPLOYEE_ID as TEAM_LEADER_ID,                         
    tl.FIRST_NAME || ' ' || tl.FAMILY_NAME as TEAM_LEADER_NAME,     

    a.ASSIGNMENT_START_DATE,                                          
    DATEDIFF(day, a.ASSIGNMENT_START_DATE, CURRENT_DATE()) as ASSIGNMENT_DURATION_DAYS, 
    ROUND(DATEDIFF(day, a.ASSIGNMENT_START_DATE, CURRENT_DATE()) / 365.25, 1) as ASSIGNMENT_DURATION_YEARS, 
    a.ASSIGNMENT_REASON,                                              

    c360.TOTAL_BALANCE,                                               
    c360.TOTAL_TRANSACTIONS,                                          
    c360.TOTAL_ACCOUNTS,                                              
    c360.ACCOUNT_TIER,                                                
    c360.RISK_CLASSIFICATION,                                         
    c360.DAYS_SINCE_LAST_TRANSACTION                                  

FROM {{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT a
JOIN {{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_CURRENT c ON a.CUSTOMER_ID = c.CUSTOMER_ID
JOIN {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE e ON a.ADVISOR_EMPLOYEE_ID = e.EMPLOYEE_ID
LEFT JOIN {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE tl ON e.MANAGER_EMPLOYEE_ID = tl.EMPLOYEE_ID
LEFT JOIN {{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360 c360 ON a.CUSTOMER_ID = c360.CUSTOMER_ID
WHERE a.IS_CURRENT = TRUE;

DEFINE VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_ASSIGNMENT_HISTORY
COMMENT = 'Complete assignment history with SCD Type 2 tracking for audit trails and compliance'
AS
SELECT 
    a.ASSIGNMENT_ID,                                                  

    a.CUSTOMER_ID,                                                    
    c.FIRST_NAME || ' ' || c.FAMILY_NAME as CUSTOMER_NAME,           

    a.ADVISOR_EMPLOYEE_ID,                                            
    e.FIRST_NAME || ' ' || e.FAMILY_NAME as ADVISOR_NAME,            
    e.COUNTRY as ADVISOR_COUNTRY,                                     

    a.ASSIGNMENT_START_DATE,                                          
    a.ASSIGNMENT_END_DATE,                                            
    a.IS_CURRENT,                                                     
    COALESCE(
        DATEDIFF(day, a.ASSIGNMENT_START_DATE, a.ASSIGNMENT_END_DATE),
        DATEDIFF(day, a.ASSIGNMENT_START_DATE, CURRENT_DATE())
    ) as ASSIGNMENT_DURATION_DAYS,                                   

    a.ASSIGNMENT_REASON,                                              

    CASE WHEN a.ASSIGNMENT_END_DATE IS NULL THEN 'ACTIVE' ELSE 'ENDED' END as ASSIGNMENT_STATUS, 
    CASE 
        WHEN a.ASSIGNMENT_REASON = 'TRANSFER' THEN 'Advisor Change'
        WHEN a.ASSIGNMENT_REASON = 'ESCALATION' THEN 'Escalated to Senior'
        WHEN a.ASSIGNMENT_REASON = 'REBALANCING' THEN 'Workload Rebalancing'
        ELSE 'Initial Assignment'
    END as ASSIGNMENT_TYPE_DESCRIPTION                                

FROM {{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT a
JOIN {{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_CURRENT c ON a.CUSTOMER_ID = c.CUSTOMER_ID
JOIN {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE e ON a.ADVISOR_EMPLOYEE_ID = e.EMPLOYEE_ID;

DEFINE VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_WORKLOAD_DISTRIBUTION
COMMENT = 'Workload distribution analysis by country and region for capacity planning and rebalancing'
AS
SELECT 
    e.COUNTRY,                                                        
    e.REGION,                                                         

    COUNT(DISTINCT e.EMPLOYEE_ID) as ADVISOR_COUNT,                  
    COUNT(DISTINCT a.CUSTOMER_ID) as TOTAL_CLIENTS,                  

    ROUND(COUNT(DISTINCT a.CUSTOMER_ID) / NULLIF(COUNT(DISTINCT e.EMPLOYEE_ID), 0), 1) as AVG_CLIENTS_PER_ADVISOR, 
    MIN(client_counts.CLIENT_COUNT) as MIN_CLIENTS_PER_ADVISOR,      
    MAX(client_counts.CLIENT_COUNT) as MAX_CLIENTS_PER_ADVISOR,      
    ROUND(STDDEV(client_counts.CLIENT_COUNT), 1) as STDDEV_CLIENTS, 

    COUNT(DISTINCT e.EMPLOYEE_ID) * 200 as TOTAL_CAPACITY,          
    COUNT(DISTINCT a.CUSTOMER_ID) as USED_CAPACITY,                 
    (COUNT(DISTINCT e.EMPLOYEE_ID) * 200) - COUNT(DISTINCT a.CUSTOMER_ID) as AVAILABLE_CAPACITY, 
    ROUND(COUNT(DISTINCT a.CUSTOMER_ID) / (COUNT(DISTINCT e.EMPLOYEE_ID) * 200.0) * 100, 2) as CAPACITY_UTILIZATION_PCT, 

    CASE 
        WHEN STDDEV(client_counts.CLIENT_COUNT) < 20 THEN 'WELL_BALANCED'
        WHEN STDDEV(client_counts.CLIENT_COUNT) < 40 THEN 'MODERATELY_BALANCED'
        ELSE 'IMBALANCED'
    END as BALANCE_STATUS                                            

FROM {{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE e
LEFT JOIN {{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT a 
    ON e.EMPLOYEE_ID = a.ADVISOR_EMPLOYEE_ID 
    AND a.IS_CURRENT = TRUE
LEFT JOIN (
    SELECT 
        ADVISOR_EMPLOYEE_ID,
        COUNT(DISTINCT CUSTOMER_ID) as CLIENT_COUNT
    FROM {{ crm_raw }}.EMPI_RAW_TB_CLIENT_ASSIGNMENT
    WHERE IS_CURRENT = TRUE
    GROUP BY ADVISOR_EMPLOYEE_ID
) client_counts ON e.EMPLOYEE_ID = client_counts.ADVISOR_EMPLOYEE_ID
WHERE e.POSITION_LEVEL = 'CLIENT_ADVISOR'
GROUP BY e.COUNTRY, e.REGION;

DEFINE VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_ADVISORS
COMMENT = 'Alias view for {{ crm_agg }}.EMPA_AGG_DT_ADVISOR_PERFORMANCE to match semantic view references'
AS SELECT 
    EMPLOYEE_ID,
    ADVISOR_NAME AS FULL_NAME,
    ADVISOR_NAME AS FIRST_NAME,
    ADVISOR_NAME AS FAMILY_NAME,
    NULL AS EMAIL,
    NULL AS PHONE,
    NULL AS DATE_OF_BIRTH,
    HIRE_DATE,
    TEAM_LEADER_ID AS MANAGER_EMPLOYEE_ID,
    LANGUAGES_SPOKEN,
    NULL AS CERTIFICATIONS,
    TOTAL_CLIENTS,
    TOTAL_PORTFOLIO_VALUE AS TOTAL_AUM,
    AVG_CLIENT_BALANCE,
    TOTAL_CLIENTS AS ACTIVE_CLIENTS,
    0 AS CLOSED_CLIENTS,
    CASE WHEN TOTAL_CLIENTS > 0 THEN 100.0 ELSE 0 END AS CLIENT_RETENTION_RATE,
    TOTAL_CLIENT_ACCOUNTS AS TOTAL_ACCOUNTS_MANAGED,
    NULL AS TOTAL_CHECKING_BALANCE,
    NULL AS TOTAL_SAVINGS_BALANCE,
    NULL AS TOTAL_INVESTMENT_BALANCE,
    HIGH_RISK_CLIENTS,
    0 AS PREMIUM_CLIENTS,
    COUNTRY,
    REGION,
    EMPLOYMENT_STATUS,
    NULL AS POSITION_LEVEL,
    PERFORMANCE_RATING,
    NULL AS OFFICE_LOCATION
FROM {{ crm_agg }}.EMPA_AGG_DT_ADVISOR_PERFORMANCE;
