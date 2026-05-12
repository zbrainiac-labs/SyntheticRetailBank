"""
Data Loading Functions
Handles data loading from Snowflake with caching
"""

import streamlit as st
import pandas as pd
from .snowflake_connection import get_snowflake_session


@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_customer_360():
    """
    Load complete customer 360° data
    
    Returns:
        pandas.DataFrame: Customer 360 data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT *
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            ORDER BY CUSTOMER_ID
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading customer 360 data: {str(e)}")
        # Return empty DataFrame with expected columns
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_high_risk_customers():
    """
    Load high-risk customers requiring review
    
    Returns:
        pandas.DataFrame: High-risk customer data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                CUSTOMER_ID,
                FULL_NAME,
                COUNTRY,
                ACCOUNT_TIER,
                OVERALL_RISK_RATING,
                OVERALL_RISK_SCORE,
                EXPOSED_PERSON_MATCH_TYPE,
                EXPOSED_PERSON_MATCH_ACCURACY_PERCENT,
                SANCTIONS_MATCH_TYPE,
                SANCTIONS_MATCH_ACCURACY_PERCENT,
                REQUIRES_EXPOSED_PERSON_REVIEW,
                REQUIRES_SANCTIONS_REVIEW,
                HIGH_RISK_CUSTOMER
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            WHERE HIGH_RISK_CUSTOMER = TRUE
               OR REQUIRES_EXPOSED_PERSON_REVIEW = TRUE
               OR REQUIRES_SANCTIONS_REVIEW = TRUE
            ORDER BY OVERALL_RISK_SCORE DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading high-risk customers: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_risk_distribution():
    """
    Load risk distribution summary
    
    Returns:
        pandas.DataFrame: Risk distribution counts
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                OVERALL_RISK_RATING,
                COUNT(*) as CUSTOMER_COUNT,
                AVG(OVERALL_RISK_SCORE) as AVG_RISK_SCORE
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            GROUP BY OVERALL_RISK_RATING
            ORDER BY AVG_RISK_SCORE DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading risk distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_pep_sanctions_summary():
    """
    Load PEP and sanctions screening summary
    
    Returns:
        tuple: (pep_df, sanctions_df)
    """
    try:
        session = get_snowflake_session()
        
        pep_query = """
            SELECT 
                EXPOSED_PERSON_MATCH_TYPE,
                COUNT(*) as COUNT,
                AVG(EXPOSED_PERSON_MATCH_ACCURACY_PERCENT) as AVG_ACCURACY,
                SUM(CASE WHEN REQUIRES_EXPOSED_PERSON_REVIEW THEN 1 ELSE 0 END) as REQUIRES_REVIEW
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            GROUP BY EXPOSED_PERSON_MATCH_TYPE
            ORDER BY COUNT DESC
        """
        
        sanctions_query = """
            SELECT 
                SANCTIONS_MATCH_TYPE,
                COUNT(*) as COUNT,
                AVG(SANCTIONS_MATCH_ACCURACY_PERCENT) as AVG_ACCURACY,
                SUM(CASE WHEN REQUIRES_SANCTIONS_REVIEW THEN 1 ELSE 0 END) as REQUIRES_REVIEW
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            GROUP BY SANCTIONS_MATCH_TYPE
            ORDER BY COUNT DESC
        """
        
        pep_df = session.sql(pep_query).to_pandas()
        sanctions_df = session.sql(sanctions_query).to_pandas()
        
        return pep_df, sanctions_df
    
    except Exception as e:
        st.error(f"Error loading PEP/Sanctions summary: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()


@st.cache_data(ttl=3600)
def load_account_tier_distribution():
    """
    Load account tier distribution
    
    Returns:
        pandas.DataFrame: Tier distribution with account holdings
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                ACCOUNT_TIER,
                COUNT(*) as CUSTOMER_COUNT,
                AVG(TOTAL_ACCOUNTS) as AVG_TOTAL_ACCOUNTS,
                AVG(CHECKING_ACCOUNTS) as AVG_CHECKING,
                AVG(SAVINGS_ACCOUNTS) as AVG_SAVINGS,
                AVG(BUSINESS_ACCOUNTS) as AVG_BUSINESS,
                AVG(INVESTMENT_ACCOUNTS) as AVG_INVESTMENT
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            GROUP BY ACCOUNT_TIER
            ORDER BY CUSTOMER_COUNT DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading account tier distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_geographic_distribution():
    """
    Load geographic distribution of customers
    
    Returns:
        pandas.DataFrame: Customer counts by country
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                COUNTRY,
                COUNT(*) as CUSTOMER_COUNT,
                COUNT(DISTINCT ACCOUNT_TIER) as TIER_DIVERSITY,
                AVG(OVERALL_RISK_SCORE) as AVG_RISK_SCORE
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            GROUP BY COUNTRY
            ORDER BY CUSTOMER_COUNT DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading geographic distribution: {str(e)}")
        return pd.DataFrame()


# ============================================================
# AML & Transaction Monitoring Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_aml_alerts():
    """
    Load AML transaction monitoring alerts
    
    Returns:
        pandas.DataFrame: AML alert data
    """
    try:
        session = get_snowflake_session()
        
        # Try PAY_AGG_001 schema first, then fallback to default schema
        queries = [
            "SELECT * FROM PAY_AGG_001.PAYA_AGG_DT_TRANSACTION_ANOMALIES ORDER BY BOOKING_DATE DESC LIMIT 1000",
            "SELECT * FROM PAYA_AGG_DT_TRANSACTION_ANOMALIES ORDER BY BOOKING_DATE DESC LIMIT 1000"
        ]
        
        for query in queries:
            try:
                df = session.sql(query).to_pandas()
                st.caption(f"✅ Loaded {len(df)} AML alert records")
                if len(df) > 0:
                    st.caption(f"📊 Columns: {', '.join(df.columns.tolist()[:10])}")  # Show first 10 columns
                    if 'BOOKING_DATE' in df.columns:
                        st.caption(f"📅 Date range: {df['BOOKING_DATE'].min()} to {df['BOOKING_DATE'].max()}")
                return df
            except Exception:
                continue  # Try next query
        
        # If all queries failed
        st.warning("⚠️ PAYA_AGG_DT_TRANSACTION_ANOMALIES table not found or empty. PAY_AGG_001 schema may not be deployed yet.")
        return pd.DataFrame()
    
    except Exception as e:
        st.error(f"Error loading AML alerts: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_aml_metrics():
    """
    Load AML key metrics summary
    
    Returns:
        dict: Dictionary of AML metrics
    """
    try:
        session = get_snowflake_session()
        
        # Try PAY_AGG_001 schema first, then fallback to default schema
        queries = [
            """
            SELECT 
                COUNT(*) as TOTAL_ALERTS,
                COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS,
                COUNT(*) as ANOMALOUS_TRANSACTIONS
            FROM PAY_AGG_001.PAYA_AGG_DT_TRANSACTION_ANOMALIES
            WHERE BOOKING_DATE >= DATEADD(day, -90, CURRENT_DATE())
            """,
            """
            SELECT 
                COUNT(*) as TOTAL_ALERTS,
                COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS,
                COUNT(*) as ANOMALOUS_TRANSACTIONS
            FROM PAYA_AGG_DT_TRANSACTION_ANOMALIES
            WHERE BOOKING_DATE >= DATEADD(day, -90, CURRENT_DATE())
            """
        ]
        
        for query in queries:
            try:
                df = session.sql(query).to_pandas()
                return df.iloc[0].to_dict() if len(df) > 0 else {}
            except Exception:
                continue  # Try next query
        
        # If all queries failed
        return {}
    
    except Exception as e:
        return {}


# ============================================================
# Lending Operations Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_lending_portfolio():
    """
    Load lending portfolio overview
    
    Returns:
        pandas.DataFrame: Lending portfolio data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                CUSTOMER_ID,
                FULL_NAME,
                COUNTRY,
                CREDIT_SCORE_BAND,
                RISK_CLASSIFICATION,
                ACCOUNT_TIER,
                TOTAL_ACCOUNTS
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            ORDER BY CUSTOMER_ID
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading lending portfolio: {str(e)}")
        return pd.DataFrame()


# ============================================================
# Wealth Management Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_wealth_portfolios():
    """
    Load wealth management portfolios
    
    Returns:
        pandas.DataFrame: Wealth portfolio data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                ADVISOR_ID,
                ADVISOR_NAME,
                TOTAL_AUM,
                CLIENT_COUNT,
                AVG_AUM_PER_CLIENT,
                PERFORMANCE_RATING,
                REGION
            FROM EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED
            ORDER BY TOTAL_AUM DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_advisor_performance():
    """
    Load advisor performance metrics
    
    Returns:
        pandas.DataFrame: Advisor performance data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT *
            FROM EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        # Log error for debugging
        st.error(f"Error loading advisor performance: {str(e)}")
        return pd.DataFrame()


# ============================================================
# Sanctions Control Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_sanctions_matches():
    """
    Load sanctions screening matches
    
    Returns:
        pandas.DataFrame: Sanctions match data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                CUSTOMER_ID,
                FULL_NAME,
                COUNTRY,
                SANCTIONS_MATCH_TYPE,
                SANCTIONS_MATCH_ACCURACY_PERCENT,
                REQUIRES_SANCTIONS_REVIEW,
                OVERALL_SANCTIONS_RISK
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            WHERE SANCTIONS_MATCH_TYPE != 'NO_MATCH'
            ORDER BY SANCTIONS_MATCH_ACCURACY_PERCENT DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading sanctions matches: {str(e)}")
        return pd.DataFrame()


# ============================================================
# Employee/Advisor Management Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_advisor_capacity():
    """
    Load advisor capacity and workload
    
    Returns:
        pandas.DataFrame: Advisor capacity data
    """
    try:
        session = get_snowflake_session()
        
        # Query all columns and let the app handle missing ones
        query = """
            SELECT 
                EMPLOYEE_ID,
                ADVISOR_NAME,
                TOTAL_CLIENTS,
                WORKLOAD_STATUS,
                AVAILABLE_CAPACITY,
                TOTAL_PORTFOLIO_VALUE,
                REGION,
                COUNTRY,
                CAPACITY_UTILIZATION_PCT,
                TOTAL_TRANSACTIONS,
                HIGH_RISK_CLIENTS,
                PERFORMANCE_RATING,
                EMPLOYMENT_STATUS
            FROM EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED
            ORDER BY AVAILABLE_CAPACITY DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading advisor capacity: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_team_performance():
    """
    Load team leader dashboard data
    
    Returns:
        pandas.DataFrame: Team performance metrics
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT *
            FROM EMPA_AGG_DT_TEAM_LEADER_DASHBOARD
            ORDER BY TOTAL_TEAM_AUM DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        return pd.DataFrame()


# ============================================================
# KYC & Screening Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_pep_matches():
    """
    Load PEP (Politically Exposed Persons) matches
    
    Returns:
        pandas.DataFrame: PEP match data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                CUSTOMER_ID,
                FULL_NAME,
                COUNTRY,
                EXPOSED_PERSON_MATCH_TYPE,
                EXPOSED_PERSON_MATCH_ACCURACY_PERCENT,
                REQUIRES_EXPOSED_PERSON_REVIEW,
                OVERALL_EXPOSED_PERSON_RISK
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            WHERE EXPOSED_PERSON_MATCH_TYPE != 'NO_MATCH'
            ORDER BY EXPOSED_PERSON_MATCH_ACCURACY_PERCENT DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading PEP matches: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_kyc_completeness():
    """
    Load KYC completeness metrics
    
    Returns:
        pandas.DataFrame: KYC completeness data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                COUNTRY,
                COUNT(*) as TOTAL_CUSTOMERS,
                SUM(CASE WHEN EMAIL IS NOT NULL THEN 1 ELSE 0 END) as EMAIL_COMPLETE,
                SUM(CASE WHEN PHONE IS NOT NULL THEN 1 ELSE 0 END) as PHONE_COMPLETE,
                SUM(CASE WHEN EMPLOYER IS NOT NULL THEN 1 ELSE 0 END) as EMPLOYER_COMPLETE,
                ROUND(AVG(CASE WHEN EMAIL IS NOT NULL THEN 100 ELSE 0 END), 2) as EMAIL_PCT,
                ROUND(AVG(CASE WHEN PHONE IS NOT NULL THEN 100 ELSE 0 END), 2) as PHONE_PCT
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
            GROUP BY COUNTRY
            ORDER BY TOTAL_CUSTOMERS DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading KYC completeness: {str(e)}")
        return pd.DataFrame()


# ============================================================
# Data Quality & Controls Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_data_quality_metrics():
    """
    Load data quality assessment metrics
    
    Returns:
        dict: Data quality metrics
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                COUNT(*) as TOTAL_RECORDS,
                COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS,
                SUM(CASE WHEN EMAIL IS NULL THEN 1 ELSE 0 END) as MISSING_EMAIL,
                SUM(CASE WHEN PHONE IS NULL THEN 1 ELSE 0 END) as MISSING_PHONE,
                SUM(CASE WHEN DATE_OF_BIRTH IS NULL THEN 1 ELSE 0 END) as MISSING_DOB,
                SUM(CASE WHEN STREET_ADDRESS IS NULL THEN 1 ELSE 0 END) as MISSING_ADDRESS,
                ROUND((COUNT(EMAIL) * 100.0 / COUNT(*)), 2) as EMAIL_COMPLETENESS,
                ROUND((COUNT(PHONE) * 100.0 / COUNT(*)), 2) as PHONE_COMPLETENESS,
                ROUND((COUNT(DATE_OF_BIRTH) * 100.0 / COUNT(*)), 2) as DOB_COMPLETENESS,
                ROUND((COUNT(STREET_ADDRESS) * 100.0 / COUNT(*)), 2) as ADDRESS_COMPLETENESS
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
        """
        
        df = session.sql(query).to_pandas()
        return df.iloc[0].to_dict() if len(df) > 0 else {}
    
    except Exception as e:
        st.error(f"Error loading data quality metrics: {str(e)}")
        return {}


# ============================================================
# Compliance Risk Management Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_compliance_risk_summary():
    """
    Load overall compliance risk profile
    
    Returns:
        dict: Compliance risk summary metrics
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                COUNT(*) as TOTAL_CUSTOMERS,
                SUM(CASE WHEN HIGH_RISK_CUSTOMER THEN 1 ELSE 0 END) as HIGH_RISK_COUNT,
                SUM(CASE WHEN EXPOSED_PERSON_MATCH_TYPE != 'NO_MATCH' THEN 1 ELSE 0 END) as PEP_MATCHES,
                SUM(CASE WHEN SANCTIONS_MATCH_TYPE != 'NO_MATCH' THEN 1 ELSE 0 END) as SANCTIONS_MATCHES,
                SUM(CASE WHEN REQUIRES_EXPOSED_PERSON_REVIEW THEN 1 ELSE 0 END) as PEP_REVIEWS_NEEDED,
                SUM(CASE WHEN REQUIRES_SANCTIONS_REVIEW THEN 1 ELSE 0 END) as SANCTIONS_REVIEWS_NEEDED,
                COALESCE(SUM(CASE WHEN HAS_ANOMALY THEN 1 ELSE 0 END), 0) as ANOMALY_COUNT,
                ROUND(AVG(OVERALL_RISK_SCORE), 2) as AVG_RISK_SCORE
            FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED
        """
        
        df = session.sql(query).to_pandas()
        return df.iloc[0].to_dict() if len(df) > 0 else {}
    
    except Exception as e:
        st.error(f"Error loading compliance risk summary: {str(e)}")
        return {}


# ============================================================
# Churn & Lifecycle Management Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_customer_lifecycle():
    """
    Load customer lifecycle data
    
    Returns:
        pandas.DataFrame: Customer lifecycle data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT *
            FROM CRMA_AGG_DT_CUSTOMER_LIFECYCLE
            ORDER BY CUSTOMER_ID
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading lifecycle data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_lifecycle_summary():
    """
    Load lifecycle stage distribution summary
    
    Returns:
        pandas.DataFrame: Lifecycle stage counts
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                LIFECYCLE_STAGE,
                COUNT(*) as CUSTOMER_COUNT,
                AVG(CHURN_PROBABILITY) as AVG_CHURN_PROBABILITY,
                AVG(DAYS_SINCE_LAST_TRANSACTION) as AVG_DAYS_INACTIVE
            FROM CRMA_AGG_DT_CUSTOMER_LIFECYCLE
            GROUP BY LIFECYCLE_STAGE
            ORDER BY 
                CASE LIFECYCLE_STAGE
                    WHEN 'NEW' THEN 1
                    WHEN 'ACTIVE' THEN 2
                    WHEN 'MATURE' THEN 3
                    WHEN 'DECLINING' THEN 4
                    WHEN 'DORMANT' THEN 5
                    WHEN 'CHURNED' THEN 6
                    ELSE 7
                END
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading lifecycle summary: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_high_churn_risk_customers():
    """
    Load high churn risk customers (>70% probability)
    
    Returns:
        pandas.DataFrame: High churn risk customer data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                l.CUSTOMER_ID,
                l.FIRST_NAME,
                l.FAMILY_NAME,
                l.LIFECYCLE_STAGE,
                l.CHURN_PROBABILITY,
                l.DAYS_SINCE_LAST_TRANSACTION,
                l.LAST_TRANSACTION_DATE,
                c.ACCOUNT_TIER,
                c.COUNTRY,
                c.EMAIL,
                c.PHONE
            FROM CRMA_AGG_DT_CUSTOMER_LIFECYCLE l
            LEFT JOIN CRMA_AGG_VW_CUSTOMER_360_ENRICHED c ON l.CUSTOMER_ID = c.CUSTOMER_ID
            WHERE l.CHURN_PROBABILITY > 70
            ORDER BY l.CHURN_PROBABILITY DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading high churn risk customers: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_premium_at_risk():
    """
    Load GOLD/PLATINUM customers at risk of churning
    
    Returns:
        pandas.DataFrame: Premium customers with high churn risk
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                l.CUSTOMER_ID,
                l.FIRST_NAME,
                l.FAMILY_NAME,
                c.ACCOUNT_TIER,
                c.COUNTRY,
                l.LIFECYCLE_STAGE,
                l.CHURN_PROBABILITY,
                l.DAYS_SINCE_LAST_TRANSACTION,
                l.LAST_TRANSACTION_DATE,
                c.EMAIL,
                c.PHONE,
                c.PREFERRED_CONTACT_METHOD
            FROM CRMA_AGG_DT_CUSTOMER_LIFECYCLE l
            LEFT JOIN CRMA_AGG_VW_CUSTOMER_360_ENRICHED c ON l.CUSTOMER_ID = c.CUSTOMER_ID
            WHERE c.ACCOUNT_TIER IN ('GOLD', 'PLATINUM')
              AND l.CHURN_PROBABILITY > 70
            ORDER BY l.CHURN_PROBABILITY DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading premium at risk: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_dormant_accounts():
    """
    Load dormant accounts (inactive >180 days)
    
    Returns:
        pandas.DataFrame: Dormant account data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                l.CUSTOMER_ID,
                l.FIRST_NAME,
                l.FAMILY_NAME,
                c.ACCOUNT_TIER,
                c.COUNTRY,
                l.LIFECYCLE_STAGE,
                l.DAYS_SINCE_LAST_TRANSACTION,
                l.LAST_TRANSACTION_DATE,
                l.CHURN_PROBABILITY,
                c.EMAIL,
                c.PHONE,
                c.TOTAL_ACCOUNTS
            FROM CRMA_AGG_DT_CUSTOMER_LIFECYCLE l
            LEFT JOIN CRMA_AGG_VW_CUSTOMER_360_ENRICHED c ON l.CUSTOMER_ID = c.CUSTOMER_ID
            WHERE l.DAYS_SINCE_LAST_TRANSACTION > 180
              AND l.LIFECYCLE_STAGE IN ('DORMANT', 'DECLINING')
            ORDER BY l.DAYS_SINCE_LAST_TRANSACTION DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading dormant accounts: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def calculate_revenue_at_risk():
    """
    Calculate revenue at risk from potential churn
    
    Returns:
        dict: Revenue at risk metrics
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                COUNT(*) as AT_RISK_CUSTOMERS,
                COUNT(CASE WHEN CHURN_PROBABILITY > 90 THEN 1 END) as CRITICAL_CUSTOMERS,
                COUNT(CASE WHEN CHURN_PROBABILITY BETWEEN 70 AND 90 THEN 1 END) as HIGH_CUSTOMERS,
                AVG(CHURN_PROBABILITY) as AVG_CHURN_PROBABILITY
            FROM CRMA_AGG_DT_CUSTOMER_LIFECYCLE
            WHERE CHURN_PROBABILITY > 70
        """
        
        df = session.sql(query).to_pandas()
        return df.iloc[0].to_dict() if len(df) > 0 else {}
    
    except Exception as e:
        st.error(f"Error calculating revenue at risk: {str(e)}")
        return {}


# ============================================================
# LCR (Liquidity Coverage Ratio) Data Loaders
# ============================================================

@st.cache_data(ttl=3600)
def load_lcr_current_status():
    """
    Load current LCR status
    
    Returns:
        pandas.DataFrame: Latest LCR calculation
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                AS_OF_DATE as REPORTING_DATE,
                LCR_RATIO,
                LCR_STATUS,
                SEVERITY,
                HQLA_TOTAL,
                OUTFLOW_TOTAL,
                LCR_BUFFER_CHF,
                LCR_BUFFER_PCT,
                L1_TOTAL,
                L2_CAPPED,
                L2A_TOTAL,
                L2B_TOTAL,
                CAP_APPLIED,
                DISCARDED_L2,
                OUTFLOW_RETAIL,
                OUTFLOW_CORP,
                OUTFLOW_FI,
                CALCULATION_TIMESTAMP
            FROM REP_AGG_001.REPP_AGG_DT_LCR_DAILY
            WHERE AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM REP_AGG_001.REPP_AGG_DT_LCR_DAILY)
            LIMIT 1
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading LCR status: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_lcr_trend(days=90):
    """
    Load LCR trend data
    
    Args:
        days (int): Number of days to load
    
    Returns:
        pandas.DataFrame: LCR trend data
    """
    try:
        session = get_snowflake_session()
        
        query = f"""
            SELECT 
                AS_OF_DATE,
                LCR_RATIO,
                LCR_7D_AVG,
                LCR_30D_AVG,
                LCR_90D_AVG,
                LCR_30D_VOLATILITY,
                LCR_30D_MIN,
                LCR_30D_MAX,
                LCR_DOD_CHANGE,
                LCR_STATUS,
                SEVERITY
            FROM REP_AGG_001.REPP_AGG_DT_LCR_TREND
            WHERE AS_OF_DATE >= DATEADD(day, -{days}, CURRENT_DATE())
            ORDER BY AS_OF_DATE
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading LCR trend: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_hqla_holdings_detail():
    """
    Load HQLA holdings detail aggregated by asset type
    
    Returns:
        pandas.DataFrame: HQLA holdings by asset type
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                ASSET_TYPE,
                REGULATORY_LEVEL,
                MAX(HAIRCUT_FACTOR) AS HAIRCUT_FACTOR,
                COUNT(*) AS HOLDING_COUNT,
                ROUND(SUM(MARKET_VALUE_CHF), 2) AS MARKET_VALUE_CHF,
                ROUND(SUM(WEIGHTED_VALUE_CHF), 2) AS WEIGHTED_VALUE_CHF,
                ROUND(AVG(MARKET_VALUE_CHF), 2) AS AVG_HOLDING_SIZE_CHF
            FROM REP_AGG_001.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL
            WHERE AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM REP_AGG_001.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL)
            GROUP BY ASSET_TYPE, REGULATORY_LEVEL
            ORDER BY SUM(MARKET_VALUE_CHF) DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading HQLA holdings: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_deposit_outflows_detail():
    """
    Load deposit outflows detail aggregated by deposit type
    
    Returns:
        pandas.DataFrame: Deposit outflows by type
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                DEPOSIT_TYPE,
                COUNTERPARTY_TYPE,
                MAX(BASE_RUN_OFF_RATE) AS BASE_RUN_OFF_RATE,
                COUNT(*) AS ACCOUNT_COUNT,
                COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMER_COUNT,
                ROUND(SUM(BALANCE_CHF), 2) AS TOTAL_BALANCE_CHF,
                ROUND(SUM(OUTFLOW_AMOUNT_CHF), 2) AS TOTAL_OUTFLOW_CHF,
                ROUND(AVG(BALANCE_CHF), 2) AS AVG_BALANCE_CHF,
                ROUND(AVG(FINAL_RUN_OFF_RATE) * 100, 2) AS AVG_ADJUSTED_RUN_OFF_RATE
            FROM REP_AGG_001.REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL
            WHERE AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM REP_AGG_001.REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL)
            GROUP BY DEPOSIT_TYPE, COUNTERPARTY_TYPE
            ORDER BY SUM(OUTFLOW_AMOUNT_CHF) DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading deposit outflows: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_lcr_alerts():
    """
    Load active LCR alerts
    
    Returns:
        pandas.DataFrame: Active alerts with flattened structure
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                AS_OF_DATE,
                LCR_RATIO,
                LCR_STATUS,
                SEVERITY,
                alert_item.value:severity::STRING AS ALERT_SEVERITY,
                alert_item.value:type::STRING AS ALERT_TYPE,
                alert_item.value:message::STRING AS ALERT_MESSAGE,
                alert_item.value:action::STRING AS RECOMMENDED_ACTION,
                ALERT_TIMESTAMP
            FROM REP_AGG_001.REPP_AGG_VW_LCR_ALERTS,
            LATERAL FLATTEN(input => ALL_ALERTS) alert_item
            WHERE TOTAL_ALERT_COUNT > 0
            ORDER BY 
                CASE alert_item.value:severity::STRING
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3
                    WHEN 'INFO' THEN 4
                    ELSE 5
                END,
                AS_OF_DATE DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading LCR alerts: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_lcr_monthly_summary():
    """
    Load monthly LCR summary for SNB reporting
    
    Returns:
        pandas.DataFrame: Monthly summary
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                REPORTING_MONTH AS REPORT_MONTH,
                TRADING_DAYS,
                LCR_AVG AS AVG_LCR_RATIO,
                LCR_MIN AS MIN_LCR_RATIO,
                LCR_MAX AS MAX_LCR_RATIO,
                LCR_VOLATILITY,
                AVG_HQLA_TOTAL,
                AVG_OUTFLOW_TOTAL,
                BREACH_DAYS AS DAYS_BELOW_100_PCT,
                WARNING_DAYS AS DAYS_BELOW_105_PCT,
                COMPLIANT_DAYS,
                BREACH_RATE_PCT,
                CASE 
                    WHEN BREACH_DAYS = 0 THEN 'PASS'
                    WHEN BREACH_DAYS <= 3 THEN 'WARNING'
                    ELSE 'FAIL'
                END AS COMPLIANCE_STATUS
            FROM REP_AGG_001.REPP_AGG_VW_LCR_MONTHLY_SUMMARY
            ORDER BY REPORTING_MONTH DESC
            LIMIT 12
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading monthly summary: {str(e)}")
        return pd.DataFrame()


# ============================================================
# LOAN PORTFOLIO DATA LOADERS
# ============================================================

@st.cache_data(ttl=3600)
def load_loan_portfolio_summary():
    """
    Load loan portfolio summary metrics
    
    Returns:
        pandas.DataFrame: Portfolio summary by country and product
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                COUNTRY,
                PRODUCT_TYPE,
                APPLICATION_STATUS,
                LOAN_COUNT,
                TOTAL_REQUESTED_AMOUNT,
                AVG_REQUESTED_AMOUNT,
                MIN_REQUESTED_AMOUNT,
                MAX_REQUESTED_AMOUNT,
                AVG_TERM_MONTHS
            FROM REP_AGG_001.LOAR_AGG_DT_PORTFOLIO_SUMMARY
            WHERE AS_OF_DATE = CURRENT_DATE()
            ORDER BY TOTAL_REQUESTED_AMOUNT DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading loan portfolio summary: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_loan_ltv_distribution():
    """
    Load LTV distribution for loan portfolio
    
    Returns:
        pandas.DataFrame: LTV distribution by bucket
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                LTV_BUCKET,
                LTV_BUCKET_SORT_ORDER,
                LOAN_COUNT,
                TOTAL_LOAN_AMOUNT,
                AVG_LTV_PCT,
                TOTAL_COLLATERAL_VALUE,
                PCT_OF_TOTAL_LOANS
            FROM REP_AGG_001.LOAR_AGG_DT_LTV_DISTRIBUTION
            WHERE AS_OF_DATE = CURRENT_DATE()
            ORDER BY LTV_BUCKET_SORT_ORDER
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading LTV distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_loan_application_funnel():
    """
    Load loan application funnel by status
    
    Returns:
        pandas.DataFrame: Application counts by status
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                PRODUCT_TYPE,
                COUNTRY,
                CHANNEL,
                TOTAL_APPLICATIONS,
                APPROVED_COUNT,
                DECLINED_COUNT,
                UNDER_REVIEW_COUNT,
                APPROVAL_RATE_PCT,
                DECLINE_RATE_PCT,
                AVG_REQUESTED_AMOUNT
            FROM REP_AGG_001.LOAR_AGG_DT_APPLICATION_FUNNEL
            WHERE AS_OF_DATE = CURRENT_DATE()
            ORDER BY TOTAL_APPLICATIONS DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading application funnel: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_loan_affordability_analysis():
    """
    Load affordability analysis for loan applications
    
    Returns:
        pandas.DataFrame: Affordability metrics by country
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                COUNTRY,
                AFFORDABILITY_RESULT,
                ASSESSMENT_COUNT,
                AVG_DTI_RATIO_PCT,
                AVG_DSTI_RATIO_PCT,
                AVG_GROSS_INCOME,
                AVG_DEBT_OBLIGATIONS,
                PASS_RATE_PCT
            FROM REP_AGG_001.LOAR_AGG_DT_AFFORDABILITY_SUMMARY
            WHERE AS_OF_DATE = CURRENT_DATE()
            ORDER BY COUNTRY, AFFORDABILITY_RESULT
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading affordability analysis: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_loan_compliance_screening():
    """
    Load compliance screening results for loan applications
    
    Returns:
        pandas.DataFrame: Applications with compliance flags
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                APPLICATION_ID,
                CUSTOMER_ID,
                FULL_NAME,
                COUNTRY,
                REQUESTED_AMOUNT,
                APPLICATION_STATUS,
                REQUIRES_SANCTIONS_REVIEW,
                REQUIRES_EXPOSED_PERSON_REVIEW,
                OVERALL_RISK_RATING,
                VULNERABLE_CUSTOMER_FLAG,
                COMPLIANCE_HOLD_FLAG,
                COMPLIANCE_STATUS,
                APPLICATION_DATE_TIME
            FROM REP_AGG_001.LOAR_AGG_VW_COMPLIANCE_SCREENING
            WHERE COMPLIANCE_HOLD_FLAG = TRUE
                OR VULNERABLE_CUSTOMER_FLAG = TRUE
                OR OVERALL_RISK_RATING IN ('CRITICAL', 'HIGH')
            ORDER BY 
                CASE COMPLIANCE_STATUS
                    WHEN 'SANCTIONS_REVIEW' THEN 1
                    WHEN 'PEP_REVIEW' THEN 2
                    WHEN 'HIGH_RISK_REVIEW' THEN 3
                    ELSE 4
                END,
                APPLICATION_DATE_TIME DESC
            LIMIT 100
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading compliance screening: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_loan_customer_summary():
    """
    Load customer-level loan summary
    
    Returns:
        pandas.DataFrame: Loan summary per customer
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                cls.CUSTOMER_ID,
                c.FULL_NAME as CUSTOMER_NAME,
                cls.TOTAL_APPLICATIONS,
                cls.APPROVED_APPLICATIONS,
                cls.DECLINED_APPLICATIONS,
                cls.TOTAL_APPROVED_AMOUNT,
                cls.AVG_REQUESTED_AMOUNT,
                cls.LATEST_APPLICATION_DATE,
                cls.LATEST_APPLICATION_STATUS,
                cls.AVG_LTV_PCT,
                cls.AFFORDABILITY_PASS_COUNT,
                cls.AFFORDABILITY_FAIL_COUNT,
                c.VULNERABLE_CUSTOMER_FLAG,
                c.REQUIRES_SANCTIONS_REVIEW,
                c.REQUIRES_EXPOSED_PERSON_REVIEW
            FROM REP_AGG_001.LOAR_AGG_DT_CUSTOMER_LOAN_SUMMARY cls
            LEFT JOIN CRM_AGG_V001.CRMA_AGG_VW_CUSTOMER_360_ENRICHED c ON cls.CUSTOMER_ID = c.CUSTOMER_ID
            WHERE cls.TOTAL_APPLICATIONS > 0
            ORDER BY cls.TOTAL_APPROVED_AMOUNT DESC
            LIMIT 100
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading customer loan summary: {str(e)}")
        return pd.DataFrame()

