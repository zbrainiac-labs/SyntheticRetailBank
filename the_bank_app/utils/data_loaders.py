"""
Data Loading Functions
Handles data loading from Snowflake with caching
"""

import streamlit as st
import pandas as pd
from .snowflake_connection import get_snowflake_session


@st.cache_data(ttl=14400)  # Cache for 4 hours (240 min)
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


def load_high_risk_customers():
    """Load high-risk customers requiring review (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty:
            return pd.DataFrame()
        cols = ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'ACCOUNT_TIER', 'OVERALL_RISK_RATING',
                'OVERALL_RISK_SCORE', 'EXPOSED_PERSON_MATCH_TYPE', 'EXPOSED_PERSON_MATCH_ACCURACY_PERCENT',
                'SANCTIONS_MATCH_TYPE', 'SANCTIONS_MATCH_ACCURACY_PERCENT',
                'REQUIRES_EXPOSED_PERSON_REVIEW', 'REQUIRES_SANCTIONS_REVIEW', 'HIGH_RISK_CUSTOMER']
        cols = [c for c in cols if c in df.columns]
        mask = pd.Series(False, index=df.index)
        if 'HIGH_RISK_CUSTOMER' in df.columns:
            mask |= df['HIGH_RISK_CUSTOMER'] == True
        if 'REQUIRES_EXPOSED_PERSON_REVIEW' in df.columns:
            mask |= df['REQUIRES_EXPOSED_PERSON_REVIEW'] == True
        if 'REQUIRES_SANCTIONS_REVIEW' in df.columns:
            mask |= df['REQUIRES_SANCTIONS_REVIEW'] == True
        result = df.loc[mask, cols].copy()
        if 'OVERALL_RISK_SCORE' in result.columns:
            result = result.sort_values('OVERALL_RISK_SCORE', ascending=False)
        return result
    except Exception as e:
        st.error(f"Error loading high-risk customers: {str(e)}")
        return pd.DataFrame()


def load_risk_distribution():
    """Load risk distribution summary (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty or 'OVERALL_RISK_RATING' not in df.columns:
            return pd.DataFrame()
        result = df.groupby('OVERALL_RISK_RATING').agg(
            CUSTOMER_COUNT=('OVERALL_RISK_RATING', 'size'),
            AVG_RISK_SCORE=('OVERALL_RISK_SCORE', 'mean')
        ).reset_index().sort_values('AVG_RISK_SCORE', ascending=False)
        return result
    except Exception as e:
        st.error(f"Error loading risk distribution: {str(e)}")
        return pd.DataFrame()


def load_pep_sanctions_summary():
    """Load PEP and sanctions screening summary (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()
        pep_df = df.groupby('EXPOSED_PERSON_MATCH_TYPE').agg(
            COUNT=('EXPOSED_PERSON_MATCH_TYPE', 'size'),
            AVG_ACCURACY=('EXPOSED_PERSON_MATCH_ACCURACY_PERCENT', 'mean'),
            REQUIRES_REVIEW=('REQUIRES_EXPOSED_PERSON_REVIEW', 'sum')
        ).reset_index().sort_values('COUNT', ascending=False)
        sanctions_df = df.groupby('SANCTIONS_MATCH_TYPE').agg(
            COUNT=('SANCTIONS_MATCH_TYPE', 'size'),
            AVG_ACCURACY=('SANCTIONS_MATCH_ACCURACY_PERCENT', 'mean'),
            REQUIRES_REVIEW=('REQUIRES_SANCTIONS_REVIEW', 'sum')
        ).reset_index().sort_values('COUNT', ascending=False)
        return pep_df, sanctions_df
    except Exception as e:
        st.error(f"Error loading PEP/Sanctions summary: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()


def load_account_tier_distribution():
    """Load account tier distribution (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty or 'ACCOUNT_TIER' not in df.columns:
            return pd.DataFrame()
        result = df.groupby('ACCOUNT_TIER').agg(**{
            'CUSTOMER_COUNT': ('ACCOUNT_TIER', 'size'),
            **{f'AVG_{c.split("_")[0]}' if c != 'TOTAL_ACCOUNTS' else 'AVG_TOTAL_ACCOUNTS': (c, 'mean')
               for c in ['TOTAL_ACCOUNTS', 'CHECKING_ACCOUNTS', 'SAVINGS_ACCOUNTS', 'BUSINESS_ACCOUNTS', 'INVESTMENT_ACCOUNTS']
               if c in df.columns}
        }).reset_index().sort_values('CUSTOMER_COUNT', ascending=False)
        return result
    except Exception as e:
        st.error(f"Error loading account tier distribution: {str(e)}")
        return pd.DataFrame()


def load_geographic_distribution():
    """Load geographic distribution of customers (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty or 'COUNTRY' not in df.columns:
            return pd.DataFrame()
        result = df.groupby('COUNTRY').agg(
            CUSTOMER_COUNT=('COUNTRY', 'size'),
            TIER_DIVERSITY=('ACCOUNT_TIER', 'nunique'),
            AVG_RISK_SCORE=('OVERALL_RISK_SCORE', 'mean')
        ).reset_index().sort_values('CUSTOMER_COUNT', ascending=False)
        return result
    except Exception as e:
        st.error(f"Error loading geographic distribution: {str(e)}")
        return pd.DataFrame()


# ============================================================
# AML & Transaction Monitoring Data Loaders
# ============================================================

@st.cache_data(ttl=14400)
def load_aml_alerts():
    """
    Load AML transaction monitoring alerts
    
    Returns:
        pandas.DataFrame: AML alert data
    """
    try:
        session = get_snowflake_session()
        
        # Try PAY_AGG_V001 schema first, then fallback to default schema
        queries = [
            "SELECT * FROM PAY_AGG_V001.PAYA_AGG_DT_TRANSACTION_ANOMALIES ORDER BY BOOKING_DATE DESC LIMIT 1000",
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
        st.warning("⚠️ PAYA_AGG_DT_TRANSACTION_ANOMALIES table not found or empty. PAY_AGG_V001 schema may not be deployed yet.")
        return pd.DataFrame()
    
    except Exception as e:
        st.error(f"Error loading AML alerts: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
def load_aml_metrics():
    """
    Load AML key metrics summary
    
    Returns:
        dict: Dictionary of AML metrics
    """
    try:
        session = get_snowflake_session()
        
        # Try PAY_AGG_V001 schema first, then fallback to default schema
        queries = [
            """
            SELECT 
                COUNT(*) as TOTAL_ALERTS,
                COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS,
                COUNT(*) as ANOMALOUS_TRANSACTIONS
            FROM PAY_AGG_V001.PAYA_AGG_DT_TRANSACTION_ANOMALIES
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

def load_lending_portfolio():
    """Load lending portfolio overview (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty:
            return pd.DataFrame()
        cols = ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'CREDIT_SCORE_BAND', 'RISK_CLASSIFICATION', 'ACCOUNT_TIER', 'TOTAL_ACCOUNTS']
        cols = [c for c in cols if c in df.columns]
        return df[cols].sort_values('CUSTOMER_ID') if cols else pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading lending portfolio: {str(e)}")
        return pd.DataFrame()


# ============================================================
# Wealth Management Data Loaders
# ============================================================

@st.cache_data(ttl=14400)
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


@st.cache_data(ttl=14400)
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

def load_sanctions_matches():
    """Load sanctions screening matches (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty or 'SANCTIONS_MATCH_TYPE' not in df.columns:
            return pd.DataFrame()
        cols = ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'SANCTIONS_MATCH_TYPE',
                'SANCTIONS_MATCH_ACCURACY_PERCENT', 'REQUIRES_SANCTIONS_REVIEW', 'OVERALL_SANCTIONS_RISK']
        cols = [c for c in cols if c in df.columns]
        result = df[df['SANCTIONS_MATCH_TYPE'] != 'NO_MATCH'][cols].copy()
        if 'SANCTIONS_MATCH_ACCURACY_PERCENT' in result.columns:
            result = result.sort_values('SANCTIONS_MATCH_ACCURACY_PERCENT', ascending=False)
        return result
    except Exception as e:
        st.error(f"Error loading sanctions matches: {str(e)}")
        return pd.DataFrame()


# ============================================================
# Employee/Advisor Management Data Loaders
# ============================================================

@st.cache_data(ttl=14400)
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


@st.cache_data(ttl=14400)
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

def load_pep_matches():
    """Load PEP matches (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty or 'EXPOSED_PERSON_MATCH_TYPE' not in df.columns:
            return pd.DataFrame()
        cols = ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'EXPOSED_PERSON_MATCH_TYPE',
                'EXPOSED_PERSON_MATCH_ACCURACY_PERCENT', 'REQUIRES_EXPOSED_PERSON_REVIEW', 'OVERALL_EXPOSED_PERSON_RISK']
        cols = [c for c in cols if c in df.columns]
        result = df[df['EXPOSED_PERSON_MATCH_TYPE'] != 'NO_MATCH'][cols].copy()
        if 'EXPOSED_PERSON_MATCH_ACCURACY_PERCENT' in result.columns:
            result = result.sort_values('EXPOSED_PERSON_MATCH_ACCURACY_PERCENT', ascending=False)
        return result
    except Exception as e:
        st.error(f"Error loading PEP matches: {str(e)}")
        return pd.DataFrame()


def load_kyc_completeness():
    """Load KYC completeness metrics (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty or 'COUNTRY' not in df.columns:
            return pd.DataFrame()
        result = df.groupby('COUNTRY').agg(
            TOTAL_CUSTOMERS=('COUNTRY', 'size'),
            EMAIL_COMPLETE=('EMAIL', lambda x: x.notna().sum()),
            PHONE_COMPLETE=('PHONE', lambda x: x.notna().sum()),
            EMPLOYER_COMPLETE=('EMPLOYER', lambda x: x.notna().sum()) if 'EMPLOYER' in df.columns else ('COUNTRY', 'size'),
        ).reset_index()
        result['EMAIL_PCT'] = (result['EMAIL_COMPLETE'] / result['TOTAL_CUSTOMERS'] * 100).round(2)
        result['PHONE_PCT'] = (result['PHONE_COMPLETE'] / result['TOTAL_CUSTOMERS'] * 100).round(2)
        return result.sort_values('TOTAL_CUSTOMERS', ascending=False)
    except Exception as e:
        st.error(f"Error loading KYC completeness: {str(e)}")
        return pd.DataFrame()


# ============================================================
# Data Quality & Controls Data Loaders
# ============================================================

def load_data_quality_metrics():
    """Load data quality assessment metrics (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty:
            return {}
        total = len(df)
        return {
            'TOTAL_RECORDS': total,
            'UNIQUE_CUSTOMERS': df['CUSTOMER_ID'].nunique() if 'CUSTOMER_ID' in df.columns else total,
            'MISSING_EMAIL': int(df['EMAIL'].isna().sum()) if 'EMAIL' in df.columns else 0,
            'MISSING_PHONE': int(df['PHONE'].isna().sum()) if 'PHONE' in df.columns else 0,
            'MISSING_DOB': int(df['DATE_OF_BIRTH'].isna().sum()) if 'DATE_OF_BIRTH' in df.columns else 0,
            'MISSING_ADDRESS': int(df['STREET_ADDRESS'].isna().sum()) if 'STREET_ADDRESS' in df.columns else 0,
            'EMAIL_COMPLETENESS': round(df['EMAIL'].notna().mean() * 100, 2) if 'EMAIL' in df.columns else 0,
            'PHONE_COMPLETENESS': round(df['PHONE'].notna().mean() * 100, 2) if 'PHONE' in df.columns else 0,
            'DOB_COMPLETENESS': round(df['DATE_OF_BIRTH'].notna().mean() * 100, 2) if 'DATE_OF_BIRTH' in df.columns else 0,
            'ADDRESS_COMPLETENESS': round(df['STREET_ADDRESS'].notna().mean() * 100, 2) if 'STREET_ADDRESS' in df.columns else 0,
        }
    except Exception as e:
        st.error(f"Error loading data quality metrics: {str(e)}")
        return {}


# ============================================================
# Compliance Risk Management Data Loaders
# ============================================================

def load_compliance_risk_summary():
    """Load overall compliance risk profile (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty:
            return {}
        return {
            'TOTAL_CUSTOMERS': len(df),
            'HIGH_RISK_COUNT': int(df['HIGH_RISK_CUSTOMER'].sum()) if 'HIGH_RISK_CUSTOMER' in df.columns else 0,
            'PEP_MATCHES': int((df['EXPOSED_PERSON_MATCH_TYPE'] != 'NO_MATCH').sum()) if 'EXPOSED_PERSON_MATCH_TYPE' in df.columns else 0,
            'SANCTIONS_MATCHES': int((df['SANCTIONS_MATCH_TYPE'] != 'NO_MATCH').sum()) if 'SANCTIONS_MATCH_TYPE' in df.columns else 0,
            'PEP_REVIEWS_NEEDED': int(df['REQUIRES_EXPOSED_PERSON_REVIEW'].sum()) if 'REQUIRES_EXPOSED_PERSON_REVIEW' in df.columns else 0,
            'SANCTIONS_REVIEWS_NEEDED': int(df['REQUIRES_SANCTIONS_REVIEW'].sum()) if 'REQUIRES_SANCTIONS_REVIEW' in df.columns else 0,
            'ANOMALY_COUNT': int(df['HAS_ANOMALY'].sum()) if 'HAS_ANOMALY' in df.columns else 0,
            'AVG_RISK_SCORE': round(df['OVERALL_RISK_SCORE'].mean(), 2) if 'OVERALL_RISK_SCORE' in df.columns else 0,
        }
    except Exception as e:
        st.error(f"Error loading compliance risk summary: {str(e)}")
        return {}


# ============================================================
# Churn & Lifecycle Management Data Loaders
# ============================================================

@st.cache_data(ttl=14400)
def load_customer_lifecycle():
    """
    Load customer lifecycle data
    
    Returns:
        pandas.DataFrame: Customer lifecycle data
    """
    try:
        session = get_snowflake_session()
        
        query = """
            SELECT 
                CUSTOMER_ID,
                LIFECYCLE_STAGE,
                CHURN_PROBABILITY,
                DAYS_SINCE_LAST_TRANSACTION,
                LAST_TRANSACTION_DATE,
                ACCOUNT_TIER,
                COUNTRY
            FROM CRMA_AGG_DT_CUSTOMER_360
            ORDER BY CUSTOMER_ID
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading lifecycle data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
            FROM CRMA_AGG_DT_CUSTOMER_360
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


def load_high_churn_risk_customers():
    """Load high churn risk customers >70% (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty or 'CHURN_PROBABILITY' not in df.columns:
            return pd.DataFrame()
        cols = ['CUSTOMER_ID', 'FIRST_NAME', 'FAMILY_NAME', 'LIFECYCLE_STAGE', 'CHURN_PROBABILITY',
                'DAYS_SINCE_LAST_TRANSACTION', 'LAST_TRANSACTION_DATE', 'ACCOUNT_TIER', 'COUNTRY', 'EMAIL', 'PHONE']
        cols = [c for c in cols if c in df.columns]
        return df[df['CHURN_PROBABILITY'] > 70][cols].sort_values('CHURN_PROBABILITY', ascending=False)
    except Exception as e:
        st.error(f"Error loading high churn risk customers: {str(e)}")
        return pd.DataFrame()


def load_premium_at_risk():
    """Load GOLD/PLATINUM customers at risk (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty or 'ACCOUNT_TIER' not in df.columns or 'CHURN_PROBABILITY' not in df.columns:
            return pd.DataFrame()
        cols = ['CUSTOMER_ID', 'FIRST_NAME', 'FAMILY_NAME', 'ACCOUNT_TIER', 'COUNTRY', 'LIFECYCLE_STAGE',
                'CHURN_PROBABILITY', 'DAYS_SINCE_LAST_TRANSACTION', 'LAST_TRANSACTION_DATE', 'EMAIL', 'PHONE', 'PREFERRED_CONTACT_METHOD']
        cols = [c for c in cols if c in df.columns]
        mask = df['ACCOUNT_TIER'].isin(['GOLD', 'PLATINUM']) & (df['CHURN_PROBABILITY'] > 70)
        return df.loc[mask, cols].sort_values('CHURN_PROBABILITY', ascending=False)
    except Exception as e:
        st.error(f"Error loading premium at risk: {str(e)}")
        return pd.DataFrame()


def load_dormant_accounts():
    """Load dormant accounts >180 days (from cached customer 360)"""
    try:
        df = load_customer_360()
        if df.empty or 'DAYS_SINCE_LAST_TRANSACTION' not in df.columns:
            return pd.DataFrame()
        cols = ['CUSTOMER_ID', 'FIRST_NAME', 'FAMILY_NAME', 'ACCOUNT_TIER', 'COUNTRY', 'LIFECYCLE_STAGE',
                'DAYS_SINCE_LAST_TRANSACTION', 'LAST_TRANSACTION_DATE', 'CHURN_PROBABILITY', 'EMAIL', 'PHONE', 'TOTAL_ACCOUNTS']
        cols = [c for c in cols if c in df.columns]
        mask = (df['DAYS_SINCE_LAST_TRANSACTION'] > 180)
        if 'LIFECYCLE_STAGE' in df.columns:
            mask &= df['LIFECYCLE_STAGE'].isin(['DORMANT', 'DECLINING'])
        return df.loc[mask, cols].sort_values('DAYS_SINCE_LAST_TRANSACTION', ascending=False)
    except Exception as e:
        st.error(f"Error loading dormant accounts: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
                AVG(CHURN_PROBABILITY) as AVG_CHURN_PROBABILITY,
                COALESCE(SUM(TOTAL_BALANCE), 0) / 1000 as TOTAL_REVENUE_AT_RISK
            FROM CRMA_AGG_DT_CUSTOMER_360
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

@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.REPP_AGG_DT_LCR_DAILY
            WHERE AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM REP_AGG_V001.REPP_AGG_DT_LCR_DAILY)
            LIMIT 1
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading LCR status: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.REPP_AGG_DT_LCR_TREND
            ORDER BY AS_OF_DATE
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading LCR trend: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL
            WHERE AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM REP_AGG_V001.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL)
            GROUP BY ASSET_TYPE, REGULATORY_LEVEL
            ORDER BY MARKET_VALUE_CHF DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading HQLA holdings: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL
            WHERE AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM REP_AGG_V001.REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL)
            GROUP BY DEPOSIT_TYPE, COUNTERPARTY_TYPE
            ORDER BY SUM(OUTFLOW_AMOUNT_CHF) DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading deposit outflows: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.REPP_AGG_VW_LCR_ALERTS,
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


@st.cache_data(ttl=14400)
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
                WARNING_DAYS AS DAYS_100_TO_110_PCT,
                COMPLIANT_DAYS,
                BREACH_RATE_PCT,
                CASE 
                    WHEN BREACH_DAYS = 0 THEN 'PASS'
                    WHEN BREACH_DAYS <= 3 THEN 'WARNING'
                    ELSE 'FAIL'
                END AS COMPLIANCE_STATUS
            FROM REP_AGG_V001.REPP_AGG_VW_LCR_MONTHLY_SUMMARY
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

@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.LOAR_AGG_DT_PORTFOLIO_SUMMARY
            WHERE AS_OF_DATE = CURRENT_DATE()
            ORDER BY TOTAL_REQUESTED_AMOUNT DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading loan portfolio summary: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.LOAR_AGG_DT_LTV_DISTRIBUTION
            WHERE AS_OF_DATE = CURRENT_DATE()
            ORDER BY LTV_BUCKET_SORT_ORDER
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading LTV distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.LOAR_AGG_DT_APPLICATION_FUNNEL
            WHERE AS_OF_DATE = CURRENT_DATE()
            ORDER BY TOTAL_APPLICATIONS DESC
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading application funnel: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.LOAR_AGG_DT_AFFORDABILITY_SUMMARY
            WHERE AS_OF_DATE = CURRENT_DATE()
            ORDER BY COUNTRY, AFFORDABILITY_RESULT
        """
        
        df = session.sql(query).to_pandas()
        return df
    
    except Exception as e:
        st.error(f"Error loading affordability analysis: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.LOAR_AGG_VW_COMPLIANCE_SCREENING
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


@st.cache_data(ttl=14400)
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
            FROM REP_AGG_V001.LOAR_AGG_DT_CUSTOMER_LOAN_SUMMARY cls
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



# ============================================================
# LCR engine loaders (scenarios, attribution, drill-down, simulator)
# All read the REP_AGG_V001 LCR engine objects; dates are bound parameters.
# ============================================================

def _lcr_query(query, params=None, error_label="LCR data"):
    """Run a parameterised LCR query and return a DataFrame (empty on error)."""
    try:
        session = get_snowflake_session()
        return session.sql(query, params=params or []).to_pandas()
    except Exception as e:
        st.error(f"Error loading {error_label}: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=14400)
def load_lcr_scenario_daily():
    """Daily LCR per scenario (BASE, BANK_STRESS, MARKET_STRESS, COMBINED)."""
    return _lcr_query("""
        SELECT AS_OF_DATE, STRESS_SCENARIO, SCENARIO_NAME, IS_REGULATORY, LCR_RATIO, LCR_STATUS,
               HQLA_TOTAL, L1_TOTAL, L2A_TOTAL, L2B_TOTAL, L2B_CAPPED, L2_UNCAPPED, L2_CAPPED,
               L2B_CAP_APPLIED, L2_CAP_APPLIED, OUTFLOW_RETAIL, OUTFLOW_CORP, OUTFLOW_FI,
               OUTFLOW_FACILITIES, OUTFLOW_TOTAL, INFLOW_NONFIN, INFLOW_FI, INFLOW_TOTAL,
               INFLOW_CAPPED, INFLOW_CAP_APPLIED, NET_OUTFLOW, LCR_BUFFER_CHF
        FROM REP_AGG_V001.REPP_AGG_DT_LCR_SCENARIO_DAILY
        ORDER BY AS_OF_DATE, STRESS_SCENARIO
    """, error_label="LCR scenarios")


@st.cache_data(ttl=14400)
def load_lcr_events():
    """Showcase liquidity events (latest load per event)."""
    return _lcr_query("""
        SELECT EVENT_ID, EVENT_DATE, EVENT_TYPE, DESCRIPTION, COUNTERPARTY_ID, CONTRACT_ID,
               AMOUNT_CHF, EXPECTED_DIRECTION
        FROM REP_RAW_V001.LIQI_RAW_TB_EVENTS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY INSERT_TIMESTAMP_UTC DESC) = 1
        ORDER BY EVENT_DATE
    """, error_label="LCR events")


@st.cache_data(ttl=14400)
def load_lcr_liquidity_gap(as_of_date):
    """Maturity ladder / liquidity gap for one date, all scenarios."""
    return _lcr_query("""
        SELECT STRESS_SCENARIO, BUCKET_CODE, BUCKET_NAME, SORT_ORDER, IN_LCR_HORIZON,
               CONTRACTUAL_INFLOW_CHF, CONTRACTUAL_OUTFLOW_CHF, BEHAVIORAL_OUTFLOW_CHF,
               COUNTERBALANCING_CHF, NET_GAP_CHF, CUMULATIVE_GAP_CHF
        FROM REP_AGG_V001.REPP_AGG_DT_LCR_LIQUIDITY_GAP
        WHERE AS_OF_DATE = ?
        ORDER BY STRESS_SCENARIO, SORT_ORDER
    """, [str(as_of_date)], "liquidity gap")


@st.cache_data(ttl=14400)
def load_lcr_cutoff_deltas(as_of_date, comparison):
    """Cut-off comparison (DOD/WOW/MOM) by asset class for one date, BASE scenario."""
    return _lcr_query("""
        SELECT ASSET_CLASS, PRIOR_DATE, HQLA_DELTA_CHF, OUTFLOW_DELTA_CHF, INFLOW_DELTA_CHF,
               LCR_RATIO, LCR_PRIOR, LCR_DELTA_PP
        FROM REP_AGG_V001.REPP_AGG_DT_LCR_CUTOFF_DELTAS
        WHERE AS_OF_DATE = ? AND COMPARISON_TYPE = ? AND STRESS_SCENARIO = 'BASE'
        ORDER BY ASSET_CLASS
    """, [str(as_of_date), comparison], "cut-off deltas")


@st.cache_data(ttl=14400)
def load_lcr_variance_drivers(as_of_date, comparison, scenario="BASE"):
    """Top-10 client variance drivers (funding-adjusted impact)."""
    return _lcr_query("""
        SELECT CLIENT_RANK, POSITION_ID, COUNTERPARTY_NAME, PRODUCT_TYPE, ASSET_CLASS, BUSINESS_UNIT,
               CLIENT_IMPACT_PP, CONTRIBUTION_PP, SETTLEMENT_IMPACT_PP, DELTA_OUTFLOW_CHF, LCR_DELTA_PP
        FROM REP_AGG_V001.REPP_AGG_VW_LCR_VARIANCE_DRIVERS
        WHERE AS_OF_DATE = ? AND COMPARISON_TYPE = ? AND STRESS_SCENARIO = ?
        ORDER BY CLIENT_RANK
    """, [str(as_of_date), comparison, scenario], "variance drivers")


@st.cache_data(ttl=14400)
def load_lcr_drilldown(as_of_date, scenario="BASE"):
    """Drill-down aggregated to counterparty level (asset class > product group > counterparty).
    Returns (DataFrame, query_seconds)."""
    import time
    t0 = time.perf_counter()
    df = _lcr_query("""
        SELECT ASSET_CLASS, PRODUCT_GROUP, COUNTERPARTY_NAME, LCR_TREATMENT, BUSINESS_UNIT, CURRENCY,
               COUNT(*) AS POSITIONS, SUM(EXPOSURE_CHF) AS EXPOSURE_CHF,
               SUM(WEIGHTED_CHF) AS WEIGHTED_CHF, SUM(LCR_IMPACT_CHF) AS LCR_IMPACT_CHF
        FROM REP_AGG_V001.REPP_AGG_VW_LCR_DRILLDOWN
        WHERE AS_OF_DATE = ? AND STRESS_SCENARIO = ?
        GROUP BY ALL
    """, [str(as_of_date), scenario], "drill-down")
    return df, time.perf_counter() - t0


@st.cache_data(ttl=14400)
def load_lcr_drilldown_positions(as_of_date, scenario, asset_class):
    """Contract-level rows for one asset class (top 500 by weighted amount)."""
    return _lcr_query("""
        SELECT POSITION_ID, POSITION_NAME, PRODUCT_TYPE, COUNTERPARTY_NAME, BUSINESS_UNIT, CURRENCY, RATING,
               EXPOSURE_CHF, WEIGHTED_CHF, LCR_IMPACT_CHF
        FROM REP_AGG_V001.REPP_AGG_VW_LCR_DRILLDOWN
        WHERE AS_OF_DATE = ? AND STRESS_SCENARIO = ? AND ASSET_CLASS = ?
        ORDER BY ABS(WEIGHTED_CHF) DESC
        LIMIT 500
    """, [str(as_of_date), scenario, asset_class], "drill-down positions")


@st.cache_data(ttl=14400)
def load_lcr_exceptions(as_of_date):
    """Quarantined data-quality exceptions for one snapshot date."""
    return _lcr_query("""
        SELECT CONTRACT_ID, SOURCE_SYSTEM, PRODUCT_TYPE, COUNTERPARTY_ID, CURRENCY, AMOUNT_CCY,
               ERROR_CODE, ERROR_DETAIL, LOGGED_AT
        FROM REP_AGG_V001.REPP_AGG_DT_LCR_EXCEPTIONS
        WHERE SNAPSHOT_DATE = ?
        ORDER BY ERROR_CODE, CONTRACT_ID
    """, [str(as_of_date)], "data-quality exceptions")


@st.cache_data(ttl=600)
def run_lcr_simulation(as_of_date, params_json):
    """Run the what-if simulator; cached 10 minutes per (date, params)."""
    return _lcr_query("CALL REP_AGG_V001.REPP_AGG_SP_LCR_SIMULATE(?, ?)",
                      [str(as_of_date), params_json], "LCR simulation")
