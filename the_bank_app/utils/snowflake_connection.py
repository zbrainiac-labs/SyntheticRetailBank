"""
Snowflake Connection Management
Handles Snowflake session creation and connection management.

Uses the Snowflake connection defined in ~/.snowflake/connections.toml
(connection name configured in .streamlit/secrets.toml or defaults to DEMO_MDAEPPEN).
"""

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException


@st.cache_resource
def get_snowflake_session():
    """
    Create and cache Snowflake session using ~/.snowflake/connections.toml.

    Reads connection_name, warehouse, database, schema, and role from
    .streamlit/secrets.toml [snowflake] section. Falls back to sensible defaults.

    Returns:
        Session: Snowflake Snowpark session
    """
    try:
        # Read app-level overrides from secrets.toml (optional keys)
        sf_cfg = st.secrets.get("snowflake", {})
        connection_name = sf_cfg.get("connection_name", "DEMO_MDAEPPEN")

        connection_parameters = {
            "connection_name": connection_name,
            "warehouse": sf_cfg.get("warehouse", "MD_TEST_WH"),
            "database": sf_cfg.get("database", "AAA_DEV_SYNTHETIC_BANK"),
            "schema": sf_cfg.get("schema", "CRM_AGG_V001"),
            "role": sf_cfg.get("role", "ACCOUNTADMIN"),
        }

        session = Session.builder.configs(connection_parameters).create()
        return session

    except Exception as e:
        raise Exception(f"Failed to connect to Snowflake: {e}")


def test_connection():
    """
    Test Snowflake connection
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        session = get_snowflake_session()
        result = session.sql("SELECT CURRENT_VERSION()").collect()
        return len(result) > 0
    except Exception:
        return False


def execute_query(query: str):
    """
    Execute SQL query and return results as pandas DataFrame
    
    Args:
        query: SQL query string
        
    Returns:
        pandas.DataFrame: Query results
    """
    try:
        session = get_snowflake_session()
        df = session.sql(query).to_pandas()
        return df
    except SnowparkSQLException as e:
        raise Exception(f"SQL execution failed: {e}")
    except Exception as e:
        raise Exception(f"Query execution failed: {e}")

