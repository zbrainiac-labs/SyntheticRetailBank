import streamlit as st
from utils.data_loaders import load_customer_360
from utils.visualizations import plot_account_tier_distribution, plot_geographic_distribution

st.header("Portfolio Analytics")

try:
    df_customers = load_customer_360()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Avg Accounts per Customer", f"{df_customers['TOTAL_ACCOUNTS'].mean():.1f}")
    with col2:
        multi_currency = len(df_customers[df_customers['CURRENCIES'].str.contains(',', na=False)])
        st.metric("Multi-Currency Customers", multi_currency)
    with col3:
        premium = len(df_customers[df_customers['ACCOUNT_TIER'].isin(['PREMIUM', 'PLATINUM'])])
        st.metric("Premium/Platinum", premium)
    with col4:
        investment = len(df_customers[df_customers['INVESTMENT_ACCOUNTS'] > 0])
        st.metric("Investment Holders", investment)

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Account Tier Distribution")
        fig_tier = plot_account_tier_distribution(df_customers)
        st.plotly_chart(fig_tier, width="stretch")
    with col2:
        st.subheader("Geographic Distribution")
        fig_geo = plot_geographic_distribution(df_customers)
        st.plotly_chart(fig_geo, width="stretch")

    st.divider()
    st.subheader("Account Type Holdings by Tier")
    account_summary = df_customers.groupby('ACCOUNT_TIER').agg({
        'TOTAL_ACCOUNTS': 'mean',
        'CHECKING_ACCOUNTS': 'mean',
        'SAVINGS_ACCOUNTS': 'mean',
        'BUSINESS_ACCOUNTS': 'mean',
        'INVESTMENT_ACCOUNTS': 'mean'
    }).round(1)
    st.dataframe(account_summary, width="stretch")

except Exception as e:
    st.error(f"Error loading portfolio data: {str(e)}")
