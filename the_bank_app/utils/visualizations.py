"""
Visualization Functions
Reusable chart and graph functions using Plotly
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


# Color palette for professional banking UI
COLORS = {
    'CRITICAL': '#FF3B30',   # Crimson
    'HIGH': '#FF9F0A',       # Dark Orange
    'MEDIUM': '#FFD700',     # Gold
    'LOW': '#34C759',        # Lime Green
    'NO_RISK': '#1E90FF',    # Dodger Blue
    'primary': '#007AFF',    # Dark Blue
    'secondary': '#007AFF'   # Medium Blue
}


def plot_risk_distribution(df):
    """
    Create pie chart for risk distribution
    
    Args:
        df: DataFrame with OVERALL_RISK_RATING column
        
    Returns:
        plotly.graph_objects.Figure
    """
    risk_counts = df['OVERALL_RISK_RATING'].value_counts()
    
    fig = px.pie(
        values=risk_counts.values,
        names=risk_counts.index,
        title="Customer Risk Distribution (All)",
        color=risk_counts.index,
        color_discrete_map=COLORS,
        hole=0.3  # Donut chart
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=True,
        height=400,
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.05
        )
    )
    
    return fig


def plot_risk_distribution_excluding_no_risk(df):
    """
    Create pie chart for risk distribution excluding NO_RISK customers
    Focus on actual risk customers only
    
    Args:
        df: DataFrame with OVERALL_RISK_RATING column
        
    Returns:
        plotly.graph_objects.Figure
    """
    # Filter out NO_RISK customers
    df_risk = df[df['OVERALL_RISK_RATING'] != 'NO_RISK'].copy()
    
    if len(df_risk) == 0:
        # Return empty figure if no risk customers
        fig = go.Figure()
        fig.add_annotation(
            text="No customers with risk ratings",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14)
        )
        fig.update_layout(height=400, title="Risk Customers Distribution (Excluding NO_RISK)")
        return fig
    
    risk_counts = df_risk['OVERALL_RISK_RATING'].value_counts()
    
    fig = px.pie(
        values=risk_counts.values,
        names=risk_counts.index,
        title="Risk Customers Distribution (Excluding NO_RISK)",
        color=risk_counts.index,
        color_discrete_map=COLORS,
        hole=0.4  # Donut chart
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=True,
        height=400,
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.05
        )
    )
    
    return fig


def plot_account_tier_distribution(df):
    """
    Create bar chart for account tier distribution
    
    Args:
        df: DataFrame with ACCOUNT_TIER column
        
    Returns:
        plotly.graph_objects.Figure
    """
    tier_counts = df['ACCOUNT_TIER'].value_counts().sort_values(ascending=True)
    
    fig = px.bar(
        x=tier_counts.values,
        y=tier_counts.index,
        orientation='h',
        title="Customers by Account Tier",
        labels={'x': 'Number of Customers', 'y': 'Account Tier'},
        color=tier_counts.values,
        color_continuous_scale='Blues'
    )
    
    fig.update_traces(
        hovertemplate='<b>%{y}</b><br>Customers: %{x}<extra></extra>',
        text=tier_counts.values,
        textposition='auto'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Number of Customers",
        yaxis_title="Account Tier"
    )
    
    return fig


def plot_geographic_distribution(df):
    """
    Create bar chart for geographic distribution
    
    Args:
        df: DataFrame with COUNTRY column
        
    Returns:
        plotly.graph_objects.Figure
    """
    country_counts = df['COUNTRY'].value_counts().sort_values(ascending=True)
    
    fig = px.bar(
        x=country_counts.values,
        y=country_counts.index,
        orientation='h',
        title="Customers by Country",
        labels={'x': 'Number of Customers', 'y': 'Country'},
        color=country_counts.values,
        color_continuous_scale='Teal'
    )
    
    fig.update_traces(
        hovertemplate='<b>%{y}</b><br>Customers: %{x}<extra></extra>',
        text=country_counts.values,
        textposition='auto'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Number of Customers",
        yaxis_title="Country"
    )
    
    return fig


def plot_risk_score_histogram(df):
    """
    Create histogram for risk score distribution
    
    Args:
        df: DataFrame with OVERALL_RISK_SCORE column
        
    Returns:
        plotly.graph_objects.Figure
    """
    fig = px.histogram(
        df,
        x='OVERALL_RISK_SCORE',
        nbins=20,
        title="Risk Score Distribution",
        labels={'OVERALL_RISK_SCORE': 'Risk Score', 'count': 'Number of Customers'},
        color_discrete_sequence=[COLORS['primary']]
    )
    
    fig.update_traces(
        hovertemplate='Risk Score: %{x}<br>Customers: %{y}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Risk Score",
        yaxis_title="Number of Customers"
    )
    
    return fig


def plot_pep_sanctions_summary(pep_counts, sanctions_counts):
    """
    Create grouped bar chart for PEP and Sanctions screening
    
    Args:
        pep_counts: Series with PEP match type counts
        sanctions_counts: Series with Sanctions match type counts
        
    Returns:
        plotly.graph_objects.Figure
    """
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='PEP Screening',
        x=pep_counts.index,
        y=pep_counts.values,
        marker_color=COLORS['secondary']
    ))
    
    fig.add_trace(go.Bar(
        name='Sanctions Screening',
        x=sanctions_counts.index,
        y=sanctions_counts.values,
        marker_color=COLORS['primary']
    ))
    
    fig.update_layout(
        title="PEP & Sanctions Screening Results",
        xaxis_title="Match Type",
        yaxis_title="Number of Customers",
        barmode='group',
        height=400,
        hovermode='x unified'
    )
    
    return fig


def plot_account_holdings_by_tier(df):
    """
    Create grouped bar chart for account holdings by tier
    
    Args:
        df: DataFrame with account tier and account type columns
        
    Returns:
        plotly.graph_objects.Figure
    """
    account_summary = df.groupby('ACCOUNT_TIER').agg({
        'CHECKING_ACCOUNTS': 'mean',
        'SAVINGS_ACCOUNTS': 'mean',
        'BUSINESS_ACCOUNTS': 'mean',
        'INVESTMENT_ACCOUNTS': 'mean'
    }).round(1)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Checking',
        x=account_summary.index,
        y=account_summary['CHECKING_ACCOUNTS'],
        marker_color='#1f77b4'
    ))
    
    fig.add_trace(go.Bar(
        name='Savings',
        x=account_summary.index,
        y=account_summary['SAVINGS_ACCOUNTS'],
        marker_color='#FF9F0A'
    ))
    
    fig.add_trace(go.Bar(
        name='Business',
        x=account_summary.index,
        y=account_summary['BUSINESS_ACCOUNTS'],
        marker_color='#2ca02c'
    ))
    
    fig.add_trace(go.Bar(
        name='Investment',
        x=account_summary.index,
        y=account_summary['INVESTMENT_ACCOUNTS'],
        marker_color='#FF3B30'
    ))
    
    fig.update_layout(
        title="Average Account Holdings by Tier",
        xaxis_title="Account Tier",
        yaxis_title="Average Number of Accounts",
        barmode='group',
        height=400,
        hovermode='x unified'
    )
    
    return fig


# ============================================================
# New Visualization Functions for Additional Dashboards
# ============================================================

def plot_aml_alert_trend(df):
    """
    Create line chart for AML alert trends over time
    
    Args:
        df: DataFrame with booking date and anomaly flags
        
    Returns:
        plotly.graph_objects.Figure
    """
    # Check if DataFrame is empty
    if df is None or len(df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="No AML alert data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color="gray")
        )
        fig.update_layout(height=400, title="AML Alert Trend (Last 90 Days)")
        return fig
    
    if 'BOOKING_DATE' not in df.columns:
        fig = go.Figure()
        fig.add_annotation(
            text="BOOKING_DATE column not found",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color="gray")
        )
        fig.update_layout(height=400, title="AML Alert Trend (Last 90 Days)")
        return fig
    
    # Convert to datetime and handle errors
    try:
        df['BOOKING_DATE'] = pd.to_datetime(df['BOOKING_DATE'], errors='coerce')
        # Remove rows with invalid dates
        df = df.dropna(subset=['BOOKING_DATE'])
        
        if len(df) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="No valid dates in alert data",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=14, color="gray")
            )
            fig.update_layout(height=400, title="AML Alert Trend (Last 90 Days)")
            return fig
    except Exception as e:
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error processing dates: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color="gray")
        )
        fig.update_layout(height=400, title="AML Alert Trend (Last 90 Days)")
        return fig
    
    # Group by date
    daily_alerts = df.groupby(df['BOOKING_DATE'].dt.date).size().reset_index()
    daily_alerts.columns = ['Date', 'Alert_Count']
    
    if len(daily_alerts) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="No alerts to display",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color="gray")
        )
        fig.update_layout(height=400, title="AML Alert Trend (Last 90 Days)")
        return fig
    
    fig = px.line(
        daily_alerts,
        x='Date',
        y='Alert_Count',
        title="AML Alert Trend (Last 90 Days)",
        labels={'Alert_Count': 'Number of Alerts', 'Date': 'Date'},
        color_discrete_sequence=[COLORS['primary']]
    )
    
    fig.update_layout(
        showlegend=False,
        height=400,
        hovermode='x unified'
    )
    
    return fig


def plot_credit_risk_distribution(df):
    """
    Create pie chart for credit risk distribution
    
    Args:
        df: DataFrame with CREDIT_SCORE_BAND column
        
    Returns:
        plotly.graph_objects.Figure
    """
    if 'CREDIT_SCORE_BAND' not in df.columns:
        return go.Figure()
    
    credit_counts = df['CREDIT_SCORE_BAND'].value_counts()
    
    fig = px.pie(
        values=credit_counts.values,
        names=credit_counts.index,
        title="Credit Risk Distribution",
        hole=0.3
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label'
    )
    
    fig.update_layout(
        showlegend=True,
        height=400
    )
    
    return fig


def plot_advisor_aum_distribution(df):
    """
    Create bar chart for advisor AUM distribution
    
    Args:
        df: DataFrame with advisor performance data
        
    Returns:
        plotly.graph_objects.Figure
    """
    # Check for correct column names (TOTAL_PORTFOLIO_VALUE is the actual column)
    if 'ADVISOR_NAME' not in df.columns:
        return go.Figure()
    
    # Use TOTAL_PORTFOLIO_VALUE (actual column name) or TOTAL_AUM (legacy)
    aum_col = 'TOTAL_PORTFOLIO_VALUE' if 'TOTAL_PORTFOLIO_VALUE' in df.columns else 'TOTAL_AUM'
    if aum_col not in df.columns:
        return go.Figure()
    
    df_sorted = df.sort_values(aum_col, ascending=True).tail(20)
    
    fig = px.bar(
        df_sorted,
        x=aum_col,
        y='ADVISOR_NAME',
        orientation='h',
        title="Top 20 Advisors by AUM",
        labels={aum_col: 'Assets Under Management', 'ADVISOR_NAME': 'Advisor'},
        color=aum_col,
        color_continuous_scale='Greens'
    )
    
    fig.update_layout(
        showlegend=False,
        height=500,
        xaxis_title="AUM (CHF)",
        yaxis_title="Advisor"
    )
    
    return fig


def plot_advisor_capacity(df):
    """
    Create scatter plot for advisor capacity vs AUM
    
    Args:
        df: DataFrame with advisor capacity data
        
    Returns:
        plotly.graph_objects.Figure
    """
    # Use correct column names (TOTAL_CLIENTS, TOTAL_PORTFOLIO_VALUE, WORKLOAD_STATUS)
    client_col = 'TOTAL_CLIENTS' if 'TOTAL_CLIENTS' in df.columns else 'CLIENT_COUNT'
    aum_col = 'TOTAL_PORTFOLIO_VALUE' if 'TOTAL_PORTFOLIO_VALUE' in df.columns else 'TOTAL_AUM'
    status_col = 'WORKLOAD_STATUS' if 'WORKLOAD_STATUS' in df.columns else 'CAPACITY_STATUS'
    
    if client_col not in df.columns or aum_col not in df.columns:
        return go.Figure()
    
    # Create a copy to avoid modifying original data
    df_plot = df.copy()
    
    # Use absolute value for size parameter (negative values not allowed)
    # Add a new column for sizing
    df_plot['AUM_SIZE'] = df_plot[aum_col].abs()
    
    fig = px.scatter(
        df_plot,
        x=client_col,
        y=aum_col,
        size='AUM_SIZE',
        color=status_col if status_col in df.columns else None,
        hover_data=['ADVISOR_NAME'] if 'ADVISOR_NAME' in df.columns else None,
        title="Advisor Capacity Analysis",
        labels={
            client_col: 'Number of Clients', 
            aum_col: 'Assets Under Management',
            status_col: 'Workload Status'
        }
    )
    
    fig.update_layout(
        height=400,
        hovermode='closest'
    )
    
    return fig


def plot_sanctions_screening_results(df):
    """
    Create bar chart for sanctions screening results
    
    Args:
        df: DataFrame with sanctions match data
        
    Returns:
        plotly.graph_objects.Figure
    """
    if 'SANCTIONS_MATCH_TYPE' not in df.columns:
        return go.Figure()
    
    match_counts = df['SANCTIONS_MATCH_TYPE'].value_counts()
    
    fig = px.bar(
        x=match_counts.values,
        y=match_counts.index,
        orientation='h',
        title="Sanctions Screening Results",
        labels={'x': 'Number of Matches', 'y': 'Match Type'},
        color=match_counts.values,
        color_continuous_scale='Reds'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400
    )
    
    return fig


def plot_pep_screening_results(df):
    """
    Create bar chart for PEP screening results
    
    Args:
        df: DataFrame with PEP match data
        
    Returns:
        plotly.graph_objects.Figure
    """
    if 'EXPOSED_PERSON_MATCH_TYPE' not in df.columns:
        return go.Figure()
    
    match_counts = df['EXPOSED_PERSON_MATCH_TYPE'].value_counts()
    
    fig = px.bar(
        x=match_counts.values,
        y=match_counts.index,
        orientation='h',
        title="PEP Screening Results",
        labels={'x': 'Number of Matches', 'y': 'Match Type'},
        color=match_counts.values,
        color_continuous_scale='Oranges'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400
    )
    
    return fig


def plot_data_quality_completeness(metrics):
    """
    Create gauge charts for data quality completeness
    
    Args:
        metrics: Dictionary with data quality metrics
        
    Returns:
        plotly.graph_objects.Figure
    """
    fig = go.Figure()
    
    completeness_fields = [
        ('EMAIL_COMPLETENESS', 'Email'),
        ('PHONE_COMPLETENESS', 'Phone'),
        ('DOB_COMPLETENESS', 'Date of Birth')
    ]
    
    for i, (field, label) in enumerate(completeness_fields):
        if field in metrics:
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=metrics[field],
                title={'text': label},
                domain={'row': 0, 'column': i},
                gauge={
                    'axis': {'range': [0, 100]},
                    'bar': {'color': COLORS['primary']},
                    'steps': [
                        {'range': [0, 60], 'color': '#FFE5E5'},
                        {'range': [60, 80], 'color': '#FFF5E5'},
                        {'range': [80, 100], 'color': '#E5F5E5'}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))
    
    fig.update_layout(
        grid={'rows': 1, 'columns': 3, 'pattern': "independent"},
        title="Data Quality Completeness",
        height=300
    )
    
    return fig


def plot_compliance_risk_heatmap(df):
    """
    Create heatmap for compliance risk by country
    
    Args:
        df: DataFrame with country and risk metrics
        
    Returns:
        plotly.graph_objects.Figure
    """
    if 'COUNTRY' not in df.columns:
        return go.Figure()
    
    risk_by_country = df.groupby('COUNTRY').agg({
        'OVERALL_RISK_SCORE': 'mean',
        'CUSTOMER_ID': 'count'
    }).reset_index()
    risk_by_country.columns = ['Country', 'Avg_Risk_Score', 'Customer_Count']
    risk_by_country = risk_by_country.sort_values('Avg_Risk_Score', ascending=False).head(20)
    
    fig = px.bar(
        risk_by_country,
        x='Avg_Risk_Score',
        y='Country',
        orientation='h',
        title="Average Risk Score by Country (Top 20)",
        labels={'Avg_Risk_Score': 'Average Risk Score', 'Country': 'Country'},
        color='Avg_Risk_Score',
        color_continuous_scale='RdYlGn_r'
    )
    
    fig.update_layout(
        showlegend=False,
        height=500
    )
    
    return fig


# ============================================================
# Churn & Lifecycle Visualization Functions
# ============================================================

def plot_lifecycle_stage_distribution(df):
    """
    Create pie chart for lifecycle stage distribution
    
    Args:
        df: DataFrame with LIFECYCLE_STAGE column
        
    Returns:
        plotly.graph_objects.Figure
    """
    if 'LIFECYCLE_STAGE' not in df.columns or 'CUSTOMER_COUNT' not in df.columns:
        return go.Figure()
    
    # Define colors for lifecycle stages
    lifecycle_colors = {
        'NEW': '#34C759',        # Green
        'ACTIVE': '#007AFF',     # Blue
        'MATURE': '#FF9F0A',     # Orange
        'DECLINING': '#FFCC00',  # Amber
        'DORMANT': '#8E8E93',    # Grey
        'CHURNED': '#FF3B30'     # Red
    }
    
    colors = [lifecycle_colors.get(stage, '#C7C7CC') for stage in df['LIFECYCLE_STAGE']]
    
    fig = px.pie(
        df,
        values='CUSTOMER_COUNT',
        names='LIFECYCLE_STAGE',
        title="Customer Lifecycle Stage Distribution",
        hole=0.4,
        color='LIFECYCLE_STAGE',
        color_discrete_map=lifecycle_colors
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Customers: %{value}<br>Percentage: %{percent}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=True,
        height=450,
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.05
        )
    )
    
    return fig


def plot_churn_probability_distribution(df):
    """
    Create histogram for churn probability distribution
    
    Args:
        df: DataFrame with CHURN_PROBABILITY column
        
    Returns:
        plotly.graph_objects.Figure
    """
    if 'CHURN_PROBABILITY' not in df.columns:
        return go.Figure()
    
    fig = px.histogram(
        df,
        x='CHURN_PROBABILITY',
        nbins=20,
        title="Churn Probability Distribution",
        labels={'CHURN_PROBABILITY': 'Churn Probability (%)', 'count': 'Number of Customers'},
        color_discrete_sequence=['#FF6B6B']
    )
    
    # Add vertical lines for risk thresholds
    fig.add_vline(x=70, line_dash="dash", line_color="orange", annotation_text="High Risk (70%)")
    fig.add_vline(x=90, line_dash="dash", line_color="red", annotation_text="Critical Risk (90%)")
    
    fig.update_traces(
        hovertemplate='Churn Probability: %{x}%<br>Customers: %{y}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Churn Probability (%)",
        yaxis_title="Number of Customers"
    )
    
    return fig


def plot_lifecycle_revenue(df):
    """
    Create bar chart for revenue by lifecycle stage
    
    Args:
        df: DataFrame with lifecycle stage and revenue data
        
    Returns:
        plotly.graph_objects.Figure
    """
    if 'LIFECYCLE_STAGE' not in df.columns or 'TOTAL_REVENUE' not in df.columns:
        return go.Figure()
    
    lifecycle_colors = {
        'NEW': '#34C759',
        'ACTIVE': '#007AFF',
        'MATURE': '#FF9F0A',
        'DECLINING': '#FFCC00',
        'DORMANT': '#8E8E93',
        'CHURNED': '#FF3B30'
    }
    
    colors = [lifecycle_colors.get(stage, '#C7C7CC') for stage in df['LIFECYCLE_STAGE']]
    
    fig = go.Figure(data=[
        go.Bar(
            x=df['LIFECYCLE_STAGE'],
            y=df['TOTAL_REVENUE'],
            marker_color=colors,
            text=df['TOTAL_REVENUE'].apply(lambda x: f'CHF {x:,.0f}K'),
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Total Revenue: CHF %{y:,.0f}K<extra></extra>'
        )
    ])
    
    fig.update_layout(
        title="Total Revenue by Lifecycle Stage",
        xaxis_title="Lifecycle Stage",
        yaxis_title="Total Annual Revenue (CHF K)",
        showlegend=False,
        height=400
    )
    
    return fig


def plot_churn_risk_by_tier(df):
    """
    Create grouped bar chart for churn risk by account tier
    
    Args:
        df: DataFrame with ACCOUNT_TIER and CHURN_PROBABILITY
        
    Returns:
        plotly.graph_objects.Figure
    """
    if 'ACCOUNT_TIER' not in df.columns or 'CHURN_PROBABILITY' not in df.columns:
        return go.Figure()
    
    # Calculate average churn probability by tier
    churn_by_tier = df.groupby('ACCOUNT_TIER').agg({
        'CHURN_PROBABILITY': 'mean',
        'CUSTOMER_ID': 'count'
    }).reset_index()
    churn_by_tier.columns = ['Account_Tier', 'Avg_Churn_Probability', 'Customer_Count']
    churn_by_tier = churn_by_tier.sort_values('Avg_Churn_Probability', ascending=False)
    
    fig = px.bar(
        churn_by_tier,
        x='Account_Tier',
        y='Avg_Churn_Probability',
        title="Average Churn Probability by Account Tier",
        labels={'Avg_Churn_Probability': 'Average Churn Probability (%)', 'Account_Tier': 'Account Tier'},
        color='Avg_Churn_Probability',
        color_continuous_scale='Reds',
        text='Avg_Churn_Probability'
    )
    
    fig.update_traces(
        texttemplate='%{text:.1f}%',
        textposition='outside',
        hovertemplate='<b>%{x}</b><br>Avg Churn Probability: %{y:.1f}%<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Account Tier",
        yaxis_title="Average Churn Probability (%)"
    )
    
    return fig


def plot_revenue_at_risk_gauge(metrics):
    """
    Create gauge chart for revenue at risk
    
    Args:
        metrics: Dictionary with revenue at risk metrics
        
    Returns:
        plotly.graph_objects.Figure
    """
    total_revenue_at_risk = metrics.get('TOTAL_REVENUE_AT_RISK', 0)
    
    # Assume total potential revenue is higher (for gauge max)
    max_revenue = total_revenue_at_risk * 2 if total_revenue_at_risk > 0 else 1000000
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=total_revenue_at_risk,
        title={'text': "Total Revenue at Risk (CHF K)"},
        delta={'reference': max_revenue * 0.1, 'increasing': {'color': "red"}},
        gauge={
            'axis': {'range': [0, max_revenue]},
            'bar': {'color': "darkred"},
            'steps': [
                {'range': [0, max_revenue * 0.3], 'color': "#E8F5E9"},
                {'range': [max_revenue * 0.3, max_revenue * 0.6], 'color': "#FFF9C4"},
                {'range': [max_revenue * 0.6, max_revenue], 'color': "#FFEBEE"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_revenue * 0.7
            }
        }
    ))
    
    fig.update_layout(
        height=300
    )
    
    return fig


def plot_days_inactive_distribution(df):
    """
    Create histogram for days since last transaction
    
    Args:
        df: DataFrame with DAYS_SINCE_LAST_TRANSACTION column
        
    Returns:
        plotly.graph_objects.Figure
    """
    if 'DAYS_SINCE_LAST_TRANSACTION' not in df.columns:
        return go.Figure()
    
    fig = px.histogram(
        df,
        x='DAYS_SINCE_LAST_TRANSACTION',
        nbins=30,
        title="Distribution of Days Since Last Transaction",
        labels={'DAYS_SINCE_LAST_TRANSACTION': 'Days Inactive', 'count': 'Number of Customers'},
        color_discrete_sequence=['#8E8E93']
    )
    
    # Add vertical line for dormant threshold (180 days)
    fig.add_vline(x=180, line_dash="dash", line_color="red", annotation_text="Dormant (180 days)")
    
    fig.update_traces(
        hovertemplate='Days Inactive: %{x}<br>Customers: %{y}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Days Since Last Transaction",
        yaxis_title="Number of Customers"
    )
    
    return fig


# ============================================================
# LCR Visualization Functions
# ============================================================

def plot_lcr_trend(df):
    """
    Create line chart for LCR trend with moving averages
    
    Args:
        df: DataFrame with AS_OF_DATE, LCR_RATIO, and moving averages
        
    Returns:
        plotly.graph_objects.Figure
    """
    fig = go.Figure()
    
    # Add LCR ratio line
    fig.add_trace(go.Scatter(
        x=df['AS_OF_DATE'],
        y=df['LCR_RATIO'],
        mode='lines+markers',
        name='LCR Ratio',
        line=dict(color='#007AFF', width=3),
        marker=dict(size=6)
    ))
    
    # Add 7-day average
    if 'LCR_7D_AVG' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['AS_OF_DATE'],
            y=df['LCR_7D_AVG'],
            mode='lines',
            name='7-Day Average',
            line=dict(color='#007AFF', width=2, dash='dot')
        ))
    
    # Add 30-day average
    if 'LCR_30D_AVG' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['AS_OF_DATE'],
            y=df['LCR_30D_AVG'],
            mode='lines',
            name='30-Day Average',
            line=dict(color='#34C759', width=2, dash='dash')
        ))
    
    # Add regulatory threshold line at 100%
    fig.add_hline(
        y=100,
        line_dash="solid",
        line_color="red",
        annotation_text="Regulatory Minimum (100%)",
        annotation_position="right"
    )
    
    # Add warning threshold line at 105%
    fig.add_hline(
        y=105,
        line_dash="dot",
        line_color="orange",
        annotation_text="Warning Threshold (105%)",
        annotation_position="right"
    )
    
    fig.update_layout(
        title="LCR Ratio Trend (90 Days)",
        height=500,
        xaxis_title="Date",
        yaxis_title="LCR Ratio (%)",
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig


def plot_hqla_composition(df):
    """
    Create stacked bar chart for HQLA composition by regulatory level
    
    Args:
        df: DataFrame with REGULATORY_LEVEL and WEIGHTED_VALUE_CHF
        
    Returns:
        plotly.graph_objects.Figure
    """
    # Aggregate by level
    level_totals = df.groupby('REGULATORY_LEVEL')['WEIGHTED_VALUE_CHF'].sum().reset_index()
    
    color_map = {
        'L1': '#34C759',   # Green
        'L2A': '#FFCC00',  # Yellow
        'L2B': '#FF9F0A'   # Orange
    }
    
    fig = px.pie(
        level_totals,
        values='WEIGHTED_VALUE_CHF',
        names='REGULATORY_LEVEL',
        title="HQLA Composition by Regulatory Level",
        color='REGULATORY_LEVEL',
        color_discrete_map=color_map,
        hole=0.4
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Value: CHF %{value:,.0f}<br>Percentage: %{percent}<extra></extra>'
    )
    
    fig.update_layout(
        height=400,
        showlegend=True
    )
    
    return fig


def plot_hqla_by_asset_type(df):
    """
    Create horizontal bar chart for HQLA by asset type
    
    Args:
        df: DataFrame with ASSET_TYPE and WEIGHTED_VALUE_CHF
        
    Returns:
        plotly.graph_objects.Figure
    """
    # Sort by value
    df_sorted = df.sort_values('WEIGHTED_VALUE_CHF', ascending=True)
    
    fig = px.bar(
        df_sorted,
        x='WEIGHTED_VALUE_CHF',
        y='ASSET_TYPE',
        orientation='h',
        title="HQLA Holdings by Asset Type",
        labels={'WEIGHTED_VALUE_CHF': 'Weighted Value (CHF)', 'ASSET_TYPE': 'Asset Type'},
        color='REGULATORY_LEVEL',
        color_discrete_map={
            'L1': '#34C759',
            'L2A': '#FFCC00',
            'L2B': '#FF9F0A'
        }
    )
    
    fig.update_traces(
        hovertemplate='<b>%{y}</b><br>Value: CHF %{x:,.0f}<extra></extra>'
    )
    
    fig.update_layout(
        height=400,
        showlegend=True,
        xaxis_tickformat=',.0f'
    )
    
    return fig


def plot_deposit_outflows_by_type(df):
    """
    Create bar chart for deposit outflows by type
    
    Args:
        df: DataFrame with DEPOSIT_TYPE and TOTAL_OUTFLOW_CHF
        
    Returns:
        plotly.graph_objects.Figure
    """
    # Sort by outflow
    df_sorted = df.sort_values('TOTAL_OUTFLOW_CHF', ascending=False)
    
    fig = px.bar(
        df_sorted,
        x='DEPOSIT_TYPE',
        y='TOTAL_OUTFLOW_CHF',
        title="Deposit Outflows by Type",
        labels={'TOTAL_OUTFLOW_CHF': 'Total Outflow (CHF)', 'DEPOSIT_TYPE': 'Deposit Type'},
        color='COUNTERPARTY_TYPE',
        color_discrete_map={
            'RETAIL': '#007AFF',
            'CORPORATE': '#FF9F0A',
            'FINANCIAL_INSTITUTION': '#FF3B30'
        }
    )
    
    fig.update_traces(
        hovertemplate='<b>%{x}</b><br>Outflow: CHF %{y:,.0f}<extra></extra>'
    )
    
    fig.update_layout(
        height=400,
        xaxis_tickangle=-45,
        yaxis_tickformat=',.0f'
    )
    
    return fig


def plot_lcr_gauge(current_lcr):
    """
    Create gauge chart for current LCR ratio
    
    Args:
        current_lcr (float): Current LCR ratio percentage
        
    Returns:
        plotly.graph_objects.Figure
    """
    # Determine color based on value
    if current_lcr >= 105:
        color = '#34C759'  # Green
    elif current_lcr >= 100:
        color = '#FFCC00'  # Yellow
    else:
        color = '#FF3B30'  # Red
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=current_lcr,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Current LCR Ratio", 'font': {'size': 24}},
        delta={'reference': 100, 'suffix': ' pp'},
        gauge={
            'axis': {'range': [None, 150], 'ticksuffix': '%'},
            'bar': {'color': color},
            'steps': [
                {'range': [0, 95], 'color': "#FFE5E5"},    # Light red
                {'range': [95, 100], 'color': "#FFF3CD"},  # Light yellow
                {'range': [100, 105], 'color': "#E8F5E9"}, # Light green
                {'range': [105, 150], 'color': "#C8E6C9"}  # Darker green
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 100
            }
        }
    ))
    
    fig.update_layout(
        height=350,
        margin=dict(l=20, r=20, t=80, b=20)
    )
    
    return fig


def plot_hqla_vs_outflows(df):
    """
    Create waterfall chart showing HQLA vs Outflows
    
    Args:
        df: DataFrame with HQLA_TOTAL and OUTFLOW_TOTAL
        
    Returns:
        plotly.graph_objects.Figure
    """
    if len(df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        return fig
    
    hqla = df['HQLA_TOTAL'].iloc[0]
    outflow = df['OUTFLOW_TOTAL'].iloc[0]
    buffer = hqla - outflow
    
    fig = go.Figure(go.Waterfall(
        name="LCR Components",
        orientation="v",
        measure=["absolute", "absolute", "total"],
        x=["HQLA<br>(Numerator)", "Net Outflows<br>(Denominator)", "LCR Buffer"],
        y=[hqla, -outflow, buffer],
        text=[f"CHF {hqla:,.0f}", f"CHF {outflow:,.0f}", f"CHF {buffer:,.0f}"],
        textposition="outside",
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        decreasing={"marker": {"color": "#FF3B30"}},
        increasing={"marker": {"color": "#34C759"}},
        totals={"marker": {"color": "#007AFF"}}
    ))
    
    fig.update_layout(
        title="HQLA vs Net Cash Outflows",
        height=450,
        showlegend=False,
        yaxis_tickformat=',.0f'
    )
    
    return fig


def plot_monthly_compliance_trend(df):
    """
    Create bar chart for monthly compliance status
    
    Args:
        df: DataFrame with REPORT_MONTH, AVG_LCR_RATIO, and compliance days
        
    Returns:
        plotly.graph_objects.Figure
    """
    fig = go.Figure()
    
    # Add average LCR bar
    fig.add_trace(go.Bar(
        x=df['REPORT_MONTH'],
        y=df['AVG_LCR_RATIO'],
        name='Average LCR',
        marker_color='#007AFF',
        text=df['AVG_LCR_RATIO'].round(1),
        texttemplate='%{text}%',
        textposition='outside'
    ))
    
    # Add regulatory line
    fig.add_hline(
        y=100,
        line_dash="dash",
        line_color="red",
        annotation_text="Regulatory Minimum",
        annotation_position="right"
    )
    
    fig.update_layout(
        title="Monthly LCR Compliance Trend",
        height=400,
        xaxis_title="Month",
        yaxis_title="Average LCR Ratio (%)",
        hovermode='x unified'
    )
    
    return fig

