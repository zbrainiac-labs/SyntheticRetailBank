#!/bin/bash
# Quick start script for Synthetic Retail Bank

echo "═══════════════════════════════════════════════════════════"
echo "🏦 Synthetic Retail Bank - Quick Start"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
    echo ""
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate
echo "✅ Virtual environment activated"
echo ""

# Check if dependencies are installed
if [ ! -f "venv/lib/python3.*/site-packages/streamlit" ]; then
    echo "📥 Installing dependencies..."
    pip install -r requirements.txt
    echo "✅ Dependencies installed"
    echo ""
fi

# Check if secrets.toml exists
if [ ! -f ".streamlit/secrets.toml" ]; then
    echo "⚠️  WARNING: .streamlit/secrets.toml not found!"
    echo ""
    echo "Creating default secrets.toml (uses ~/.snowflake/connections.toml for auth)..."
    echo ""
    cat > .streamlit/secrets.toml << 'EOF'
# Snowflake connection configuration
# Authentication is handled via ~/.snowflake/connections.toml (OAuth browser flow)

[snowflake]
connection_name = "DEMO_MDAEPPEN"
warehouse = "MD_TEST_WH"
database = "AAA_DEV_SYNTHETIC_BANK"
schema = "CRM_AGG_V001"
role = "ACCOUNTADMIN"
EOF
    echo "✅ Created .streamlit/secrets.toml"
    echo ""
fi

# Run Streamlit app
echo "🚀 Starting Synthetic Retail Bank..."
echo ""
echo "The app will open in your browser at: http://localhost:8501"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo ""

streamlit run app.py

