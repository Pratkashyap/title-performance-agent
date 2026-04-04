#!/bin/bash
# ─────────────────────────────────────────────────────────────
# Title Performance Analyst — One-command setup
# Run: bash setup.sh
# ─────────────────────────────────────────────────────────────

set -e

echo ""
echo "🎬 Title Performance Analyst — Setup"
echo "─────────────────────────────────────"

# 1. Check Python
if ! command -v python3 &>/dev/null; then
  echo "❌ Python 3 not found. Install from https://www.python.org/downloads/"
  exit 1
fi
echo "✅ Python: $(python3 --version)"

# 2. Create virtual environment
if [ ! -d "venv" ]; then
  echo "📦 Creating virtual environment..."
  python3 -m venv venv
else
  echo "✅ Virtual environment already exists"
fi

# 3. Activate and install dependencies
source venv/bin/activate
echo "📦 Installing dependencies..."
pip install -q -r requirements.txt
echo "✅ Dependencies installed"

# 4. Check .env
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo ""
  echo "⚠️  ACTION REQUIRED:"
  echo "   Open .env and add your Anthropic API key:"
  echo "   ANTHROPIC_API_KEY=\"sk-ant-...\""
  echo ""
  echo "   Get your key at: https://console.anthropic.com"
  echo ""
  read -p "Press Enter once you've added your API key..."
fi

# 5. Generate database
if [ ! -f "data/title_performance.duckdb" ]; then
  echo "🦆 Generating DuckDB database..."
  python3 data/generate_data.py
  echo "✅ Database created: data/title_performance.duckdb"
else
  echo "✅ Database already exists"
fi

# 6. Done
echo ""
echo "─────────────────────────────────────"
echo "✅ Setup complete! Run the dashboard:"
echo ""
echo "   source venv/bin/activate"
echo "   streamlit run dashboard_app.py"
echo ""
echo "   Then open: http://localhost:8501"
echo "─────────────────────────────────────"
