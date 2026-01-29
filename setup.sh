#!/bin/bash
# One-click setup script for DW MCP Server

set -e

echo "=========================================="
echo "DW MCP Server Setup Script"
echo "=========================================="
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC}  $1"
}

# Step 1: Check Python version
echo "Step 1: Checking Python version..."
PYTHON_CMD=""

# Check current python
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
    
    if [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 10 ]; then
        PYTHON_CMD="python3"
        print_success "Found Python $PYTHON_VERSION"
    fi
fi

# Try to find python3.10, python3.11, python3.12
if [ -z "$PYTHON_CMD" ]; then
    for version in 3.12 3.11 3.10; do
        if command -v python$version &> /dev/null; then
            PYTHON_CMD="python$version"
            PYTHON_VERSION=$(python$version --version 2>&1 | awk '{print $2}')
            print_success "Found Python $PYTHON_VERSION"
            break
        fi
    done
fi

if [ -z "$PYTHON_CMD" ]; then
    print_error "Python 3.10+ not found"
    echo ""
    echo "Solutions:"
    echo "  macOS:   brew install python@3.10"
    echo "  Ubuntu:  sudo apt install python3.10"
    echo "  Windows: Download from python.org"
    exit 1
fi

# Step 2: Create virtual environment (optional)
echo ""
echo "Step 2: Setting up virtual environment (optional)..."
if [ ! -d "venv" ]; then
    read -p "Create a virtual environment? (recommended) [Y/n]: " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        $PYTHON_CMD -m venv venv
        print_success "Virtual environment created"
        
        # Activate virtual environment
        if [ -f "venv/bin/activate" ]; then
            source venv/bin/activate
            print_success "Virtual environment activated"
        elif [ -f "venv/Scripts/activate" ]; then
            source venv/Scripts/activate
            print_success "Virtual environment activated"
        fi
    fi
else
    print_warning "Virtual environment already exists"
    # Try to activate it
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
        print_success "Virtual environment activated"
    elif [ -f "venv/Scripts/activate" ]; then
        source venv/Scripts/activate
        print_success "Virtual environment activated"
    fi
fi

# Step 3: Install dependencies
echo ""
echo "Step 3: Installing dependencies..."
pip install -q --upgrade pip
pip install -r requirements.txt

if [ $? -eq 0 ]; then
    print_success "Dependencies installed successfully"
else
    print_error "Failed to install dependencies"
    exit 1
fi

# Step 4: Check for .env file
echo ""
echo "Step 4: Checking configuration..."
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        print_warning ".env file not found"
        read -p "Create .env from .env.example? [Y/n]: " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            cp .env.example .env
            print_success ".env file created"
            print_warning "Please edit .env file with your credentials"
        fi
    else
        print_warning ".env file not found (optional)"
    fi
else
    print_success ".env file found"
fi

# Step 5: Run validation
echo ""
echo "Step 5: Validating setup..."
$PYTHON_CMD -c "
import sys
from src.dw_mcp.startup_checks import check_python_version, check_dependencies

print()
if not check_python_version():
    sys.exit(1)
    
if not check_dependencies():
    sys.exit(1)

print()
print('✓ All checks passed!')
"

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    print_success "Setup completed successfully!"
    echo "=========================================="
    echo ""
    echo "Next steps:"
    echo "1. Configure your database connections in .env file"
    echo "2. Run the server:"
    echo "   $PYTHON_CMD -m src.dw_mcp.server"
    echo ""
else
    print_error "Setup validation failed"
    exit 1
fi
