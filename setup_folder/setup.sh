#!/bin/bash

# Step 1: Create virtual environment
python3.11 -m venv biopapers

# Step 2: Activate the virtual environment
source biopapers/bin/activate

# Step 3: Upgrade pip (optional but recommended)
pip install --upgrade pip

# Step 4: Install dependencies
pip install -r requirements.txt

echo "✅ Environment '.robust_lab' is ready and all dependencies are installed."
