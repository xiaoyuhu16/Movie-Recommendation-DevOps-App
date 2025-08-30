#!/bin/bash

# Set paths
PROJECT_DIR=~/group-project-f24-jurassic-spark
VENV_DIR=$PROJECT_DIR/.venv

# Ensure the project directory exists
if [ ! -d "$PROJECT_DIR" ]; then
  echo "Project directory $PROJECT_DIR does not exist."
  exit 1
fi

# Navigate to the project directory
cd "$PROJECT_DIR"

# Check if the virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
  echo "Virtual environment not found. Creating one..."
  python3 -m venv "$VENV_DIR"
  echo "Virtual environment created."
fi

# Activate the virtual environment
source "$VENV_DIR/bin/activate"

# Install dependencies
pip install -r requirements.txt

# Run the Python script
python3 automated_model_updates.py
