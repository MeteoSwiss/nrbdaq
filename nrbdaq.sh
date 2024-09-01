#!/bin/bash
cd /

# load virtual environment
source .venv/bin/activate

# navigate to launch folder
cd /home/gaw/git/nrbdaq/

# execute script
python3 nrbdaq.py

# go back to root
cd /

# NB: make sure, this script is executable
# chmod 755 nrbdaq.sh