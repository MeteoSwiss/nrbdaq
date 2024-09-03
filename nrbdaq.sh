#!/bin/bash

# Set necessary environment variables
export PATH="/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="/usr/lib/python311.zip:/usr/lib/python3.11:/usr/lib/python3.11/lib-dynload:/usr/local/lib/python3.11/dist-packages:/usr/lib/python3/dist-packages:/usr/lib/python3.11/dist-packages"

# load virtual environment
/bin/bash .venv/bin/activate

# navigate to launch folder
cd /home/gaw/git/nrbdaq/

# execute script
python3 nrbdaq.py

# NB: make sure, these are executable
# chmod +x /home/gaw/git/nrbdaq/nrbdaq.sh
# chmod +x .venv/bin/activate