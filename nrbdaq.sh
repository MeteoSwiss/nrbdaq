#!/bin/bash

# Set necessary environment variables
export PATH="/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="/usr/lib/python311.zip:/usr/lib/python3.11:/usr/lib/python3.11/lib-dynload:/usr/local/lib/python3.11/dist-packages:/usr/lib/python3/dist-packages:/usr/lib/python3.11/dist-packages"

# Check if the script is already running
if pgrep -f "python3 /home/gaw/git/nrbdaq/nrbdaq.py" > /dev/null
then
    echo "nrbdaq.py already running."
    exit 1
else
    # load venv
    source .venv/bin/activate
    # Start the script
    python3 /home/gaw/git/nrbdaq/nrbdaq.py
fi

# NB: make sure, these are executable
# chmod +x /home/gaw/git/nrbdaq/nrbdaq.sh
# chmod +x .venv/bin/activate
