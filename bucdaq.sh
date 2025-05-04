#!/bin/bash

# Set necessary environment variables
export PATH="/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="/usr/lib/python312.zip:/usr/lib/python3.12:/usr/lib/python3.12/lib-dynload:/usr/local/lib/python3.12/dist-packages:/usr/lib/python3/dist-packages:/usr/lib/python3.12/dist-packages"

# Ensure the named pipe exists or create it
#PIPE="/tmp/bucdaq"
#if [ ! -p "$PIPE" ]; then
#    mkfifo "$PIPE"
#fi

# Check if the script is already running
if pgrep -f -a "bucdaq.py" > /dev/null
then
    echo "$(date +\%FT\%T), INFO, bucdaq.sh, bucdaq.py already running."
    # exit 1
else
    # load venv
    source /home/admin/git/nrbdaq/.venv/bin/activate
    echo "$(date +\%FT\%T), INFO, bucdaq.sh, .venv activated"

    # change cwd
    cd /home/admin/git/nrbdaq/

    # Start the script
    /home/admin/git/nrbdaq/.venv/bin/python3 -u /home/admin/git/nrbdaq/bucdaq.py
    echo "$(date +\%FT\%T), INFO, bucdaq.sh, == BUCDAQ (re)started ====" >> /home/admin/Documents/bucdaq/bucdaq.log 2>&1
fi

# NB: make sure, these are executable
# chmod +x /home/admin/git/bucdaq/bucdaq.sh
# chmod +x .venv/bin/activate
