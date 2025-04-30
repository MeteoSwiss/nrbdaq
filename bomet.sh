#!/bin/bash

# Set necessary environment variables
export PATH="/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="/usr/lib/python313.zip:/usr/lib/python3.13:/usr/lib/python3.13/lib-dynload:/usr/local/lib/python3.13/dist-packages:/usr/lib/python3/dist-packages:/usr/lib/python3.13/dist-packages"

# Ensure the named pipe exists or create it
#PIPE="/tmp/bomet"
#if [ ! -p "$PIPE" ]; then
#    mkfifo "$PIPE"
#fi

# Check if the script is already running
if pgrep -f -a "bomet.py" > /dev/null
then
    echo "$(date +\%FT\%T), INFO, bomet.sh, bomet.py already running."
    # exit 1
else
    # load venv
    source /home/admin/Documents/git/nrbdaq/.venv/bin/activate
    echo "$(date +\%FT\%T), INFO, bomet.sh, .venv activated"

    # change cwd
    cd /home/admin/Documents/git/nrbdaq/

    # Start the script
    /home/admin/Documents/git/nrbdaq/.venv/bin/python3 -u /home/admin/Documents/git/nrbdaq/bomet.py
#    /home/admin/Documents/git/.venv/bin/python3 -u /home/admin/Documents/git/nrbdaq/bomet.py > "$PIPE"
    echo "$(date +\%FT\%T), INFO, bomet.sh, == BOMET daQ (re)started ====" >> /home/admin/Documents/bomet/bomet.log 2>&1
fi

# NB: make sure, these are executable
# chmod +x /home/admin/Documents/git/nrbdaq/bomet.sh
# chmod +x .venv/bin/activate
