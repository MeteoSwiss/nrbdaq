# nrbdaq
DAQ for Nairobi GAW station. Intended for deployment on a Raspberry Pi 4.

# setup crontab for automatic execution like so ...
$ crontab -e

1. add the code shown in file 'cron'
2. make executable
$ sudo chmod +x /home/gaw/git/nrbdaq/nrbdaq.sh

# Alternatively, set up a systemd service
1. copy file 'nrbdaq.service' to /etc/systemd/system/nrbdaq.service
2. make executable
$ sudo chmod 744 /home/gaw/git/nrbdaq/nrbdaq.sh
$ sudo chmod 664 /etc/systemd/system/nrbdaq.service
3. enable service
$ sudo systemctl daemon-reload
$ sudo systemctl enable nrbdaq.service

# read journal
$ sudo journalctl -p err --since "2024-08-27" --until "2024-08-29"
    show all journal entries of level ERROR in specified period

$ systemctl status cron.service	
    show status of cron

$ ps [aux]
    show active processes 
    [a: displays information about other users' processes as well as your own.
     u: displays the processes belonging to the specified usernames.
     x: includes processes that do not have a controlling terminal.]
