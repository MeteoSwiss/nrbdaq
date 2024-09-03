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