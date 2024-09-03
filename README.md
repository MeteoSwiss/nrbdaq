# nrbdaq
DAQ for Nairobi GAW station. Intended for deployment on a Raspberry Pi 4.

# setup crontab for automatic execution like so ...
$ crontab -e

1. add the code shown in file 'cron'
2. make executable
$ sudo chmod +x /home/gaw/git/nrbdaq/nrbdaq.sh
