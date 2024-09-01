# nrbdaq
DAQ for Nairobi GAW station. Intended for deployment on a Raspberry Pi 4.

# setup crontab for automatic execution like so ...
# sudo crontab -e
# add the following code
# @reboot source /home/gaw/git/nrbdaq/nrbdaq.sh > /var/log/syslog 2>&1
# 0 0 * * * source /home/gaw/git/nrbdaq/nrbdaq.sh > /var/log/syslog 2>&1