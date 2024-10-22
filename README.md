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

# linux goodies
## read journal
$ sudo journalctl -p err --since "2024-08-27" --until "2024-08-29"
    show all journal entries of level ERROR in specified period

$ systemctl status cron.service	
    show status of cron

$ ps [aux]
    show active processes 
    [a: displays information about other users' processes as well as your own.
     u: displays the processes belonging to the specified usernames.
     x: includes processes that do not have a controlling terminal.]

## list active instances of nrbdaq.py
$ pgrep -f -a "nrbdaq.py"

## kill a process by id
$ kill <pid>

<<<<<<< HEAD
## list USB / serial ports
$ dmesg | grep tty
=======
## How-to operate Get red-y MFCs
1. Install cable PPDM-U driver from /resources
2. Install get red-y MFC software
3. Plug in USB cable, check COM port used in device manager
4. Start get red-y software, select port, search for device, stop search once found

## Setup
2024-10-22/jkl
- Aurora3000 
    - flow controlled by get red-y MFC set to 4 lnpm
    - dark count found to be 450-500
    - Wavelength 1 Shtr Count ca 1.1M
    - Wavelength 2 Shtr Count ca 1.6M
    - Wavelength 3 Shtr Count ca 1.7M
- AE31 flow controlled by get red-y set to 3 lnpm
    - 
>>>>>>> 933ff3bf245e0136a2440e07991a4298a1de60cc
