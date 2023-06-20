#!/usr/bin/bash


# Add versiosn to apt packages

txtblk='\e[0;30m' # Black - Regular
txtred='\e[0;31m' # Red
txtgrn='\e[0;32m' # Green
txtylw='\e[0;33m' # Yellow
txtblu='\e[0;34m' # Blue
txtpur='\e[0;35m' # Purple
txtcyn='\e[0;36m' # Cyan
txtwht='\e[0;37m' # White
txtori='\e[0m'

if [ "$EUID" -ne 0 ]
  then echo "Unistall must be executed as root"
  exit
fi

error_handler(){
    echo -ne "${txtred}ERROR\n${txtori}"
    echo -e "An error acurred during Gonarch uninstall. Please send the output of this script and the uninstall_error.log file to ${txtcyn}support@gonarch.tech${txtori} to get some help. Thanks"
    exit
}

# Create a short intro
echo -e "Gonarch HA Community Edition - Uninstaller"
echo -e "NOTE: This is a development version and should not be used in Production"
echo -e "NOTE: OS and Python dependencies will not be removed in case they are in use by another process."

# Stop Gonarch services
echo -n "Stop Gonarch Check service... "
systemctl stop gonarch-check
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi
echo -n "Stop Gonarch Core service... "
systemctl stop gonarch-core
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi
echo -n "Stop Gonarch API service... "
systemctl stop gonarch-api
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

sleep 5
# Remove systemd files 
echo -n "Remove Gonarch services... "
rm -f /etc/systemd/system/gonarch*
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi
systemctl daemon-reload

# Remove any installed files 
echo -n "Remove config,logs and app folder... "
rm -rf /opt/gonarch
rm -f /etc/gonarch.conf
rm -f /var/log/gonarch.log
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

# Cleanup HAProxy
echo -n "Reload HAProxy... "
systemctl reload haproxy
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

# Remove gonarch user 
echo -n "Remove Gonarch user... "
userdel gonarch
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

echo -e "Gonarch succesfully uninstalled"
