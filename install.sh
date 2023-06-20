#!/usr/bin/bash

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
  then echo "Installation must be executed as root"
  exit
fi

error_handler(){
    echo -ne "${txtred}ERROR\n${txtori}"
    echo -e "An error acurred during the installation. Please send the whole output of the install script to ${txtcyn}support@gonarch.tech${txtori} to get some help. Thanks"
    exit
}

# Create a short intro
echo -e "Gonarch HA Community Edition - Installer"
echo -e "Gonarch implements proxy, load balancer, replication manager and auto failover capabilities for MySQL 5.7 and 8.0"
echo -e "NOTE: This is a development version and should not be used in production"
version=$(cat version)
echo -e "Version: ${version}"

echo -n "Create a new workspace name: " 
read wname
echo -n "Insert the local IP that Gonarch will use to expose traffic to the other apps: " 
read ip

# install dependencies
echo -e "Installing package dependencies"
os_name=$(cat /etc/os-release | grep ^ID= | awk -F"=" '{print $2}' | tr -d '"')

if [ "$os_name" == "ubuntu" ]; then
    package_l=('sqlite' 'libmysqlclient-dev' 'haproxy')
    for i in "${package_l[@]}"; do
        echo -ne "- Install ${i}... "
        apt install --no-install-recommends -y -qqq ${i} >/dev/null 2>&1
        if [ "$?" -eq 0 ]; then
            echo -ne "${txtgrn}OK\n${txtori}"
        else
            error_handler
        fi
    done 

elif [ "$os_name" == "centos" ]; then
    package_l=('epel-release' 'socat' 'sqlite-devel' 'haproxy' 'mariadb-libs-5.5.68-1.el7.x86_64')
    for i in "${package_l[@]}"; do
        echo -ne "- Install ${i}... "
        yum install -y -q ${i} >/dev/null 2>&1
        if [ "$?" -eq 0 ]; then
            echo -ne "${txtgrn}OK\n${txtori}"
        else
            error_handler
        fi
    done 
    echo -e "Create a firewall rule for API connection... "
    # Allow bind address for HAproxy at Selinux level
    setsebool -P haproxy_connect_any=1
    # Copy the xml for firewall rule
    cp resources/firewalld-service.xml /usr/lib/firewalld/services/gonarch-api.xml
    if [ "$?" -ne 0 ]; then
        error_handler
    fi
    # Restart firewalld
    systemctl reload firewalld
    if [ "$?" -ne 0 ]; then
        error_handler
    fi
    # Add the new created rule
    firewall-cmd --zone=public --permanent --add-service=gonarch-api
    if [ "$?" -eq 0 ]; then
        echo -ne "${txtgrn}OK\n${txtori}"
    else
        error_handler
    fi

else
    echo -e "This OS is not supported (${os_name}). You can contact us to check if this OS is in the Gonarch's route map"
fi

# Create /run/haproxy, /var/log/gonarch & /opt/gonarch
echo -n "Create Gonarch folder in /opt... "
mkdir -p /var/log/gonarch >/dev/null 2>&1
if [ "$?" -gt 0 ]; then    
    error_handler
fi
mkdir -p /opt/gonarch >/dev/null 2>&1
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi
mkdir -p /run/haproxy >/dev/null 2>&1
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

# Move gonarch.conf and edit it
echo -n "Create Gonarch config file in /etc... "
cp resources/gonarch.conf.template /etc/gonarch.conf >/dev/null 2>&1
if [ "$?" -gt 0 ]; then    
    error_handler
fi
sed -i "s/WSNAME/$wname/g" /etc/gonarch.conf >/dev/null 2>&1
if [ "$?" -gt 0 ]; then    
    error_handler
fi
sed -i "s/WSIP/$ip/g" /etc/gonarch.conf >/dev/null 2>&1
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

# Move gonarch bin files
cp bin/core bin/check bin/api resources/check_status.sh resources/uninstall.sh resources/haproxy_template.j2 version /opt/gonarch

# Create the backend DB
echo -n "Create backend DB... "
if [ ! -e "/opt/gonarch/backend.db" ]; then
    sqlite3 /opt/gonarch/backend.db < resources/backend_table_def.sql >/dev/null 2>&1
    if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
    else
        error_handler
    fi
else
    echo -ne "${txtgrn}SKIPPED\n${txtori}"
fi

# Create gonarch user and assign all files to it
echo -n "Create Gonarch user... "
if [ $(id gonarch | wc -l) -eq 0 ]; then
    useradd gonarch >/dev/null 2>&1
    if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
    else
        error_handler
    fi
else
    echo -ne "${txtgrn}SKIPPED\n${txtori}"
fi

echo -n "Change file permissions... "
chown gonarch:gonarch /etc/gonarch.conf >/dev/null 2>&1
if [ "$?" -gt 0 ]; then    
    error_handler
fi
chown gonarch:gonarch /etc/haproxy/haproxy.cfg >/dev/null 2>&1
if [ "$?" -gt 0 ]; then    
    error_handler
fi
chown -R gonarch:gonarch /opt/gonarch >/dev/null 2>&1
if [ "$?" -gt 0 ]; then    
    error_handler
fi
chown -R gonarch:gonarch /var/log/gonarch  >/dev/null 2>&1
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

# Move systemd files 
echo -n "Create Gonarch Check service... "
cp resources/gonarch-check.service /etc/systemd/system/
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi
echo -n "Create Gonarch Core service... "
cp resources/gonarch-core.service /etc/systemd/system/
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi
echo -n "Create Gonarch API service... "
cp resources/gonarch-api.service /etc/systemd/system/
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi
systemctl daemon-reload
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

# Start services
echo -n "Start HAproxy... "
systemctl restart haproxy
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

echo -n "Start Gonarch Check service... "
systemctl restart gonarch-check
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi
echo -n "Start Gonarch Core service... "
systemctl restart gonarch-core
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi
echo -n "Start Gonarch API service... "
systemctl restart gonarch-api
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

echo -e "Installation completed. Gonarch API is now reachable in port 2423."

