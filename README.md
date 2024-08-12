# Gonarch HA community edition

Deploy a robust MySQL high availability solution utilizing Gonarch HA, a versatile open-source tool that integrates proxy, load balancing, automatic failover, and replication management functionalities into a cohesive package.

Documentation: https://github.com/raa82/gonarch_ha_ce/wiki

Creator LinkedIn profile: https://www.linkedin.com/in/ralvarezaragon/ 

## Install

#### From binaries
Just download the latest release tar file and run install.sh but keep in mind that the script only runs in Ubuntu for now.

#### From source code
The whole installation script can be done manually in case you are not in Ubuntu. As ROOT:

* Install sqlite3, haproxy and socat in your OS (packages names may vary)
* Create the folder in /opt/gonarch and /run/haproxy
* Move gonarch.conf to /etc/
* Edit /etc/gonarch.confg and change this
  ```bash
  workspace:
  name: Choose a workspace name (this has no real impact so far just pick up anything you want)
  ip: This host IP
  ```
* Move files to /opt/gonarch:
  - bin/*
  - resources/haproxy_template.j2
* Create the sqlite backend DB
  ```bash
  sqlite3 /opt/gonarch/backend.db < resources/backend_table_def.sql
  ```
* Create gonarch user at OS level and change ownership to it for these files/folders:
  ```bash
  chown gonarch:gonarch /etc/gonarch.conf
  chown gonarch:gonarch /etc/haproxy/haproxy.cfg
  chown -R gonarch:gonarch /opt/gonarch
  chown -R gonarch:gonarch /var/log/gonarch
  ```
* Move systemd files
  ```bash
  cp resources/gonarch-check.service /etc/systemd/system/
  cp resources/gonarch-core.service /etc/systemd/system/
  cp resources/gonarch-api.service /etc/systemd/system/
  ```
* Reload systemctl daemons
  ```bash
  systemctl daemon-reload
  ```
* Start Gonarch HA services
  ```bash
  systemctl restart haproxy
  systemctl start gonarch-check
  systemctl start gonarch-core
  systemctl start gonarch-api
  ```
Now Gonarch HA is installed and ready to be used regardless the Linux distro.

**NOTE**: Your OS could have some firewall in place that block multiple modules. Ensure you have whitelist these systemd processes.

## Need to talk?
You can reach me either via LinkedIn (I'm pretty active there) or via email at ralvarez@gonarch.tech
