#!/usr/bin/bash

service_l=("gonarch-api" "gonarch-gui" "gonarch-core" "gonarch-check")

echo "Gonarch service check"
echo "Timestamp: " $(date +"%Y-%m-%d %H:%M:%S")
echo "=========================="

for i in "${service_l[@]}"; do    
    status=$(systemctl is-active ${i})
    if [ "$?" -gt 0 ]; then    
        echo "${i}: not reachable"
    else
        echo "${i}: ${status}"
    fi
    
done
echo " "
echo "HAProxy load balancer layer"
echo "=========================="
echo "show stat" | socat /run/haproxy/api.sock stdio | cut -d "," -f 1-2,18 | column -s, -t

