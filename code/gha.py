#!/usr/bin/python3
import requests, yaml, sys
from tabulate import tabulate

# Load classes depending if PyInstaller is using them 
if getattr(sys, 'frozen', False):
   from classes_backend import BackEndSqlModel
elif __file__:
    from classes.classes_backend import BackEndSqlModel

####### Config section ####### 
with open(r'/etc/gonarch.conf') as file:
    config_file = yaml.load(file, Loader=yaml.FullLoader)
dbname = config_file['workspace']['backend_dbname']
gha_ip = config_file['workspace']['ip']

# Define the backend model class for db operations
backend_db = BackEndSqlModel(dbname)
cluster_l = backend_db.ClusterList()  

for c in cluster_l:
    json_data = {"name": c['name']}
    response = requests.get('http://127.0.0.1:2423/api/v1/cluster/details', json=json_data)
    result = response.json()

    print("              Cluster status")

    cluster_info_tbody = [
        ["Cluster name", result['cluster_name']], 
        ["Creation date",  result['creation_date']], 
        ["In maintenance", "\033[32mNo\033[0m" if result['in_maintenance'] == 0 else "\033[31mYes\033[0m"], 
        ["Max replication lag", result['max_replication_lag']], 
        ["Promotion rule",  result['promotion_rule']],
        ["Reader endpoint",  result['reader_endpoint']],
        ["Writer endpoint", result['writer_endpoint']]      
    ]
    print(tabulate(cluster_info_tbody, tablefmt="rounded_grid"))

    # create header
    node_info_thead = ["Id", "Name", "Ip:port", "Reachable", "Role", "Promotable", "Access level", "Act/runn", "Replication"]
    node_info_tbody = []
    print("              Nodes status")

    for i in result['instances']:         
        node_info_tbody.append([
            i['instance_id'], 
            i['instance_name'], 
            i['hostname:port'],         
            "\033[32mYes\033[0m" if i['reachable'] == 1 else "\033[31mNo\033[0m",
            i['role'],
            "NA" if i['role'] == 'primary' else "\033[32mYes\033[0m" if i['replication']['promotable'] == 1 else "\033[31mNo\033[0m",
            "Out of traffic" if i['role'] == 'backup' else "Read/Write" if i['access_level'] == 'rw' else "Read only",
            "{0}/{1}".format(i['threads']['connected'], i['threads']['running']), 
            "NA" if i['role'] == 'primary' else "\033[31mIO thread stopped\033[0m" if i['replication']['io_thread_running'] == "No" else "\033[31mSQL thread stopped\033[0m" if i['replication']['sql_thread_running'] == "No" else "\033[33mCathing up\033[0m" if i['replication']['replication_lag'] > 0 else "\033[32mRunning\033[0m"            
        ])

    print(tabulate(node_info_tbody, headers=node_info_thead, tablefmt="rounded_grid"))
    print("===============================================================================")