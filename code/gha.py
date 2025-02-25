#!/usr/bin/python3
import requests, yaml, sys, argparse
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

#Argparser config
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--status", help="Shows the summary of all clusters status", action='store_true')
parser.add_argument("-c", "--cluster", help = "Shows a given cluster details", type=str, dest="cluster_name")

args = parser.parse_args()
if args.status:
    # create header
    cluster_info_thead = ["Name", "Creation date", "Status", "Max replication lag", "Promotion rule", "Reader endpoint", "Writer endpoint"]
    cluster_info_tbody = []    
    cluster_l = backend_db.ClusterList() 
    # Loop over all clusters to show details of them
    for c in cluster_l:
        json_data = {"name": c['name']}
        response = requests.get('http://127.0.0.1:2423/api/v1/cluster/details', json=json_data)
        result = response.json()        
        cluster_info_tbody.append([
            result['cluster_name'], 
             result['creation_date'], 
            "\033[32mUp\033[0m" if result['in_maintenance'] == 0 else "\033[31mDown\033[0m", 
            result['max_replication_lag'], 
            result['promotion_rule'],
            result['reader_endpoint'],
            result['writer_endpoint']   
        ])

        print(tabulate(cluster_info_tbody, headers=cluster_info_thead, tablefmt="rounded_grid"))
if args.cluster_name:
    cluster_result = backend_db.ClusterCheckExisting(args.cluster_name)
    if cluster_result == 0:  
        print("Cluster name doesnt exists")
        exit()

    json_data = {"name": args.cluster_name}
    response = requests.get('http://127.0.0.1:2423/api/v1/cluster/details', json=json_data)
    result = response.json()
    # create header
    node_info_thead = ["Id", "Name", "Ip:port", "Reachable", "Role", "Promotable", "Access level", "Act/runn", "Replication"]
    node_info_tbody = []
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

