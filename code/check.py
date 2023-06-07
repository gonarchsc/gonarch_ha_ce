#!/usr/bin/python3
import logging, yaml, time, datetime, subprocess, socket, json, threading, sys
from sqlalchemy import exc
from multiprocessing import Process, Queue

# Load classes depending if PyInstaller is using them 
if getattr(sys, 'frozen', False):
   from classes_node import Node
   from classes_backend import BackEndSqlModel
elif __file__:
    from classes.classes_backend import BackEndSqlModel
    from classes.classes_node import Node

####### Config section ####### 
with open(r'/etc/gonarch.conf') as file:
    config_file = yaml.load(file, Loader=yaml.FullLoader)

dbname = config_file['workspace']['backend_dbname']
gonarch_cred = "{0}:{1}".format(config_file['mysql_credentials']['user'], config_file['mysql_credentials']['pass'])

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "cluster": "%(cname)s", "node": "%(nname)s", "msg": "%(message)s", "detail": "%(detail)s"}',
    handlers=[
        logging.FileHandler(config_file['core']['logging']['path'])        
    ]
)  
logger = logging.getLogger()
           
def elapsed_time(start):
    end = time.time()
    duration = (end - start)    
    return str(datetime.timedelta(seconds=duration))

def is_reachable(gonarch_cred, i):    
    start = time.time()  
    api_socket = 'socat /run/haproxy/api.sock stdio'  
    # Create the socket
    #s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # try to perform initial conection to node. Add it as non reachable if connection fails.
    try:                   
        # Define the object and perform the connection to MySQL target instance.
        node = Node(i['c_name'], dbname, logger, gonarch_cred)
        conn_string = "{0}:{1}".format(i['hostname'], i['port'])         
        conn = node.Connect(conn_string)
        # Fetch MySQL global variables           
        mysql_vars = node.FetchTargetVars(conn)
        # Fetch MySQL Global status variables
        mysql_status_var = node.FetchTargetStatus(conn)        
        # Fetch master status for ALL nodes
        master_status = node.FetchTargetMasterStatus(conn)        
        # Fetch replica's IP from primary's processlist
        repl_ip_list = node.GetReplicaIpList(conn)   
        # Fetch slave status 
        repl_status_dict = node.FetchTargetSlaveStatus(conn)
        # Build up the final result dict for Core module        
        node_info_dict = {
            'cluster_name': i['c_name'],
            'promotion_rule': i['promotion_rule'],
            'node_id': i['id'],            
            #'role': node.SetRole(repl_ip_list, repl_status_dict),            
            'reachable': 1,
            'read_only': mysql_vars['@@read_only'],
            'elapsed_time': elapsed_time(start),
            'node_name': mysql_vars['@@hostname'],
            'hostname': mysql_vars['@@hostname'],                
            'port': mysql_vars['@@port'],
            'version': mysql_vars['@@version'],
            'server_uuid': mysql_vars['@@server_uuid'], 
            'expire_logs_days': mysql_vars['@@expire_logs_days'],
            'version': mysql_vars['@@version'],
            'replication_mode': 'gtid' if mysql_vars['@@gtid_mode'] in ('ON', 'ON_PERMISSIVE') else 'binlog',
            'master_binlog_file': master_status['File'],
            'master_binlog_pos': master_status['Position'],
            'master_gtid_executed': master_status['Executed_Gtid_Set'],            
            'repl_ip_list': repl_ip_list,
            'failover_coord': i['failover_coord'],
            'arch': 'rds' if 'rds' in mysql_vars['@@basedir'] else 'on-prem',   
            'thread_connected': mysql_status_var['Threads_connected'], 
            'thread_running': mysql_status_var['Threads_running'],       
            'repl_status_dict': repl_status_dict               
        }
        action= 'enable'           
        if i['reachable'] == 0:
            logger.info("Node reachable", extra = {"detail": "", "cname": i['c_name'], "nname": i['name']})      
    except exc.OperationalError:  
        node_info_dict = {
            'cluster_name': i['c_name'],
            'node_id': i['id'],
            'node_name': i['name'],
            'reachable': 0,
            'elapsed_time': elapsed_time(start),
            'thread_connected': 0, 
            'thread_running': 0,
            'repl_ip_list': {}           
        } 
        if i['reachable'] == 1:
            logger.error("Node unreachable", extra = {"detail": "", "cname": i['c_name'], "nname": i['name']})              
        action= 'disable'   
    except AttributeError:   
        logger.debug("Connection got closed", extra = {"detail": "", "cluster": i['c_name'], "node": i['name']})    
    try:      
        node_info_str = json.dumps(node_info_dict) 
        #print(node_info_str)
        #print("======================================")
        s.settimeout(10)       
        s.connect(('127.0.0.1', 8283))
        s.send(node_info_str.encode())        
    except Exception:        
        #logger.error("Connection to the Core module failed", extra = {"detail": "", "cname": i['c_name'], "nname": i['name']})  
        return 1
    finally:
        s.close()      

    if i['access_level'] == 'rw':        
        subprocess.run('echo "{3} server {1}_writer/{2}" | {0}'.format(api_socket, i['c_name'], i['name'], action), stdout = subprocess.PIPE, stderr = subprocess.DEVNULL, shell=True) 
        subprocess.run('echo "{3} server {1}_reader/{2}" | {0}'.format(api_socket, i['c_name'], i['name'], action), stdout = subprocess.PIPE, stderr = subprocess.DEVNULL, shell=True) 
    elif i['access_level'] == 'w':
        subprocess.run('echo "{3} server {1}_writer/{2}" | {0}'.format(api_socket, i['c_name'], i['name'], action), stdout = subprocess.PIPE, stderr = subprocess.DEVNULL, shell=True) 
    elif i['access_level'] == 'r':
        subprocess.run('echo "{3} server {1}_reader/{2}" | {0}'.format(api_socket, i['c_name'], i['name'], action), stdout = subprocess.PIPE, stderr = subprocess.DEVNULL, shell=True) 
    
########################################
def reachable_check_old(gonarch_cred, backend_db, cluster_name):
    instance_l = backend_db.InstanceGetInstanceListFromCluster(cluster_name)    
    for i in instance_l:                      
        p1 = Process(target=is_reachable, args = (gonarch_cred, i))            
        p1.start()
    if 'p1' in locals():
        p1.join()
        p1.close()

def reachable_check_single(gonarch_cred, backend_db, cluster_name):
    instance_l = backend_db.InstanceGetInstanceListFromCluster(cluster_name)    
    for i in instance_l:                      
        is_reachable(gonarch_cred, i)


def reachable_check_multithread(gonarch_cred, backend_db, cluster_name):
    instance_list = backend_db.InstanceGetInstanceListFromCluster(cluster_name)
    threads = []
    
    for i in instance_list:
        thread = threading.Thread(target=is_reachable, args=(gonarch_cred, i))
        threads.append(thread)
        thread.start()
        
    for thread in threads:
        thread.join()
# Define the backend model class for db operations
backend_db = BackEndSqlModel(dbname)

while True:    
    cluster_l = backend_db.ClusterList()
    for c in cluster_l:               
        if c['maint_mode'] == 0:
            p0 = Process(target=reachable_check_single, args = (gonarch_cred, backend_db, c['name']))            
            p0.start()        
            p0.join()
            p0.close()            
        
    
