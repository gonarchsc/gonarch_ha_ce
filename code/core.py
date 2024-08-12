#!/usr/bin/python3
import logging, yaml, json, socket, sys, getpass

# core.py only runs as ROOT
if getpass.getuser() != 'root':
    print("This module can be executed ONLY as root")
    exit()

# Load classes depending if PyInstaller is using them 
if getattr(sys, 'frozen', False):   
    from classes_core import Core
    from classes_backend import BackEndSqlModel
elif __file__:       
    from classes.classes_backend import BackEndSqlModel
    from classes.classes_core import Core

####### Config section ####### 
with open(r'/etc/gonarch.conf') as file:
    config_file = yaml.load(file, Loader=yaml.FullLoader)

dbname = config_file['workspace']['backend_dbname']
haproxy_template = config_file['proxy']['template_path']

# Define the backend model class for db operations
backend_db = BackEndSqlModel(dbname)
####### Logging section ####### 
logging.basicConfig(
    level=logging.INFO,    
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "cluster": "%(cluster)s", "node": "%(node)s", "msg": "%(message)s", "detail": "%(detail)s"}',
    handlers=[
        logging.FileHandler(config_file['core']['logging']['path'])               
    ]
)  
logger = logging.getLogger()
########################################
def core_handler(node_info):     

    node_info['role'] = backend_db.InstanceGetRole(node_info['node_id'])
    core_obj = Core(dbname, logger, node_info, config_file)   
    #print("{node_id}::{node_name}::{elapsed_time} -> {reachable}|{role}".format(**node_info))   

    ## Get HAproxy status info
    proxy_initial_dict = core_obj.GetProxyData()
     
    ## If the current node is the primary try to add new replicas
    if len(node_info['repl_ip_list']) > 0:
        # Add all new replicas in the list
        core_obj.DiscoverNewReplica()   

    ## Update instance, instance status & instance metrics in backend
    if node_info['reachable'] == 1:
        core_obj.UpdateNode()        
    core_obj.UpdateNodeStatus()
    core_obj.UpdateNodeMetric()
    
    ## Check if all nodes in cluster are down. IF so skip any further action
    if core_obj.StopIfAllNodesDown == 1:
        return 1

    ## Set read only based on node's role 
    if node_info['reachable'] == 1:           
        core_obj.ManageReadOnly()

    ## If primary dies perform a failover
    if node_info['reachable'] == 0:
        core_obj.ForcedFailover()
    
    ## Check if any replica is replicating from a non-primary node 
    if node_info['role'] == 'replica' and node_info['reachable'] == 1:
        core_obj.UpdateBrokenReplicaRole()
    
    ## Any missconfigured node will be fixed:
    if node_info['reachable'] == 1 and node_info['role'] == 'unknown':
        core_obj.RejoinNode()
    
    proxy_updated_dict = core_obj.GetProxyData() 
    # Get updated status of proxy data for primary
    core_obj.UpdateProxyCfg(proxy_initial_dict, proxy_updated_dict, haproxy_template)    

    return 0
    
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('127.0.0.1', 8283))
s.listen(1)
while True:   
        clientsocket, addr = s.accept()        
        data = clientsocket.recv(2048)
        if data:           
            node_info = json.loads(data.decode())
            #print(node_info) 
            core_handler(node_info)
            #print("===========================================================")
            
