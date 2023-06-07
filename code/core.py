#!/usr/bin/python3
import logging, yaml, json, socket, sys

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
gonarch_cred = "{0}:{1}".format(config_file['mysql_credentials']['user'], config_file['mysql_credentials']['pass'])
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
    core_obj = Core(dbname, logger, node_info)   
    #print("{node_id}::{node_name}::{elapsed_time} -> {reachable}|{role}".format(**node_info))   

    ## Get HAproxy status info
    proxy_initial_dict = core_obj.GetProxyData()
     
    ## If the current node is the primary try to add new replicas
    if len(node_info['repl_ip_list']) > 0:
        # Add all new replicas in the list
        core_obj.DiscoverNewReplica(gonarch_cred)   

    ## Update instance, instance status & instance metrics in backend
    if node_info['reachable'] == 1:
        core_obj.UpdateNode()        
    core_obj.UpdateNodeStatus(gonarch_cred)
    core_obj.UpdateNodeMetric()
    
    ## Check if all nodes in cluster are down. IF so skip any further action
    if core_obj.StopIfAllNodesDown == 1:
        return 1

    ## Set read only based on node's role 
    if node_info['reachable'] == 1:           
        core_obj.ManageReadOnly(gonarch_cred)

    ## If primary dies perform a failover
    if node_info['reachable'] == 0:
        core_obj.ForcedFailover(gonarch_cred)
    
    ## Check if any replica is replicating from a non-primary node 
    if node_info['role'] == 'replica' and node_info['reachable'] == 1:
        core_obj.UpdateBrokenReplicaRole()
    
    ## Any missconfigured node will be fixed:
    if node_info['reachable'] == 1 and node_info['role'] == 'unknown':
        core_obj.RejoinNode(gonarch_cred)
    
    proxy_updated_dict = core_obj.GetProxyData() 
    # Get updated status of proxy data for primary
    core_obj.UpdateProxyCfg(proxy_initial_dict, proxy_updated_dict)    

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
            

        


'''
while True:       
    #start = time.time()    
    cluster_l = backend_db.ClusterList()  

    #proxy_initial_dict = GetProxyData(backend_db)  

    for c in cluster_l:          
        cluster_name = c['name']  
        if c['maint_mode'] == 0:
            p = Process(target=node_handler, args = (gonarch_cred, backend_db, cluster_name))            
            p.start()
        
            p.join()
            p.close()  
'''             