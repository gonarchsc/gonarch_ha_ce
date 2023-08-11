#!/usr/bin/python3
import logging, yaml, sys, time
from sqlalchemy import exc


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

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "cluster": "%(cname)s", "node": "%(nname)s", "error_no": "%(error_no)s", "msg": "%(message)s", "detail": "%(detail)s"}',
    handlers=[
        logging.FileHandler(config_file['medkit']['logging']['path'])        
    ]
)  
logger = logging.getLogger()
backend_db = BackEndSqlModel(dbname)
print("start")
while True:
    cluster_l = backend_db.ClusterList()   
    for c in cluster_l:               
        if c['maint_mode'] == 0:
            # Fetch replication status from backend db
            replica_result = backend_db.InstanceGetReplicaListFromCluster(c['name'])            
            for replica in replica_result:
                print(replica['name'])
                # Instatiate the node class
                repl_node_obj = Node(c['name'], dbname, logger, config_file)
                conn_string = "{0}:{1}".format(replica['hostname'],replica['port']) 
                conn = repl_node_obj.Connect(conn_string)
                # Could not connect to the master
                if replica['io_thread_errorno'] == 1231:
                    print("1231 detected...")
                    if repl_node_obj.err_1231() == 0:
                        logger.info("Replica connected to primary again.", extra = {"detail": "", "cluster": replica['c_name'], "node": replica['name']})
                    else:    
                        logger.info("Replica cannot connect to primary. Error persists to medkit actions.", extra = {"detail": "", "cluster": replica['c_name'], "node": replica['name']})
                # Can't find record in 'table_name'
                elif replica['sql_thread_errorno'] == 1032:
                    print("1032 detected...")
                    # Get binlog folder
                    mysql_vars = repl_node_obj.FetchTargetVars(conn)
                    binlog_folder = mysql_vars['@@log_bin_basename'].rsplit('/',1)[0]
                    print(binlog_folder)
                    # Get binlog file and coord where is failing
                    # fetch only that trx.
                    # Apply the trx into the right db.table
                    
             
            # Each interaction taken by medkit must be logged in detail for audit purposes     
    time.sleep(config_file['medkit']['refresh'])   
    
