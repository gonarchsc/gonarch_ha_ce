#!/usr/bin/python3
import logging, yaml, sys, time, subprocess, re
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

while True:
    cluster_l = backend_db.ClusterList()   
    for c in cluster_l:               
        if c['maint_mode'] == 0:
            # Fetch replication status from backend db
            replica_result = backend_db.InstanceGetReplicaListFromCluster(c['name'])  
            # Fetch primary data
            primary = backend_db.InstanceGetPrimaryFromCluster(c['name'])             
            for replica in replica_result:
                print(replica['name'])
                # Instatiate the node class
                repl_node_obj = Node(c['name'], dbname, logger, config_file)
                conn_string = "{0}:{1}".format(replica['hostname'],replica['port']) 
                conn = repl_node_obj.Connect(conn_string)
                # Could not connect to the master
                if replica['io_thread_errorno'] == 1231:
                    print("1231 detected...")
                    if repl_node_obj.MedkitErr1231(conn) == 0:
                        logger.info("Replica connected to primary again.", extra = {"detail": "", "cluster": replica['c_name'], "node": replica['name']})
                    else:    
                        logger.info("Replica cannot connect to primary. Error persists to medkit actions.", extra = {"detail": "", "cluster": replica['c_name'], "node": replica['name']})
                # Can't find record in 'table_name'
                elif replica['sql_thread_errorno'] == 1032:
                    print("1032 detected...")                    
                    binlog_end_pos = re.findall(r'end_log_pos (\d*)', replica['sql_thread_error'])[0]
                    result = subprocess.getoutput(
                        "mysqlbinlog --read-from-remote-server --host={0} --port={1} --user={2} --password={3} --ssl-mode=required --base64-output=decode-rows --verbose --start-position={4} --stop-position={5} {6}".format(
                        primary['hostname'], 
                        primary['port'], 
                        config_file['mysql_credentials']['user'], 
                        config_file['mysql_credentials']['pass'], 
                        replica['binlog_pos'], 
                        binlog_end_pos, 
                        replica['binlog_file']
                        )
                    )
                    query_ori = ""
                    for line in result.splitlines():
                        if line.startswith("### "):
                            query_ori += str(line[3:])
                    query_ori = query_ori.strip()
                    if query_ori.startswith("UPDATE"):
                        tname = re.findall(r'UPDATE (.*) WHERE', query_ori)[0]
                        tname = tname.strip()
                        values_raw = re.findall(r'SET (.*)', query_ori)[0]
                        values = re.sub(r' +\@\d\=', ', ', values_raw)
                        values = values[2:]
                        query_final = "INSERT INTO {0} VALUES ({1});".format(tname, values) 
                        print (query_final)
                        backend_db.ClusterUpdateMaintMode(1,c['name'])   
                        repl_node_obj.SetReadOnly(conn, 0)  
                        # Stop replication
                        repl_node_obj.StopReplication(conn, replica['arch'])
                        # execute query
                        repl_node_obj.ExecuteInsert(conn, query_final)
                        # Start replication
                        repl_node_obj.StartReplication(conn, replica['arch'])
                        repl_node_obj.SetReadOnly(conn, 1)   
                        backend_db.ClusterUpdateMaintMode(0,c['name'])                  
  
    time.sleep(config_file['medkit']['refresh'])   