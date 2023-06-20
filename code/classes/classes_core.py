#!/usr/bin/python3
from jinja2 import Template
import time, json, subprocess, sys

# Load classes depending if PyInstaller is using them 
if getattr(sys, 'frozen', False):   
    from classes_node import Node
    from classes_backend import BackEndSqlModel
elif __file__:       
    from classes.classes_backend import BackEndSqlModel
    from classes.classes_node import Node

class Core():
    def __init__(self, dbname, logger, ninfo):         
        self.backend_db_obj = BackEndSqlModel(dbname)
        self.backend_db_name = dbname
        self.cluster_name = ninfo['cluster_name']
        self.logger_obj = logger
        self.node_info = ninfo
###############################################################################################################################    
    def GetProxyData(self):  
        proxy_l = []     
        for listener in self.backend_db_obj.ProxyListenerGetAll():
            proxy_dict = {
                'port': listener['port'],
                'ng_id': listener['ng_id'],
                'name': listener['name']
            }
            node_l = []
            for node in self.backend_db_obj.InstanceGetProxyNodesByNodeGroupId(listener['ng_id']):        
                if node['proxy_status'] == 'up' and (node['access_level'] == 'r' or node['access_level'] == 'rw') and 'reader' in listener['name']:
                    node_l.append(node)
                elif node['proxy_status'] == 'up' and (node['access_level'] == 'w' or node['access_level'] == 'rw') and 'writer' in listener['name']:
                    node_l.append(node)             
            proxy_dict['node_l'] = node_l
            proxy_l.append(proxy_dict)        
        return proxy_l
###############################################################################################################################
    def UpdateProxyCfg(self, proxy_initial_dict, proxy_updated_dict, template): 
        if proxy_initial_dict == proxy_updated_dict:       
            return 0

        with open(template) as f:
            tmpl = Template(f.read())
            cfg =  tmpl.render(
                listener_data = proxy_updated_dict,                
            )
        with open("/etc/haproxy/haproxy.cfg", "w") as output_file:
            output_file.write(cfg)
        try:
            subprocess.call('sudo systemctl reload haproxy', shell=True)     
        except Exception as e:             
            self.logger_obj.error("Proxy layer update failed", extra = {"detail": "", "cluster": self.node_info['cluster_name'], "node": ""})             
        self.logger_obj.info("Proxy layer updated and reloaded", extra = {"detail": "", "cluster": self.node_info['cluster_name'], "node": ""}) 
###############################################################################################################################    
    def DiscoverNewReplica(self, gonarch_cred):
        # Loop each item of the replica_ip list
        for r in self.node_info['repl_ip_list']:
            # Check if that IP already exists in the backend
            if self.backend_db_obj.InstanceCheckExistingIp(r['replica_ip']) == 0:
                repl_node_obj = Node(self.node_info['cluster_name'], self.backend_db_name, self.logger_obj, gonarch_cred)
                conn_string = "{0}:{1}".format(r['replica_ip'], 3306)
                # If the connection is alive perform teh connection and fetch data
                try:
                    conn = repl_node_obj.Connect(conn_string)             
                    repl_node_info = repl_node_obj.FetchTargetVars(conn)                                
                    new_replica_dict = {
                        'node_group_id': self.backend_db_obj.NodeGroupGetFromInstance(self.node_info['node_id']),
                        'name': repl_node_info['@@hostname'],
                        'hostname': r['replica_ip'],
                        'port': repl_node_info['@@port'],
                        'version': repl_node_info['@@version'],
                        'uuid': repl_node_info['@@server_uuid'],
                        'arch': 'rds' if 'rds' in repl_node_info['@@basedir'] else 'on-prem',
                        'binlog_retention': repl_node_info['@@expire_logs_days'],
                        'role': 'replica',
                        'access_level': 'r' 
                    }
                    repl_result = self.backend_db_obj.InstanceAddNew(new_replica_dict)
                    self.backend_db_obj.InstanceStatusAddNew(repl_result.lastrowid)
                    self.backend_db_obj.InstanceMetricAddNew(repl_result.lastrowid)
                    self.logger_obj.info("New replica found and added to the backend", extra = {"detail": "", "cluster": self.node_info['cluster_name'], "node": repl_node_info['@@hostname']}) 
                    repl_node_obj.CloseConnection(conn)
                except Exception as e:
                    self.logger_obj.debug("Connection lost", extra = {"detail": "DiscoverNewReplica", "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']})                    
###############################################################################################################################
    def UpdateNode(self):           
        node_dict = {
            'node_id': self.node_info['node_id'],           
            'port': self.node_info['port'],
            'version': self.node_info['version'],
            'uuid':  self.node_info['server_uuid']   
        }          
        return self.backend_db_obj.InstanceUpdateNode(node_dict)
###############################################################################################################################
    def UpdateNodeStatus(self, gonarch_cred):  
        # The Role does nto get updated here. It happens across teh whole process
        node_obj = Node(self.node_info['cluster_name'], self.backend_db_name, self.logger_obj, gonarch_cred)   
        
        if self.node_info['reachable'] == 1:           
            node_status_dict = {
                'node_id': self.node_info['node_id'],
                'reachable': self.node_info['reachable'],
                'promotable': node_obj.SetPromotable(self.node_info),
                'replication_mode': self.node_info['replication_mode'],
                'proxy_status': node_obj.SetProxyStatus(self.node_info)
            }

            if self.node_info['role'] in ('primary', 'unknown'):                
                self.backend_db_obj.InstanceStatusUpdatePrimary(node_status_dict)
            elif self.node_info['role'] == 'replica':                               
                node_status_dict.update({
                    'io_thread_running': self.node_info['repl_status_dict']['io_thread_running'],
                    'sql_thread_running': self.node_info['repl_status_dict']['sql_thread_running'],
                    'io_thread_error': self.node_info['repl_status_dict']['io_thread_error'],
                    'sql_thread_error': self.node_info['repl_status_dict']['sql_thread_error'],
                    'lag_sec': self.node_info['repl_status_dict']['lag_sec'],
                    'binlog_file': self.node_info['repl_status_dict']['sql_binlog_file'],
                    'binlog_pos': self.node_info['repl_status_dict']['sql_binlog_pos'],
                    'io_thread_errorno': self.node_info['repl_status_dict']['io_thread_errorno'],
                    'sql_thread_errorno': self.node_info['repl_status_dict']['sql_thread_errorno'],                          
                    'gtid_coord': self.node_info['repl_status_dict']['gtid_coord']                 
                })
                self.backend_db_obj.InstanceStatusUpdateReplica(node_status_dict)
        elif self.node_info['reachable'] == 0:
            node_status_dict = {
                'node_id': self.node_info['node_id'],
                'reachable': self.node_info['reachable'],
                'promotable': 0,                
                'proxy_status': 'down'
            }
            self.backend_db_obj.InstanceStatusUpdateNonReachable(node_status_dict)  
###############################################################################################################################
    def UpdateNodeMetric(self):        
        self.backend_db_obj.InstanceMetricUpdate(self.node_info)
###############################################################################################################################
    def StopIfAllNodesDown(self):
        if self.backend_db_obj.InstanceStatusGetAllReachable(self, self.node_info['cluster_name'], 1) == 0:            
            return 1
        else:
            return 0  
###############################################################################################################################
    def ManageReadOnly(self, gonarch_cred):        
        node_obj = Node(self.node_info['cluster_name'], self.backend_db_name, self.logger_obj, gonarch_cred) 
        conn_string = "{0}:{1}".format(self.backend_db_obj.InstanceGetIp(self.node_info['node_id']), self.node_info['port'])         
        try:     
            conn = node_obj.Connect(conn_string)
            if self.node_info['role'] == 'primary' and self.node_info['read_only'] == 1:
                node_obj.SetReadOnly(conn, 0)
                self.logger_obj.info("Read only and Super read only disabled", extra = {"detail": "", "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']})
            elif self.node_info['role'] != 'primary' and self.node_info['read_only'] == 0:
                node_obj.SetReadOnly(conn, 1)
                self.logger_obj.info("Read only and Super read only enabled", extra = {"detail": "", "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']})
            node_obj.CloseConnection(conn)
        except Exception as e:
             self.logger_obj.debug("Connection lost", extra = {"detail": "ManageReadOnly", "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']})
###############################################################################################################################
    def UpdateBrokenReplicaRole(self):
        # Get current primary information
        primary_node = self.backend_db_obj.InstanceGetNodeListFromRole(self.cluster_name, 'primary')   
        primary_node = primary_node[0]  
        # Check if this replica is replicating fine, in sync, with a wrong primary.
        if self.node_info['repl_status_dict']['master_ip'] != primary_node['hostname'] \
        and self.node_info['repl_status_dict']['io_thread_running'] == 'Yes' \
        and self.node_info['repl_status_dict']['sql_thread_running'] == 'Yes' \
        and self.node_info['replication_mode'] == 'binlog' \
        and self.node_info['repl_status_dict']['io_binlog_file'] == self.node_info['repl_status_dict']['sql_binlog_file'] \
        and self.node_info['repl_status_dict']['io_binlog_pos'] == self.node_info['repl_status_dict']['sql_binlog_pos'] \
        and self.node_info['repl_status_dict']['lag_sec'] == 0:
            # Update the synced replica as unknown. The RejoinNode process will fix it
            update_role_dict = {
                'role': 'unknown',
                'access_level': 'na',
                'node_id': self.node_info['node_id']
            }
            self.backend_db_obj.InstanceUpdateRole(update_role_dict)
        elif self.node_info['repl_status_dict']['master_ip'] != primary_node['hostname'] \
        and self.node_info['repl_status_dict']['io_thread_running'] == 'Yes' \
        and self.node_info['repl_status_dict']['sql_thread_running'] == 'Yes' \
        and self.node_info['replication_mode'] == 'gtid' \
        and self.node_info['repl_status_dict']['lag_sec'] == 0:
            # Update the synced replica as unknown. The RejoinNode process will fix it
            update_role_dict = {
                'role': 'unknown',
                'access_level': 'na',
                'node_id': self.node_info['node_id']
            }
            self.backend_db_obj.InstanceUpdateRole(update_role_dict)
###############################################################################################################################
    def ForcedFailover(self, gonarch_cred): 
        # A few facts awhen this gets triggered:
        # - The coming primary starts as a replica so read_only = OFF
        # - As soon as reachable = 0 for primary, it gets disabled from Proxy so no traffic can go there
        # - This function sets maint_mode = 1 so no updates will happen in check.py. writer will remain disabled until this failover is finished.
        # - In this case, the node_info is the dead master
               
        # If the dead node is not a primary skip
        if self.backend_db_obj.InstanceGetRole(self.node_info['node_id']) != 'primary':
            return

        self.logger_obj.info("Failover activity started", extra = {"detail": "Primary node is not reachable anymore", "cluster": self.node_info['cluster_name'], "node": ""})
        # Enable maint_mode in this cluster 
        self.backend_db_obj.ClusterUpdateMaintMode(1, self.cluster_name)
        self.logger_obj.info("Maintenance mode enabled", extra = {"detail": "This happens automatically. Gonarch won't update this cluster until maintenance mode is disabled.", "cluster": self.node_info['cluster_name'], "node": ""})
        # Get a promotable replica
        # Promotable status is being updated in step #3 in Core.py  
        cp = self.backend_db_obj.InstanceStatusGetPromotableReplica(self.cluster_name)              
        if cp:            
            cp_node_obj = Node(self.node_info['cluster_name'], self.backend_db_name, self.logger_obj, gonarch_cred)
            conn_string = "{0}:{1}".format(cp['hostname'], cp['port'])             
            try:
                conn = cp_node_obj.Connect(conn_string)
                # Wait until there is coming transactions into this server before to stop replication
                while cp_node_obj.CheckOpenTrx(conn) > 0:
                    time.sleep(1)                       
                # Stop replication in coming primary
                cp_node_obj.StopReplication(conn, cp['arch'])
                # Reset replication in coming primary
                cp_node_obj.ResetReplication(conn, cp['arch'])
                # Get replication details from coming primary
                cp_master_info = cp_node_obj.GetMasterInfo(conn)
                # Close the connection
                cp_node_obj.CloseConnection(conn)
            except Exception as e:
                self.logger_obj.warning("The selected node for promotion went down", extra = {"detail": "The node selected to become the new primary went down during the promotion process. The failover process will retry again with another node, if there is any other node available.", "cluster": self.node_info['cluster_name'], "node": ""})
                self.logger_obj.debug("Connection got closed", extra = {"detail": "ForcedFailover", "cluster": self.node_info['cluster_name'], "node": cp['name']})
                return 
            # Build up the dictionary for replication setup
            repl_setup_dict = {
                'hostname': cp['hostname'],
                'port': cp['port'],
                'repl_user': cp['huser'],
                'repl_pass': cp['hpass']
            }
            if cp['replication_mode'] == 'binlog':
                repl_setup_dict.update({
                    'binlog_file': cp_master_info['File'],
                    'binlog_pos': cp_master_info['Position']
                })
            else:
                repl_setup_dict.update({'gtid_auto_pos': 0})          
           
            # Update the failed primary as former-primary in backend DB 
            failed_primary = self.backend_db_obj.InstanceGetNodeListFromRole(self.node_info['cluster_name'], 'primary')
            update_role_dict = {
                'role': 'unknown',
                'access_level': 'na',
                'node_id': failed_primary[0]['id']
            }
            self.backend_db_obj.InstanceUpdateRole(update_role_dict)
            # Update coming primary as primary in backend DB
            update_role_dict = {
                'role': 'primary',
                'access_level': 'rw',
                'node_id': cp['id']
            }
            self.backend_db_obj.InstanceUpdateRole(update_role_dict)
            # Fetch failover coordinates for each dead node from the coming primary  
            # Node is in read only mode during this operation
            for dead_node in self.backend_db_obj.InstanceStatusGetDeadNodeList(self.cluster_name):
                failover_dict = {
                    'origin_instance_id' : dead_node['id'],
                    'primary_id': cp['id']
                }
                if cp['replication_mode'] == 'binlog':
                    failover_dict.update({'coord_set': "{0}:{1}".format(cp_master_info['File'], cp_master_info['Position'])})
                elif cp['replication_mode'] == 'gtid':
                    failover_dict.update({'coord_set': cp_master_info['Executed_Gtid_Set']})

                self.backend_db_obj.PromLedgerAddNew(failover_dict)           
            
            self.logger_obj.info("Node selected to become new primary", extra = {"detail": "According to this cluster's promotion rules, this node meets the requirements to become primary.", "cluster": self.node_info['cluster_name'], "node": cp['name']})               

            # Loop in all reachable replicas to apply the new master info
            for repl_node in self.backend_db_obj.InstanceGetReplicaListFromCluster(self.cluster_name):
                # Connect to replica
                repl_node_obj = Node(self.node_info['cluster_name'], self.backend_db_name, self.logger_obj, gonarch_cred)
                conn_string = "{0}:{1}".format(repl_node['hostname'], repl_node['port']) 
                
                conn = repl_node_obj.Connect(conn_string)
                # Stop replication in replicas
                repl_node_obj.StopReplication(conn, repl_node['arch'])
                # Setup replication
                repl_node_obj.SetupReplication(conn, repl_node['arch'], repl_node['replication_mode'], repl_setup_dict)
                # Start replication
                repl_node_obj.StartReplication(conn, repl_node['arch'])
                # Close the connection
                repl_node_obj.CloseConnection(conn)
                self.logger_obj.info("Replica reconfigured to sync with new primary", extra = {"detail": "Replication will start soon and this node will be serving read traffic again.", "cluster": self.node_info['cluster_name'], "node": repl_node['name']})                

            self.backend_db_obj.ClusterUpdateMaintMode(0, self.cluster_name)
            self.logger_obj.info("Maintenance mode disabled", extra = {"detail": "The failover activity is completed and now Gonarch is actively monitoring this cluster again.", "cluster": self.node_info['cluster_name'], "node": ""})
            self.logger_obj.info("Failover completed. New primary accepting traffic", extra = {"detail": "", "cluster": self.node_info['cluster_name'], "node": cp['name']})                
        else:
            # NO promotable replica available.
            self.logger_obj.warning("There are no promotable replicas", extra = {"detail": "Review your cluster's promotion rules", "cluster": self.node_info['cluster_name'], "node": ""})
            self.backend_db_obj.ClusterUpdateMaintMode(0, self.cluster_name)
            self.logger_obj.info("Maintenance mode disabled", extra = {"detail": "Failover activity failed and now Gonarch is actively monitoring this cluster again.", "cluster": self.node_info['cluster_name'], "node": ""})     
###############################################################################################################################
    def RejoinNode(self, gonarch_cred):
        self.logger_obj.info("A node in unknown status is online", extra = {"detail": "Starting the rejoining process.", "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']})
        # Get the current primary ID
        primary_node = self.backend_db_obj.InstanceGetNodeListFromRole(self.cluster_name, 'primary') 
                  
        if len(primary_node) == 0:
            self.logger_obj.error("Primary node is not active in this moment. Rejoining job aborted", extra = {"detail": "Gonarch will retry again once there is a primary defined.", "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']})
            return        
        primary_node = primary_node[0] 
         
        # Fetch the coordinates from failover json
        for coord in self.backend_db_obj.PromLedgerFetchOrderedEvent(self.node_info['node_id']):
            # First of all  check if the target primary is alive
            if coord['reachable'] == 0:
                self.logger_obj.warning("The candidate node needs to fetch data from a dead node before to rejoin. This operation will remain on hold until that node is reachable again", extra = {"detail": "{0} has data missing in this node and must be fetched before to rejoin the cluster".format(coord['name']), "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']}) 
                return

            if coord['primary_id'] == primary_node['id']:
                # The primary from ledger is the current one
                binlog_coord_split = coord['coord_set'].split(":")                
                # Build up the dictionary for replication setup
                repl_setup_dict = {
                    'hostname': primary_node['hostname'],
                    'port': primary_node['port'],
                    'repl_user': primary_node['huser'],
                    'repl_pass': primary_node['hpass']
                }
                if primary_node['replication_mode'] == 'binlog':
                    repl_setup_dict.update({
                        'binlog_file': binlog_coord_split[0],
                        'binlog_pos': binlog_coord_split[1]
                    })
                else:
                    repl_setup_dict.update({'gtid_auto_pos': 0})
            else:
                # The primary from ledger is NOT the current one
                binlog_coord_split = coord['coord_set'].split(":")                
                # Build up the dictionary for replication setup
                repl_setup_dict = {
                    'hostname': coord['hostname'],
                    'port': coord['port'],
                    'repl_user': primary_node['huser'],
                    'repl_pass': primary_node['hpass']
                }
                if primary_node['replication_mode'] == 'binlog':
                    repl_setup_dict.update({
                        'binlog_file': binlog_coord_split[0],
                        'binlog_pos': binlog_coord_split[1]
                    })
                else:
                    repl_setup_dict.update({'gtid_auto_pos': 0})
        
            # Connect to replica
            rejoin_node_obj = Node(self.node_info['cluster_name'], self.backend_db_name, self.logger_obj, gonarch_cred)
            conn_string = "{0}:{1}".format(self.backend_db_obj.InstanceGetIp(self.node_info['node_id']), self.node_info['port']) 
            try:            
                conn = rejoin_node_obj.Connect(conn_string)
                # If replication is in sync with current node then try to connect to the next
                repl_status = rejoin_node_obj.FetchTargetSlaveStatus(conn)                                
                if len(repl_status) == 0:
                    repl_status['lag_sec'] = None
                if repl_status['lag_sec'] == 0  or repl_status['lag_sec'] == None:
                    # Stop replication in replica
                    rejoin_node_obj.StopReplication(conn, primary_node['arch'])
                    # Setup replication
                    rejoin_node_obj.SetupReplication(conn, primary_node['arch'], primary_node['replication_mode'], repl_setup_dict)
                    # Start replication
                    rejoin_node_obj.StartReplication(conn, primary_node['arch'])
                    # Delete this entry in the ledger as replication is working now for this given primary
                    self.backend_db_obj.PromLedgerDeleteEntry(coord['id'])
                    self.logger_obj.info("Node getting updates from {0}".format(coord['name']), extra = {"detail": "", "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']})                       
                else:
                    # Close the connection
                    rejoin_node_obj.CloseConnection(conn)
                    return 
                # Close the connection
                rejoin_node_obj.CloseConnection(conn)
            except:                 
                self.logger_obj.error("The connection with this node is lost. Rejoining process aborted", extra = {"detail": "Gonarch will retry again once the node is online again.", "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']}) 
                return        
        # Update coming primary as primary in backend DB
        update_role_dict = {
            'role': 'replica',
            'access_level': 'r',
            'node_id': self.node_info['node_id']
        }
        self.backend_db_obj.InstanceUpdateRole(update_role_dict)
        self.logger_obj.info("Rejoining process finished", extra = {"detail": "", "cluster": self.node_info['cluster_name'], "node": self.node_info['node_name']})
        
               

