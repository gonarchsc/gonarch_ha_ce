#!/usr/bin/python3
import socket, re, sys
import sqlalchemy as db
from sqlalchemy import exc

# Load classes depending if PyInstaller is using them 
if getattr(sys, 'frozen', False):
   from classes_backend import BackEndSqlModel
elif __file__:
    from classes.classes_backend import BackEndSqlModel

class Node():
    def __init__(self, cname, dbname, logger, gonarch_cfg):        
        self.backend_db_obj = BackEndSqlModel(dbname)
        self.cluster_name = cname 
        self.logger_obj = logger       
        self.gonarch_cfg_dict = gonarch_cfg                                     

    def Connect(self, conn_string):  
        conn_credential = "{0}:{1}".format(self.gonarch_cfg_dict['mysql_credentials']['user'], self.gonarch_cfg_dict['mysql_credentials']['pass'])   
                  
        url = 'mysql://{0}@{1}/information_schema'.format(conn_credential, conn_string)            
        #for i in range(3):
        engine = db.create_engine(url, connect_args={'connect_timeout': 5}, pool_size=5, max_overflow=2, pool_timeout=10, pool_recycle=5)
        return engine.connect()
                
    def CloseConnection(self, conn):
        return conn.close()

###################################### Check module functions ################################################################# 
    def FetchTargetVars(self, conn):        
        query = "SELECT @@hostname, @@port, @@version, @@server_uuid, @@expire_logs_days, @@basedir, @@gtid_mode, @@read_only, @@log_bin_basename"               
        return conn.execute(query).first()
    
    def FetchTargetStatus(self, conn):        
        query = "SHOW GLOBAL STATUS WHERE Variable_name RLIKE '^(Threads_connected|Threads_running)$'"               
        result = conn.execute(query).fetchall()   
        result_dict = {}     
        for r in result:
            result_dict.update ({
                r['Variable_name']: r['Value']
            })            
        return result_dict
    
    def FetchTargetMasterStatus(self, conn):                              
        return conn.execute("show master status").first()._asdict()
    
    def GetReplicaIpList(self, conn):              
        query = "SELECT SUBSTRING_INDEX(HOST,':',1) AS replica_ip \
            FROM information_schema.PROCESSLIST \
            WHERE COMMAND in ('Binlog Dump', 'Binlog Dump GTID') \
            GROUP BY SUBSTRING_INDEX(HOST,':',1)"
        repl_ip = conn.execute(query).fetchall()
        return [u._asdict() for u in repl_ip]
    
    def FetchTargetSlaveStatus(self, conn):                             
        replica_status = conn.execute("show slave status").first()
        if replica_status:
            replica_status_dict = {
                'master_ip': socket.gethostbyname(replica_status['Master_Host']) if re.match('^\w', replica_status['Master_Host']) else replica_status['Master_Host'],
                'io_thread_running': replica_status['Slave_IO_Running'],
                'sql_thread_running': replica_status['Slave_SQL_Running'],
                'lag_sec': replica_status['Seconds_Behind_Master'],
                'io_thread_errorno': replica_status['Last_IO_Errno'],
                'io_thread_error': replica_status['Last_IO_Error'].replace("'", "").replace('"', ""),
                'sql_thread_errorno': replica_status['Last_SQL_Errno'],
                'sql_thread_error': replica_status['Last_SQL_Error'].replace("'", "").replace('"', ""),
                'gtid_coord': replica_status['Executed_Gtid_Set'],
                'io_binlog_file': replica_status['Master_Log_File'],
                'io_binlog_pos': replica_status['Read_Master_Log_Pos'],
                'sql_binlog_file': replica_status['Relay_Master_Log_File'],
                'sql_binlog_pos': replica_status['Exec_Master_Log_Pos'],
                'gtid_retrieved': replica_status['Retrieved_Gtid_Set'],
                'gtid_executed': replica_status['Executed_Gtid_Set']
            }             
        else:
            replica_status_dict = {} 
        return replica_status_dict
###############################################################################################################################
    def CheckReplLag(self,  node_info):
        if node_info['replication_mode'] == 'binlog' \
        and node_info['repl_status_dict']['io_thread_running'] == 'Yes' \
        and node_info['repl_status_dict']['sql_thread_running'] == 'Yes' \
        and node_info['repl_status_dict']['io_binlog_file'] == node_info['repl_status_dict']['sql_binlog_file'] \
        and node_info['repl_status_dict']['io_binlog_pos'] == node_info['repl_status_dict']['sql_binlog_pos']:
            return 0
        elif node_info['replication_mode'] ==  'gtid' \
        and node_info['repl_status_dict']['io_thread_running'] == 'Yes' \
        and node_info['repl_status_dict']['sql_thread_running'] == 'Yes' \
        and node_info['repl_status_dict']['gtid_retrieved'] == node_info['repl_status_dict']['gtid_executed']:
            return  0
        else:
            return 1

    def CheckOpenTrx(self, conn):
        query = "SELECT count(*) FROM information_schema.innodb_trx"        
        result = conn.execute(query).first()
        return None if result is None else result[0]

    def SetProxyStatus(self, node_info, max_allowed_lag):
        if node_info['role'] == 'primary':
            return 'up'
        elif node_info['role'] == 'replica' \
        and node_info['repl_status_dict']['io_thread_running'] == 'Yes' \
        and node_info['repl_status_dict']['sql_thread_running'] == 'Yes' \
        and node_info['repl_status_dict']['lag_sec'] <= max_allowed_lag:
            return 'up' 
        else:
            return 'down'

    def SetPromotable(self, node_info):  
        primary_node = self.backend_db_obj.InstanceGetNodeListFromRole(self.cluster_name, 'primary')  
        if len(primary_node) == 0:
            return
        primary_node = primary_node[0]        
        if node_info['role'] in ('primary', 'unknown', 'backup'):
            return 0        
        elif node_info['role'] == 'replica' \
        and node_info['arch'] == 'on-prem' \
        and node_info['promotion_rule'] == 'synced' \
        and node_info['repl_status_dict']['master_ip'] == primary_node['hostname'] \
        and node_info['repl_status_dict']['sql_thread_running'] == 'Yes' \
        and node_info['repl_status_dict']['lag_sec'] == 0 \
        and node_info['replication_mode'] == 'binlog' \
        and node_info['repl_status_dict']['io_binlog_file'] == node_info['repl_status_dict']['sql_binlog_file'] \
        and node_info['repl_status_dict']['io_binlog_pos'] == node_info['repl_status_dict']['sql_binlog_pos']:        
            return 1
        elif node_info['role'] == 'replica' \
        and node_info['arch'] == 'on-prem' \
        and node_info['promotion_rule'] == 'synced' \
        and node_info['repl_status_dict']['master_ip'] == primary_node['hostname'] \
        and node_info['repl_status_dict']['sql_thread_running'] == 'Yes' \
        and node_info['replication_mode'] == 'gtid' \
        and node_info['repl_status_dict']['lag_sec'] == 0:
        #and node_info['repl_status_dict']['gtid_retrieved'] == node_info['repl_status_dict']['gtid_executed']:
            return 1        
        elif node_info['role'] == 'replica' \
        and node_info['arch'] == 'on-prem' \
        and node_info['repl_status_dict']['master_ip'] == primary_node['hostname'] \
        and node_info['promotion_rule'] == 'available' \
        and node_info['reachable'] == 1:
            return 1
        else:
            return 0      

    def SetReadOnly(self, conn, flag):
        query = "SET GLOBAL read_only = {0}".format(flag)               
        return conn.execute(query)  
   
    def StopReplication(self, conn, arch): 
        if arch == 'rds':
            return conn.execute("CALL mysql.rds_stop_replication")
        elif arch == 'on-prem':
            return conn.execute("stop slave")
    
    def ResetReplication(self, conn, arch):
        if arch == 'rds':
            return conn.execute("CALL mysql.rds_reset_external_mastern")
        elif arch == 'on-prem':
           return conn.execute("reset slave all")
    
    def StartReplication(self, conn, arch):
        if arch == 'rds':
            return conn.execute("CALL mysql.rds_start_replication")
        elif arch == 'on-prem':
            return conn.execute("start slave")
    
    def GetMasterInfo(self, conn):        
        return conn.execute("show master status").first()
    
    def SetupReplication(self, conn, arch, replication_mode, data):
        # SSL encryption is disabled by default for now.   
        if arch == 'rds' and replication_mode == 'binlog':
            query = "CALL mysql.rds_set_external_master ('{hostname}', {port}, '{repl_user}', '{repl_pass}', '{binlog_file}', {binlog_pos}, 0)".format(**data)
        elif arch == 'rds' and replication_mode == 'gtid':
            query = "CALL mysql.rds_set_external_master_with_auto_position ('{hostname}', {port}, '{repl_user}', '{repl_pass}', 0, 0)".format(**data)
        elif arch == 'on-prem' and replication_mode == 'binlog':
            query = "CHANGE MASTER TO MASTER_HOST='{hostname}', MASTER_PORT={port}, MASTER_USER='{repl_user}', MASTER_PASSWORD='{repl_pass}', MASTER_LOG_FILE='{binlog_file}', MASTER_LOG_POS={binlog_pos}".format(**data)
        elif arch == 'on-prem' and replication_mode == 'gtid':
            query = "CHANGE MASTER TO MASTER_HOST='{hostname}', MASTER_PORT={port}, MASTER_USER='{repl_user}', MASTER_PASSWORD='{repl_pass}', MASTER_AUTO_POSITION = {gtid_auto_pos}".format(**data)
        return conn.execute(query) 

    def MedkitErr1231(self, conn):
        query = "STOP SLAVE IO_THREAD"
        conn.execute(query)
        query = "START SLAVE IO_THREAD"
        conn.execute(query)
        replica_status = conn.execute("show slave status").first()
        if replica_status['Slave_IO_Running'] == 'Yes':
            return 0
        else:
            return 1

    def ExecuteInsert(self, conn, query):        
        return conn.execute(query)
        