import sqlalchemy as db

class BackEndSqlModel():
    def __init__(self, dbname):            
        self.backend_engine = db.create_engine('sqlite:///{0}'.format(dbname))  
             
######### CLUSTER #########    
    def ClusterCheckExisting(self, cluster_name):
        query = "SELECT COUNT(*) \
            FROM cluster \
            WHERE name = '{0}'".format(cluster_name)
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0]  
        
    def ClusterAddNew(self, data):
        query = "INSERT INTO cluster (name, huser, hpass,  promotion_rule) \
            VALUES ('{0}', '{1}', '{2}', '{3}')".format(
            data['name'],
            data['huser'],
            data['hpass'],            
            data['promotion_rule']
        )            
        return self.backend_engine.execute(query)  
    
    def ClusterList(self):
        return self.backend_engine.execute("SELECT * FROM cluster ORDER BY name").fetchall()            
    
    def ClusterInfo(self, cluster_name):
        return self.backend_engine.execute("SELECT * FROM cluster where name = '{0}'".format(cluster_name)).first()        
          
    def ClusterUpdateMaintMode(self, flag, cluster_name):
        query = "UPDATE cluster SET maint_mode = {0} WHERE name = '{1}'".format(flag, cluster_name)
        return self.backend_engine.execute(query)

    def ClusterUpdateMaxAllowedLag(self, flag, cluster_name):
        query = "UPDATE cluster SET proxy_max_allowed_lag = {0} WHERE name = '{1}'".format(flag, cluster_name)
        return self.backend_engine.execute(query)
    
    def ClusterUpdatePromotionRule(self, flag, cluster_name):
        query = "UPDATE cluster SET promotion_rule = '{0}' WHERE name = '{1}'".format(flag, cluster_name)
        return self.backend_engine.execute(query)
    
    def ClusterRemoveFull(self, cluster_name):
        query = "DELETE FROM cluster WHERE name = '{0}'".format(cluster_name)
        self.backend_engine.execute(query)
        query = "DELETE FROM node_group \
            WHERE id IN (SELECT id FROM node_group ng \
            LEFT JOIN cluster c \
            ON c.name = ng.cluster_id \
            WHERE c.name IS NULL)"
        self.backend_engine.execute(query)
        query = "DELETE FROM instance \
            WHERE id IN (SELECT i.id FROM instance i \
            LEFT JOIN node_group ng \
            ON i.node_group_id = ng.id \
            WHERE ng.id IS NULL)"
        self.backend_engine.execute(query)
        query = "DELETE FROM instance_status \
            WHERE instance_id IN (SELECT ist.instance_id FROM instance_status ist \
            LEFT JOIN instance i \
            ON i.id = ist.instance_id \
            WHERE i.id IS NULL)"
        self.backend_engine.execute(query)
        query = "DELETE FROM instance_metric \
            WHERE instance_id IN (SELECT i.id FROM instance_metric im \
            LEFT JOIN instance i \
            ON i.id = im.instance_id \
            WHERE i.id IS NULL)"
        self.backend_engine.execute(query)
        query = "DELETE FROM proxy_listener \
            WHERE ng_id IN (SELECT pl.ng_id FROM proxy_listener pl\
            LEFT JOIN node_group ng \
            ON pl.ng_id = ng.id \
            WHERE ng.id IS NULL)"
        self.backend_engine.execute(query)
        return 0
######### INSTANCE #########     
    def InstanceAddNew(self, data):
        query = "INSERT INTO instance (node_group_id, name, hostname, port, version, uuid, arch, binlog_retention, access_level, role) \
            VALUES ('{node_group_id}', '{name}', '{hostname}', '{port}', '{version}', '{uuid}', '{arch}', '{binlog_retention}', '{access_level}', '{role}') \
            ".format(**data)             
        return self.backend_engine.execute(query)
    
    def InstanceUpdateRole(self, data):
        query = "UPDATE instance SET role = '{role}', access_level = '{access_level}' WHERE id = {node_id}".format(**data)
        return self.backend_engine.execute(query) 
    
    def InstanceUpdateNode(self, data):
        query = "UPDATE instance SET \
            port = '{port}', version = '{version}', uuid = '{uuid}', \
            access_level = CASE WHEN role = 'replica' THEN 'r' ELSE access_level END WHERE id = {node_id}".format(**data)
        return self.backend_engine.execute(query)   

    def InstanceCheckExistingIp(self, ip):
        query = "SELECT count(*) \
            FROM instance \
            WHERE hostname = '{0}'".format(ip)
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0]  

    def InstanceGetIp(self, id):
        query = "SELECT hostname \
            FROM instance \
            WHERE id = {0}".format(id)
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0]   
    
    def InstanceGetRole(self, id):
        query = "SELECT role \
            FROM instance \
            WHERE id = {0}".format(id)
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0]  

    def InstanceGetNodeListFromRole(self, cluster_name, role):
        query = "SELECT i.id, i.name, i.hostname, i.port, i.arch, c.huser, c.hpass, ist.replication_mode, ist.binlog_file, ist.binlog_pos, c.maint_mode \
            FROM instance i \
            LEFT JOIN instance_status ist \
                ON ist.instance_id = i.id \
            INNER JOIN node_group ng \
                ON ng.id = i.node_group_id \
            INNER JOIN cluster c \
                ON c.name = ng.cluster_id \
            WHERE i.role = '{1}' \
            AND ng.cluster_id = '{0}'".format(cluster_name, role)
        return self.backend_engine.execute(query).fetchall()      

    def InstanceGetInstanceListFromCluster(self, cluster):
        query = "SELECT i.*, c.name 'c_name', c.created_at 'c_created', c.huser, c.hpass, c.promotion_rule, c.maint_mode, c.proxy_max_allowed_lag, ist.*, ng.name 'ng_name', im.thread_connected, im.thread_running, \
        (SELECT pl.name || ':' || pl.port FROM  proxy_listener pl  WHERE pl.ng_id = ng.id AND name like '%writer') 'writer_endpoint', \
        (SELECT pl.name || ':' || pl.port FROM  proxy_listener pl  WHERE pl.ng_id = ng.id AND name like '%reader') 'reader_endpoint' \
            FROM instance i\
            INNER JOIN node_group ng \
                ON i.node_group_id = ng.id \
            INNER JOIN cluster c \
                ON c.name = ng.cluster_id \
            LEFT JOIN instance_status ist \
                ON ist.instance_id = i.id \
            LEFT JOIN instance_metric im \
                ON im.id = i.id \
            WHERE ng.cluster_id = '{0}' \
            ORDER BY CASE role \
                WHEN 'primary' THEN 1 \
                WHEN 'replica' THEN 2 \
            END, i.name ASC".format(cluster)
        return self.backend_engine.execute(query).fetchall()
        
################################    
    def InstanceRemove(self, id):
        query = "DELETE FROM instance_status WHERE instance_id = {0}".format(id)
        self.backend_engine.execute(query)
        query = "DELETE FROM instance WHERE id = {0}".format(id)
        self.backend_engine.execute(query)
        return 0
    
    def InstanceParseServerUuid(self, uuid):
        query = "SELECT name \
            FROM instance \
            WHERE uuid = '{0}'\
        ".format(uuid)
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0] 
    
    def InstanceGetPrimaryFromCluster(self, cluster):
        query = "SELECT i.*, c.huser, c.hpass, c.maint_mode \
            FROM instance i\
            INNER JOIN node_group ng \
                ON i.node_group_id = ng.id \
            INNER JOIN cluster c \
                ON c.name = ng.cluster_id \
            WHERE ng.cluster_id = '{0}' \
            AND i.role = 'primary'".format(cluster)
        return self.backend_engine.execute(query).first()
    
    def InstanceGetReplicaListFromCluster(self, cluster):
        query = "SELECT i.*, c.name 'c_name', c.huser, c.hpass, c.maint_mode, ist.replication_mode, ist.io_thread_errorno, ist.sql_thread_errorno, ist.reachable \
            FROM instance i\
            INNER JOIN node_group ng \
                ON i.node_group_id = ng.id \
            INNER JOIN cluster c \
                ON c.name = ng.cluster_id \
            INNER JOIN instance_status ist \
                ON ist.instance_id = i.id \
            WHERE ng.cluster_id = '{0}' \
            AND i.role = 'replica' \
            AND ist.reachable = 1".format(cluster)
        return self.backend_engine.execute(query).fetchall()

    def InstanceGetBackupListFromCluster(self, cluster):
        query = "SELECT i.*, c.name 'c_name', c.huser, c.hpass, c.maint_mode, ist.replication_mode, ist.reachable \
            FROM instance i\
            INNER JOIN node_group ng \
                ON i.node_group_id = ng.id \
            INNER JOIN cluster c \
                ON c.name = ng.cluster_id \
            INNER JOIN instance_status ist \
                ON ist.instance_id = i.id \
            WHERE ng.cluster_id = '{0}' \
            AND i.role = 'backup' \
            AND ist.reachable = 1".format(cluster)
        return self.backend_engine.execute(query).fetchall()
    
    def InstanceGetProxyNodesByNodeGroupId(self, ng_id):
        query = "SELECT i.name, i.hostname, i.port, i.access_level, ist.reachable, ist.proxy_status \
            FROM instance i \
            INNER JOIN instance_status ist \
            ON i.id = ist.instance_id \
            WHERE i.node_group_id = {0}".format(ng_id)
        return self.backend_engine.execute(query).fetchall() 
    
        
######### NODE GROUP #########    
    def NodeGroupAddNew(self, data):
        query = "INSERT INTO node_group (cluster_id, name) \
            VALUES ('{0}', '{1}')".format(
            data['cluster_id'],
            data['name']
        )             
        return self.backend_engine.execute(query)
       
    def NodeGroupGetFromInstance(self, instance_id):
        query = "SELECT node_group_id FROM instance WHERE id = {0}".format(instance_id)
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0]
  
    def InstanceGetInstanceListFromClusterForGui(self, cluster):
        query = "SELECT i.*, c.name 'c_name', c.created_at 'c_created', c.huser, c.hpass, c.promotion_rule, c.maint_mode, ist.*, ng.name 'ng_name', \
            CASE i.access_level \
            when 'rw' then 'Read and write' \
            when 'r' then 'Read only' \
            when 'w' then 'Write only' \
            END 'i_access_level', \
            CASE i.role \
            when 'primary' then 'Primary' \
            when 'replica' then 'Replica' \
            when 'former-primary' then 'Former primary' \
            END 'i_role', \
            CASE i.arch \
            when 'on-prem' then 'On premises' \
            when 'rds' then 'AWS RDS' \
            when 'aurora' then 'AWS Aurora' \
            END 'i_platform', \
            case ist.replication_mode \
            when 'binlog' then 'Binlog' \
            when 'gtid' then 'GTID' \
            END 'ist_repl_mode', \
            CASE \
            when role = 'primary' then 'na' \
            when (io_thread_running = 'No' and sql_thread_running = 'No') and (io_thread_error != '' or sql_thread_error != '') then 'broken' \
            when (io_thread_running = 'No' and sql_thread_running = 'No') and (io_thread_error = '' and sql_thread_error = '') then 'stopped' \
            when (io_thread_running = 'Yes' and sql_thread_running = 'Yes' and lag_sec = 0) then 'synced' \
            when (io_thread_running = 'Yes' and sql_thread_running = 'Yes' and lag_sec > 0) then 'delayed' \
            END 'i_repl_status', \
            case  \
            when role = 'primary' then 'na' \
            when arch = 'rds' then 'Rds' \
            when ist.promotable = 1 then 'Yes' \
            when ist.promotable = 0 then 'No' \
            END 'ist_promotable', \
            im.thread_connected, \
            im.thread_running, \
            (SELECT pl.port FROM proxy_listener pl WHERE pl.ng_id = ng.id AND name LIKE '%writer') 'port_writer', \
            (SELECT  pl.port FROM proxy_listener pl WHERE pl.ng_id = ng.id AND name LIKE '%reader') 'port_reader'\
            FROM instance i\
            INNER JOIN node_group ng \
                ON i.node_group_id = ng.id \
            INNER JOIN cluster c \
                ON c.name = ng.cluster_id \
            LEFT JOIN instance_status ist \
                ON ist.instance_id = i.id \
            LEFT JOIN instance_metric im \
                ON im.instance_id = i.id \
            WHERE ng.cluster_id = '{0}' \
            ORDER BY CASE role \
                WHEN 'primary' THEN 1 \
                WHEN 'replica' THEN 2 \
                WHEN 'broken-replica' THEN 3 \
                WHEN 'unknown' THEN 3 \
            END".format(cluster)
        return self.backend_engine.execute(query).fetchall()
        
######### INSTANCE STATUS #########     
    def InstanceStatusUpdateReplica(self, data):
        query = "UPDATE instance_status SET reachable = {reachable}, \
            io_thread_running = '{io_thread_running}', \
            sql_thread_running = '{sql_thread_running}', \
            io_thread_error = '{io_thread_error}', \
            sql_thread_error = '{sql_thread_error}', \
            lag_sec = '{lag_sec}', \
            io_thread_errorno = {io_thread_errorno}, \
            sql_thread_errorno = {sql_thread_errorno}, \
            binlog_file = '{binlog_file}',\
            binlog_pos = {binlog_pos}, \
            gtid_coord = '{gtid_coord}', \
            replication_mode = '{replication_mode}', \
            promotable = {promotable}, \
            proxy_status = '{proxy_status}' \
            WHERE instance_id = {node_id} \
            ".format(**data)             
        return self.backend_engine.execute(query)

    def InstanceStatusUpdatePrimary(self, data):
        query = "UPDATE instance_status SET \
            reachable = '{reachable}', \
            promotable = {promotable}, \
            replication_mode = '{replication_mode}', \
            proxy_status = '{proxy_status}' \
            WHERE instance_id = {node_id} \
            ".format(**data)      
        return self.backend_engine.execute(query)
    
    def InstanceStatusUpdateNonReachable(self, data):
        query = "UPDATE instance_status SET \
            reachable = '{reachable}', \
            promotable = {promotable}, \
            proxy_status = '{proxy_status}' \
            WHERE instance_id = {node_id} \
            ".format(**data)      
        return self.backend_engine.execute(query)
    
    def InstanceStatusAddNew(self, node_id):
        query = "INSERT INTO instance_status (instance_id) VALUES ({0})".format(node_id)             
        return self.backend_engine.execute(query)
    
    def InstanceStatusGetAllReachable(self, cluster_name, flag):
        query = "SELECT count(*) \
            FROM instance_status ist \
            INNER JOIN instance i \
            ON i.id = ist.instance_id \
            INNER JOIN node_group ng \
            ON ng.id = i.node_group_id \
            WHERE reachable = {1} \
            and cluster_id = '{0}'".format(cluster_name, flag)        
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0]
    
    def InstanceStatusGetValidNode(self, cluster_name):
        query = "SELECT count(*) \
            FROM instance_status ist \
            INNER JOIN instance i \
            ON i.id = ist.instance_id \
            INNER JOIN node_group ng \
            ON ng.id = i.node_group_id \
            WHERE reachable = 1 \
            AND i.role != 'unknown' \
            and cluster_id = '{0}'".format(cluster_name)        
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0]
    
    def InstanceStatusGetDeadNodeList(self, cluster_name):
        query = "SELECT i.id \
            FROM instance_status ist \
            INNER JOIN instance i \
            ON i.id = ist.instance_id \
            INNER JOIN node_group ng \
            ON ng.id = i.node_group_id \
            WHERE reachable = 0 \
            and cluster_id = '{0}'".format(cluster_name)
        return self.backend_engine.execute(query).fetchall() 
    
    def InstanceStatusGetPromotableReplica(self, cluster_name):
        query = "SELECT i.id, i.name, i.hostname, i.port, i.arch, c.huser, c.hpass, ist.replication_mode \
            FROM instance_status ist \
            INNER JOIN instance i \
                ON i.id = ist.instance_id \
            INNER JOIN node_group ng \
            ON ng.id = i.node_group_id \
            INNER JOIN cluster c \
                ON c.name = ng.cluster_id \
            WHERE promotable = 1 \
            AND ng.cluster_id = '{0}' \
            LIMIT 1".format(cluster_name)
        return self.backend_engine.execute(query).first() 
    
    def InstanceStatusAddNotReachable(self, instance_id):
        query = "INSERT INTO instance_status (instance_id, reachable) \
                VALUES ('{0}', 0) \
                ON CONFLICT (instance_id) \
                DO UPDATE SET reachable = 0, \
                io_thread_running = 'No', \
                sql_thread_running = 'No', \
                promotable = 0, \
                proxy_status = 'down' \
                ".format(instance_id)           
        return self.backend_engine.execute(query)

######### INSTANCE METRICS #########
    def InstanceMetricAddNew(self, node_id):
        query = "INSERT INTO instance_metric \
            (instance_id, thread_connected, thread_running) \
            VALUES({0}, 0, 0)".format(node_id)
        return self.backend_engine.execute(query)

    def InstanceMetricUpdate(self, data):
        query = "UPDATE instance_metric \
            set thread_connected = {thread_connected}, thread_running = {thread_running} \
            WHERE instance_id = {node_id}".format(**data)
        return self.backend_engine.execute(query)
######### PROXY_LISTENER #########   
    def ProxyListenerGetLastPort(self):
        return self.backend_engine.execute("SELECT coalesce(max(port), 3000) FROM proxy_listener").first()[0] 
    
    def ProxyListenerAddNew(self, ng_id, name):
        query = "INSERT INTO proxy_listener (ng_id, port, name) \
            VALUES ({0}, {1}, '{2}') \
            ".format(
                ng_id, 
                self.ProxyListenerGetLastPort() + 1, 
                name
            )           
        return self.backend_engine.execute(query)  

    def ProxyListenerGetAll(self):
        return self.backend_engine.execute("SELECT * FROM proxy_listener order by name").fetchall() 

######### AUTH ######### 
    def AuthLoginUser(self, cred_dict):
        query = "SELECT COUNT(*) \
            FROM auth \
            WHERE email = '{0}' \
            AND pass = '{1}'".format(cred_dict['u'], cred_dict['p'])
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0]  

    def AuthGetFirstAccess(self, email):
        query = "SELECT first_access \
            FROM auth \
            WHERE email = '{0}'".format(email)
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0] 
    
    def AuthUpdatePass(self, cred_dict):
        query = "UPDATE auth \
            SET pass = '{0}' \
            WHERE email = '{1}'".format(cred_dict['np'], cred_dict['u'])
        return self.backend_engine.execute(query)
    
    def AuthUpdateFirstAccess(self, cred_dict):
        query = "UPDATE auth \
            SET first_access = {0} \
            WHERE email = '{1}'".format(cred_dict['first_access'], cred_dict['u'])
        return self.backend_engine.execute(query)
    
    def AuthAddNewUser(self, cred_dict):
        query = "INSERT INTO auth \
            (email, pass, token, mfa, first_access, role, company, name) \
            VALUES('{email}', '{pass}', '{token}', {mfa}, {first_access}, '{role}', '{company}', '{name}')".format(**cred_dict)
        return self.backend_engine.execute(query)
    
    def AuthCheckExisting(self, email):
        query = "SELECT COUNT(*) \
            FROM auth \
            WHERE email = '{0}'".format(email)
        result = self.backend_engine.execute(query).first()
        return None if result is None else result[0]  
    
    def AuthGetUserDetail(self, email):
        query = "SELECT * \
            FROM auth \
            WHERE email = '{0}'".format(email)
        return self.backend_engine.execute(query).first()

######### PROMOTION LEDGER ######### 
    def PromLedgerAddNew(self, data):
        query = "INSERT INTO promotion_ledger (origin_instance_id, primary_id, coord_set) \
            VALUES ({origin_instance_id}, {primary_id}, '{coord_set}') \
            ".format(**data)             
        return self.backend_engine.execute(query)
    
    def PromLedgerDeleteEntry(self, id):
        query = "DELETE FROM promotion_ledger where id = {0}".format(id)             
        return self.backend_engine.execute(query)

    def PromLedgerFetchOrderedEvent(self, id):
        query = "SELECT nl.*, ist.reachable, ist.replication_mode, i.name, i.hostname, i.port \
            FROM promotion_ledger nl \
            INNER JOIN instance_status ist \
                ON ist.instance_id = nl.primary_id \
            INNER JOIN instance i \
                ON i.id = ist.instance_id \
            WHERE origin_instance_id = {0} \
            ORDER BY id".format(id)
        return self.backend_engine.execute(query).fetchall() 

 



