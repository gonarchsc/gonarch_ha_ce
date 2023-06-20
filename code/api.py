#!/usr/bin/python3

from flask import Flask, request, jsonify
from flask_cors import CORS
from waitress import serve
from sqlalchemy import exc
import sys, logging, yaml, json

# Load classes depending if PyInstaller is using them 
if getattr(sys, 'frozen', False):
   from classes_backend import BackEndSqlModel
   from classes_node import Node
elif __file__:
    from classes.classes_backend import BackEndSqlModel
    from classes.classes_node import Node

####### Config section ####### 
with open(r'/etc/gonarch.conf') as file:
    config_file = yaml.load(file, Loader=yaml.FullLoader)

dbname = config_file['workspace']['backend_dbname']
env = config_file['workspace']['env']
gonarch_cred = "{0}:{1}".format(config_file['mysql_credentials']['user'], config_file['mysql_credentials']['pass'])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(config_file['api']['logging']['path']),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()
app = Flask(__name__)
CORS(app)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
api_version = '/api/v1'
# Define the backend model class for db operations
backend_db = BackEndSqlModel(dbname)

def ResultToJson(sql_func):
    return jsonify([dict(r) for r in sql_func])    

def ParseGtidServer(backend_db, instance_row):
    server_l = []     
    gtid_coord_l_raw = instance_row['gtid_coord']
    if gtid_coord_l_raw is None:
        return 1
    gitd_coord_l = gtid_coord_l_raw.split(",")    
    gitd_coord_l = [ele for ele in gitd_coord_l if ele != '']
    for gitd_coord in gitd_coord_l:              
        server_uuid = gitd_coord.split(":")[0]  
        server_uuid = server_uuid.replace('\n',"")   
        server_coord = gitd_coord.split(":")[1]  
        server_coord = server_coord.replace('\n',"")      
        server_coord_dict = {
            'hostname': backend_db.InstanceParseServerUuid(server_uuid),
            'coord': server_coord
        }
        server_l.append(server_coord_dict)
    return server_l

# ============================
# CLUSTER 
# List of clusters
@app.route('{0}/cluster/list'.format(api_version), methods=['GET'])
def cluster_list():       
    result = backend_db.ClusterList()    
    if len(result) == 0:
        result_dict = {'message': "Cluster list is empty"}
        return jsonify(result_dict) 
    else:      
        return ResultToJson(result)

# Single cluster details
@app.route('{0}/cluster/details'.format(api_version), methods=['GET'])
def cluster_detail():   
    input_dict = json.loads(request.data)
    result = backend_db.InstanceGetInstanceListFromCluster(input_dict['name'])    
    if len(result) == 0:
        result_dict = {'message': "The indicated cluster does not exists ({0})".format(input_dict['name'])}
        return jsonify(result_dict) 

    instance_l = []
    for instance in result:
        repl_dict = {
            "replication_mode": instance['replication_mode'],
            "role": instance['role'],
            "promotable": instance['promotable']
        }
        if instance['role'] == 'replica':
            repl_dict['replication_lag'] = instance['lag_sec']
            repl_dict['io_thread_running'] = instance['io_thread_running']
            repl_dict['sql_thread_running'] = instance['sql_thread_running']  

        repl_dict['binlog_coordinates'] = "{0}:{1}".format(instance['binlog_file'], instance['binlog_pos'])      
        if instance['replication_mode'] == 'gtid' and instance['role'] == 'replica':
            gtid_l = []                
            for gtid_coord in ParseGtidServer(backend_db, instance):  
                gtid_dict = {                  
                    'source_instance': gtid_coord['hostname'],
                    'coordinates': gtid_coord['coord']
                }
                gtid_l.append(gtid_dict)
            repl_dict['gtid_details'] = gtid_l

        if instance['role'] == 'replica' and (instance['io_thread_running'] != 'Yes' or instance['sql_thread_running'] != 'Yes'):
            repl_dict['io_thread_error'] = "{0}:{1}".format(instance['io_thread_error'], instance['io_thread_errorno'])
            repl_dict['sql_thread_error'] = "{0}:{1}".format(instance['sql_thread_error'], instance['sql_thread_errorno'])


        instance_dict = {
            "node_group": instance['ng_name'],
            "instance_id": instance['id'],
            "instance_name": instance['name'],
            "hostname:port": "{0}:{1}".format(instance['hostname'], instance['port']),
            "version": instance['version'],
            "access_level": instance['access_level'],
            "role": instance['role'],
            "reachable": instance['reachable'],
            "threads": {
                "connected": instance['thread_connected'],
                "runnig" : instance['thread_running']
            },
            "replication": repl_dict
        }

        instance_l.append((instance_dict))
    result_dict = {
        "cluster_name": result[0]['c_name'],
        "creation_date": result[0]['c_created'],
        "promotion_rule": result[0]['promotion_rule'],
        "in_maintenance": result[0]['maint_mode'],
        "instances": instance_l
    }
    
    return jsonify(result_dict)

# Add new cluster
@app.route('{0}/cluster/add'.format(api_version), methods=['PUT'])
def cluster_add():      
    input_dict = json.loads(request.data)       
    # Check if the cluster already exists based on name.
    cluster_result = backend_db.ClusterCheckExisting(input_dict['name'])
    if cluster_result == 1: 
        input_dict = {'output_no': 1, 'message': "The indicated cluster already exists ({0})".format(input_dict['name'])} 
        return jsonify(input_dict)
    
    # Test connectivity to instance
    try:                    
        node_obj = Node(input_dict['name'], dbname, logger, gonarch_cred)        
        conn = node_obj.Connect(input_dict['primary'])
        mysql_vars = node_obj.FetchTargetVars(conn)
    except exc.OperationalError as e: 
        input_dict = {'output_no': 3, 'message': "Invalid credentilas or unreachable node".format(input_dict['primary'])} 
        return jsonify(input_dict)    

    # Check if target instance is primary
    try:                    
        if bool(node_obj.FetchTargetSlaveStatus(conn)) == True:
            input_dict = {'output_no': 4, 'message': "{0} is not the primary node".format(input_dict['primary'])} 
            return jsonify(input_dict)
    except exc.OperationalError as e: 
        input_dict = {'output_no': 2, 'message': "Node {0} is not reachable. Is it alive?".format(input_dict['primary'])} 
        return jsonify(input_dict)  

    # Build the data dictionary
    cluster_dict = {
        'name': input_dict['name'],
        'huser': input_dict['repl_credentials'].split(":")[0],
        'hpass': input_dict['repl_credentials'].split(":")[1],
        'promotion_rule': input_dict['promotion_rule']
    }
      
    # Add cluster
    backend_db.ClusterAddNew(cluster_dict)
    # Add node group
    ng_dict = {
        "cluster_id": input_dict['name'],
        "name": "{0}-ng-01".format(input_dict['name'])
    }
    ng_result = backend_db.NodeGroupAddNew(ng_dict)
    # Add the primary instance
    instance_dict = {
        "node_group_id": ng_result.lastrowid,
        "name": mysql_vars['@@hostname'],
        "hostname": input_dict['primary'].split(":")[0],
        "port": input_dict['primary'].split(":")[1],
        "version": mysql_vars['@@version'],
        "uuid": mysql_vars['@@server_uuid'],
        "arch": 'rds' if 'rds' in mysql_vars['@@basedir'] else 'on-prem',
        "binlog_retention": mysql_vars['@@expire_logs_days'],
        "role": "primary",
        "access_level": "rw"            
    }    
    result = backend_db.InstanceAddNew(instance_dict)
    backend_db.InstanceStatusAddNotReachable(result.lastrowid)
    backend_db.InstanceMetricAddNew(result.lastrowid)
    # Create new proxy listener for writer and reader 
    backend_db.ProxyListenerAddNew(ng_result.lastrowid, "{0}_writer".format(input_dict['name']))
    backend_db.ProxyListenerAddNew(ng_result.lastrowid, "{0}_reader".format(input_dict['name']))

    input_dict = {'output_no': 0, 'message': "Cluster {0} created".format(input_dict['name'])}               
    return jsonify(input_dict)

# Remove existing cluster
@app.route('{0}/cluster/remove'.format(api_version), methods=['DELETE'])
def cluster_remove():     
    input_dict = json.loads(request.data)  
    # Check if the cluster already exists based on name.
    cluster_result = backend_db.ClusterCheckExisting(input_dict['name'])
    # Teh cluster exists
    if cluster_result == 1:
        backend_db.ClusterUpdateMaintMode(1, input_dict['name'])  
        backend_db.ClusterRemoveFull(input_dict['name'])
        backend_db.ClusterUpdateMaintMode(0, input_dict['name'])
        input_dict = {'message': "Cluster removed ({0})".format(input_dict['name'])}
    else:
        input_dict = {'message': "The indicated cluster does not exists ({0})".format(input_dict['name'])}
    return input_dict

# Edit main mode in cluster
@app.route('{0}/cluster/edit/maintenance_mode'.format(api_version), methods=['PUT'])
def cluster_edit_maintmode(): 
    input_dict = json.loads(request.data)  
    # Check if the cluster already exists based on name.
    cluster_result = backend_db.ClusterCheckExisting(input_dict['name'])
    # Teh cluster exists
    if cluster_result == 1:
        backend_db.ClusterUpdateMaintMode(input_dict['flag'], input_dict['name'])
        input_dict = {'message': "Maintenance mode changed to {0} in cluster {1}".format(input_dict['flag'], input_dict['name'])}
    else:
        input_dict = {'message': "The indicated cluster does not exists ({0})".format(input_dict['name'])}
    return input_dict

# Edit promotion rule in cluster
@app.route('{0}/cluster/edit/promotion_rule'.format(api_version), methods=['PUT'])
def cluster_edit_promrule(): 
    input_dict = json.loads(request.data)      
    # Check if the cluster already exists based on name.
    cluster_result = backend_db.ClusterCheckExisting(input_dict['name'])
    # Teh cluster exists
    if cluster_result == 1:
        backend_db.ClusterUpdatePromotionRule(input_dict['flag'], input_dict['name'])
        input_dict = {'message': "Promotion rule changed to {0} in cluster {1}".format(input_dict['flag'], input_dict['name'])}
    else:
        input_dict = {'message': "The indicated cluster does not exists ({0})".format(input_dict['name'])}
    return input_dict
# ============================
# AUTH 
# Add new user
@app.route('{0}/auth/add'.format(api_version), methods=['PUT'])
def add_user(): 
    input_dict = json.loads(request.data)         
    # Check if the user already exists based on name.
    auth_result = backend_db.AuthCheckExisting(input_dict['email'])
    # The user exists
    if auth_result == 1:        
        input_dict = {'output_no': 1, 'message': "This user already has an account in this workspace"}
    else:
        backend_db.AuthAddNewUser(input_dict)
        input_dict = {'output_no': 0, 'message': "New user account added"}
    return input_dict

# Update password
@app.route('{0}/auth/edit/password'.format(api_version), methods=['PUT'])
def update_pass(): 
    input_dict = json.loads(request.data)      
    print(input_dict)   
    # Check if the user already exists based on name.
    auth_result = backend_db.AuthCheckExisting(input_dict['u'])
    # The user exists
    if auth_result == 1:        
        backend_db.AuthUpdatePass(input_dict)
        input_dict = {'output_no': 0, 'message': "Password updated"}
    else:
        backend_db.AuthAddNewUser(input_dict)
        input_dict = {'output_no': 1, 'message': "There is not user under this email"}
    return input_dict

# ============================
# NODE 
# Get primary node
@app.route('{0}/cluster/primary'.format(api_version), methods=['GET'])
def node_primary():  
    input_dict = json.loads(request.data)
    result =  backend_db.InstanceGetPrimaryFromCluster(input_dict['name'])
    if result is None:
       result_dict = {'message': "The indicated cluster does not exists ({0})".format(input_dict['name'])}
       return jsonify(result_dict) 
    else:        
        return "{0}:{1}".format(result['hostname'], result['port'])

# Edit access level for primary
@app.route('{0}/cluster/primary/edit/access_level'.format(api_version), methods=['PUT'])
def node_edit_access_level(): 
    input_dict = json.loads(request.data)  

    if input_dict['access_level'] not in ('rw', 'w'):
        input_dict = {'output_no': 2, 'message': "Accepted values for access level are w (Write only) or rw (Read & write)"}
        return input_dict

    # Check if the cluster already exists based on name.
    cluster_result = backend_db.ClusterCheckExisting(input_dict['name'])
    # The cluster exists
    if cluster_result == 1:
        primary_dict = backend_db.InstanceGetPrimaryFromCluster(input_dict['name'])
        update_dict = {
            'role':  'primary',
            'access_level': input_dict['access_level'],
            'node_id': primary_dict['id']
        }        
        backend_db.InstanceUpdateRole(update_dict)
        input_dict = {'output_no': 0, 'message': "Primary access level changed to {0} in cluster {1}".format(input_dict['access_level'], input_dict['name'])}
    else:
        input_dict = {'output_no': 1, 'message': "The indicated cluster does not exists ({0})".format(input_dict['name'])}
    return input_dict

# Remove existing node
@app.route('{0}/cluster/replica/remove'.format(api_version), methods=['DELETE'])
def replica_remove(): 
    input_dict = json.loads(request.data)  
    # Check if the cluster already exists based on name.
    cluster_result = backend_db.ClusterCheckExisting(input_dict['name'])

    # The cluster exists
    if cluster_result == 1:
        # Check if the replica exists in the cluster
        replica_result = backend_db.InstanceGetReplicaListFromCluster(input_dict['name'])
        for replica in replica_result:        
            if input_dict['id'] == replica['id']:
                backend_db.InstanceRemove(input_dict['id'])
                input_dict = {'output_no': 0, 'message': "Replica node removed ({0}). If this replica is still connected to master it will be added automatically again".format(input_dict['id'])}
            else:
                input_dict = {'output_no': 2, 'message': "The indicated replica node does not exists or it's not a replica ({0})".format(input_dict['id'])}
    else:
        input_dict = {'output_no': 1, 'message': "The indicated cluster does not exists ({0})".format(input_dict['name'])}
    return input_dict

if __name__ == '__main__':    
    #ssl_context=context
    # context = ('local.crt', 'local.key')
    if env == 'productionn':
        serve(app, host="0.0.0.0", port=config_file['api']['port'])
    else:
        app.run(host="0.0.0.0", port=config_file['api']['port'], debug=True)


