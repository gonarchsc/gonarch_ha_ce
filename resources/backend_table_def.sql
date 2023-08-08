create table IF NOT EXISTS cluster (
    name text primary key,
    created_at datetime DEFAULT (datetime('now','localtime')),
    huser text, 
	hpass text,
	maint_mode int default 0,
    promotion_rule text check(promotion_rule IN ('synced', 'available')) default ('synced'),
    proxy_max_allowed_lag int default (0)
);

create table IF NOT EXISTS node_group (
    id integer primary key AUTOINCREMENT,
    cluster_id int,
    name text,
    created_at datetime DEFAULT (datetime('now','localtime'))
);

create table IF NOT EXISTS instance (
    id integer primary key AUTOINCREMENT,
    node_group_id int,
    name text,
    hostname text,
    port int default (3306),
    version text,
    uuid text,
	arch text check(arch IN ('on-prem', 'rds', 'aurora')) default ('on-prem'),
	binlog_retention int,
    access_level text check(access_level IN ('w', 'r', 'rw', 'na')) default ('rw'),
	role text check(role IN ('primary', 'replica', 'broken-replica', 'unknown', 'int-primary', 'backup')) default ('unknown')    
);

create table IF NOT EXISTS instance_status (    
    instance_id integer primary key,
	reachable int,
	io_thread_running text,
	sql_thread_running text,
	io_thread_error text,
	sql_thread_error text,
    lag_sec int,	
	binlog_file text,
	binlog_pos int,
	io_thread_errorno int, 
	sql_thread_errorno int,
    failover_coord json,    
	promotable int default (0),
    gtid_coord text,
    replication_mode check(replication_mode IN ('gtid', 'binlog')) default ('binlog'),
    proxy_status check(proxy_status IN ('up', 'down')) default ('down')
);

create table IF NOT EXISTS promotion_ledger (
    id integer primary key AUTOINCREMENT,
    origin_instance_id int,
    primary_id int,
    coord_set text 
);

create table IF NOT EXISTS proxy_listener (   
    port integer primary key, 
    ng_id int,    
    name text    
);

create table IF NOT EXISTS auth (
    id integer primary key AUTOINCREMENT,
    name text,    
    email text,
    pass text,
    company text,
    role text,
	token text,  
    first_access int default (1), 
    mfa int default (0)
);

create table IF NOT EXISTS instance_metric (
   id integer primary key AUTOINCREMENT,
   instance_id int,
   thread_connected int,
   thread_running int
);