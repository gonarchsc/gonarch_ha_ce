FROM ubuntu:22.04

RUN apt-get update 
RUN apt-get install -y sqlite \
libmysqlclient-dev \
haproxy \
systemd \
socat \ 
util-linux

RUN mkdir -p /var/log/gonarch \
    && mkdir -p /opt/gonarch \
    && mkdir -p /run/haproxy

COPY resources/gonarch.conf.template /etc/gonarch.conf
COPY bin/core bin/check bin/api resources/haproxy_template.j2 version /opt/gonarch/

COPY resources/backend_table_def.sql /tmp/backend_table_def.sql 
COPY resources/startup.sh /tmp/startup.sh

RUN sed -i "s/WSNAME/myws/g" /etc/gonarch.conf \
    && sed -i "s/WSIP/127.0.0.1/g" /etc/gonarch.conf 

RUN sqlite3 /opt/gonarch/backend.db < /tmp/backend_table_def.sql

RUN useradd gonarch \
    && chown gonarch:gonarch /etc/gonarch.conf \
    && chown gonarch:gonarch /etc/haproxy/haproxy.cfg \
    && chown -R gonarch:gonarch /opt/gonarch \
    && chown -R gonarch:gonarch /var/log/gonarch

CMD "/tmp/startup.sh"

