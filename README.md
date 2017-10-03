# clickhouse-mysql-data-reader
utility to read mysql data


pip install mysql-replication

you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation

grant replication client, replication slave, super on *.* to 'reader'@'localhost' identified by 'qwerty';
flush privileges;


https://github.com/noplay/python-mysql-replication
https://github.com/mymarilyn/clickhouse-driver

