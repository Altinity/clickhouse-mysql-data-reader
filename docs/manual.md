# Table of Contents

 * [Introduction](#introduction)
 * [Requirements and Installation](#requirements-and-installation)
<!-- 
   * [Dev Installation](#dev-installation)
   * [RPM Installation](#rpm-installation)
-->
   * [PyPi Installation](#pypi-installation)
   * [GitHub-based Installation - Clone Sources](#github-based-installation---clone-sources)
   * [MySQL setup](#mysql-setup)
 * [Quick Start](#quick-start)
 * [Operation](#operation)
   * [Requirements and Limitations](#requirements-and-limitations)
   * [Operation General Schema](#operation-general-schema)
   * [Performance](#performance)
 * [Examples](#examples)
   * [Base Example](#base-example)
   * [MySQL Migration Case 1 - with Tables Lock](#mysql-migration-case-1---with-tables-lock)
     * [MySQL Migration Case 1 - Create ClickHouse Table](#mysql-migration-case-1---create-clickhouse-table)
     * [MySQL Migration Case 1 - Migrate Existing Data](#mysql-migration-case-1---migrate-existing-data)
     * [MySQL Migration Case 1 - Listen For New Data](#mysql-migration-case-1---listen-for-new-data)
   * [MySQL Migration Case 2 - without Tables Lock](#mysql-migration-case-2---without-tables-lock)
     * [MySQL Migration Case 2 - Create ClickHouse Table](#mysql-migration-case-2---create-clickhouse-table)
     * [MySQL Migration Case 2 - Listen For New Data](#mysql-migration-case-2---listen-for-new-data)
     * [MySQL Migration Case 2 - Migrate Existing Data](#mysql-migration-case-2---migrate-existing-data)
   * [airline.ontime Test Case](#airlineontime-test-case)
     * [airline.ontime Data Set in CSV files](#airlineontime-data-set-in-csv-files)
     * [airline.ontime MySQL Table](#airlineontime-mysql-table)
     * [airline.ontime ClickHouse Table](#airlineontime-clickhouse-table)
     * [airline.ontime Data Reader](#airlineontime-data-reader)
     * [airline.ontime Data Importer](#airlineontime-data-importer)
 * [Testing](#testing)
   * [Testing General Schema](#testing-general-schema)
     * [MySQL Data Types](#mysql-data-types)
     * [ClickHouse Data Types](#clickhouse-data-types)
     * [MySQL -> ClickHouse Data Types Mapping](#mysql---clickhouse-data-types-mapping)
     * [MySQL Test Tables](#mysql-test-tables)
     * [ClickHouse Test Tables](#clickhouse-test-tables)
   
---

# Introduction

Utility to import data into ClickHouse from MySQL (mainly) and/or CSV files

# Requirements and Installation

Datareader requires at least **Python 3.4** with additional modules to be installed.
In most distributions Python 3 have `pip` utility named as `pip3`, so we'll use this naming. 
However, you may have it called differently.

Datareader can be installed either from `github` repo or from `pypi` repo.

<!--

## Dev Installation
```bash
sudo yum install -y rpm-build
sudo yum install -y epel-release
sudo yum install -y https://dev.mysql.com/get/mysql57-community-release-el7-11.noarch.rpm
curl -s https://packagecloud.io/install/repositories/altinity/clickhouse/script.rpm.sh | sudo bash

sudo yum install -y python34-pip python34-devel python34-setuptools

./package_rpm_distr.sh
./pack/build.sh
ls -l ./build/bdist.linux-x86_64/rpm/RPMS/noarch/
sudo yum install ./build/bdist.linux-x86_64/rpm/RPMS/noarch/clickhouse-mysql-* 
```

## RPM Installation
**Tested on CentOS 7**

Packagecloud repo from [packagecloud.io](https://packagecloud.io/Altinity/clickhouse)
More details on installation are available on [https://github.com/Altinity/clickhouse-rpm-install](https://github.com/Altinity/clickhouse-rpm-install)
```bash
curl -s https://packagecloud.io/install/repositories/altinity/clickhouse/script.rpm.sh | sudo bash
```
Install EPEL (for `python3`) and MySQL (for `libmysqlclient`) repos
```bash
sudo yum install -y epel-release
sudo yum install -y https://dev.mysql.com/get/mysql57-community-release-el7-11.noarch.rpm
```

If you do not have EPEL available in your repos, install it directly from EPEL site
```bash
sudo yum install -y https://download.fedoraproject.org/pub/epel/7/x86_64/Packages/e/epel-release-7-11.noarch.rpm
```

Install data reader from [packagecloud.io](https://packagecloud.io/Altinity/clickhouse)
```bash
sudo yum install -y clickhouse-mysql
```
clickhouse packages would also be installed as dependencies.

Prepare config file - copy **example** file into production and edit it.
```bash
sudo cp /etc/clickhouse-mysql/clickhouse-mysql-example.conf /etc/clickhouse-mysql/clickhouse-mysql.conf
sudo vim /etc/clickhouse-mysql/clickhouse-mysql.conf
```

Start service
```bash
sudo service clickhouse-mysql start
```
-->

## PyPi Installation
In case you need just to use the app - this is the most convenient way to go.

Install dependencies.
MySQL repo (for `mysql-community-devel`)
```bash
sudo yum install -y https://dev.mysql.com/get/mysql57-community-release-el7-11.noarch.rpm
```
epel (for `python3`)
```bash
sudo yum install -y epel-release
```

clickhouse-client (for `clickhouse-client`) from Packagecloud repo from [packagecloud.io](https://packagecloud.io/Altinity/clickhouse)
More details on installation are available on [https://github.com/Altinity/clickhouse-rpm-install](https://github.com/Altinity/clickhouse-rpm-install)
```bash
curl -s https://packagecloud.io/install/repositories/altinity/clickhouse/script.rpm.sh | sudo bash
```
```bash
sudo yum install -y clickhouse-client
```

and direct dependencies:
```bash
sudo yum install -y mysql-community-devel
sudo yum install -y mariadb-devel
sudo yum install -y gcc
sudo yum install -y python34-devel python34-pip
```

Install data reader
```bash
sudo pip3 install clickhouse-mysql
```

Now we are able to call datareader as an app - perform last installation steps - install service files, etc
```bash
[user@localhost ~]$ which clickhouse-mysql
/usr/bin/clickhouse-mysql
/usr/bin/clickhouse-mysql --install
```

## GitHub-based Installation - Clone Sources
In case you'd like to play around with the sources this is the way to go.

Install dependencies: 

`MySQLdb` package is used for communication with MySQL:
```bash
pip3 install mysqlclient
```

`mysql-replication` package is used for communication with MySQL also:
[https://github.com/noplay/python-mysql-replication](https://github.com/noplay/python-mysql-replication)
```bash
pip3 install mysql-replication
```

`clickhouse-driver` package is used for communication with ClickHouse:
[https://github.com/mymarilyn/clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver)
```bash
pip3 install clickhouse-driver
```

Clone sources from github
```bash
git clone https://github.com/Altinity/clickhouse-mysql-data-reader
```

## MySQL setup

Also the following (at least one of) MySQL privileges are required for this operation: `SUPER`, `REPLICATION CLIENT` 

```mysql
CREATE USER 'reader'@'%' IDENTIFIED BY 'qwerty';
CREATE USER 'reader'@'127.0.0.1' IDENTIFIED BY 'qwerty';
CREATE USER 'reader'@'localhost' IDENTIFIED BY 'qwerty';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE, SUPER ON *.* TO 'reader'@'%';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE, SUPER ON *.* TO 'reader'@'127.0.0.1';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE, SUPER ON *.* TO 'reader'@'localhost';
FLUSH PRIVILEGES;
```

Also the following MySQL config options are required:
```ini
[mysqld]
# mandatory
server-id        = 1
log_bin          = /var/lib/mysql/bin.log
binlog-format    = row # very important if you want to receive write, update and delete row events
# optional
expire_logs_days = 30
max_binlog_size  = 768M
# setup listen address
bind-address     = 0.0.0.0
```

# Quick Start

Suppose we have MySQL `airline.ontime` table of the [following structure - clickhouse_mysql_examples/airline_ontime_schema_mysql.sql](../clickhouse_mysql_examples/airline_ontime_schema_mysql.sql) and want to migrate it into ClickHouse.

Steps to do:

  * Setup MySQL access as described in [MySQL setup](#mysql-setup)
  * Run data reader as following:
  
```bash
clickhouse-mysql \
    --src-server-id=1 \
    --src-wait \
    --nice-pause=1 \
    --src-host=127.0.0.1 \
    --src-user=reader \
    --src-password=qwerty \
    --src-tables=airline.ontime \
    --dst-host=127.0.0.1 \
    --dst-create-table \
    --migrate-table \
    --pump-data \
    --csvpool
```
 
Expected results are:
  * automatically create target table in ClickHouse (if possible)
  * migrate existing data from MySQL to ClickHouse
  * after migration completed, listen for new events to come and pump data from MySQL into ClickHouse
 
Options description
  * `--src-server-id` - Master's server id
  * `--src-wait` - wait for new data to come
  * `--nice-pause=1` - when no data available sleep for 1 second
  * `--src-host=127.0.0.1` - MySQL source host
  * `--src-user=reader` - MySQL source user (remember about PRIVILEGES for this user)
  * `--src-password=qwerty` - MySQL source password (remember about PRIVILEGES for this user)
  * `--src-tables=airline.ontime` - list of MySQL source tables to process
  * `--dst-host=127.0.0.1` - ClickHouse host
  * `--dst-create-table` - create target table automatically
  * `--migrate-table` - migrate source tables
  * `--pump-data` - pump data from MySQL into ClickHouse after data migrated
  * `--csvpool` - make pool of csv files while pumping data (assumes `--mempool` also)

Choose any combination of `--pump-data`, `--migrate-table`, `--create-table-sql`, `--dst-create-table`

# Operation

## Requirements and Limitations

Data reader understands INSERT SQL statements only. In practice this means that:
  * You need to create required table in ClickHouse before starting data read procedure. More on how to create target ClickHouse table: [MySQL -> ClickHouse Data Types Mapping](#mysql---clickhouse-data-types-mapping)
  * From all DML statements INSERT-only are handled, which means:
    * UPDATE statements are not handled - meaning UPDATEs within MySQL would not be relayed into ClickHouse
    * DELETE statements are not handled - meaning DELETEs within MySQL would not be relayed into ClickHouse
  * DDL statements are not handled, which means:
    * source table structure change (ALTER TABLE) has to be handled externally and can lead to insertion errors 

## Operation General Schema

  * Step 1. Data Reader reads data from the source event-by-event (for MySQL binlog) or line-by-line (file).
  * Step 2. **OPTIONAL** Caching in memory pool. Since ClickHouse prefers to get data in bundles (row-by-row insertion is extremely slow), we need to introduce some caching.
  Cache can be flushed by either of:
    * number of rows in cache
    * number of events in cache 
    * time elapsed
    * data source depleted
  * Step 3. **OPTIONAL** Writing CSV file. Sometimes it is useful to have data also represented as a file
  * Step 4. Writing data into ClickHouse. Depending on the configuration of the previous steps data are written into ClickHouse by either of:
    * directly event-by-event or line-by-line
    * from memory cache as a bulk insert operation
    * from CSV file via `clickhouse-client` 
    
## Performance

`pypy` significantly improves performance. You should try it. Really. Up to **10 times performance boost** can be achieved.
For example you can start with [Portable PyPy distribution for Linux](https://github.com/squeaky-pl/portable-pypy#portable-pypy-distribution-for-linux)
 - use [Python 3.x release](https://github.com/squeaky-pl/portable-pypy#latest-python-35-release)
Unpack it into your place of choice.

```bash
[user@localhost ~]$ ls -l pypy3.5-5.9-beta-linux_x86_64-portable
total 32
drwxr-xr-x  2 user user   140 Oct 24 01:14 bin
drwxr-xr-x  5 user user  4096 Oct  3 11:57 include
drwxr-xr-x  4 user user  4096 Oct  3 11:57 lib
drwxr-xr-x 13 user user  4096 Oct  3 11:56 lib_pypy
drwxr-xr-x  3 user user    15 Oct  3 11:56 lib-python
-rw-r--r--  1 user user 11742 Oct  3 11:56 LICENSE
-rw-r--r--  1 user user  1296 Oct  3 11:56 README.rst
drwxr-xr-x 14 user user  4096 Oct 24 01:16 site-packages
drwxr-xr-x  2 user user   195 Oct  3 11:57 virtualenv_support
```

Install `pip`
```bash
pypy3.5-5.9-beta-linux_x86_64-portable/bin/pypy -m ensurepip
```
Install required modules
```bash
pypy3.5-5.9-beta-linux_x86_64-portable/bin/pip3 install mysql-replication
pypy3.5-5.9-beta-linux_x86_64-portable/bin/pip3 install clickhouse-driver
```
`mysqlclient` may require to install `libmysqlclient-dev` and `gcc`
```bash
pypy3.5-5.9-beta-linux_x86_64-portable/bin/pip3 install mysqlclient
```
Install them if need be
```bash
sudo apt-get install libmysqlclient-dev
```
```bash
sudo apt-get install gcc
```

Now you can run data reader via `pypy`
```bash
/home/user/pypy3.5-5.9-beta-linux_x86_64-portable/bin/pypy clickhouse-mysql
```

# Examples

## Base Example

Let's walk over test example of tool launch command line options. 
This code snippet is taken from shell script (see more details in [airline.ontime Test Case](#airlineontime-test-case))
 
```bash
$PYTHON clickhouse-mysql ${*:1} \
    --src-server-id=1 \
    --src-resume \
    --src-wait \
    --nice-pause=1 \
    --log-level=info \
    --log-file=ontime.log \
    --src-host=127.0.0.1 \
    --src-user=root \
    --dst-host=127.0.0.1 \
    --csvpool \
    --csvpool-file-path-prefix=qwe_ \
    --mempool-max-flush-interval=60 \
    --mempool-max-events-num=1000 \
    --pump-data
```
Options description
  * `--src-server-id` - Master's server id
  * `--src-resume` - resume data loading from the previous point. When the tool starts - resume from the end of the log 
  * `--src-wait` - wait for new data to come
  * `--nice-pause=1` - when no data available sleep for 1 second
  * `--log-level=info` - log verbosity
  * `--log-file=ontime.log` - log file name
  * `--src-host=127.0.0.1` - MySQL source host
  * `--src-user=root` - MySQL source user (remember about PRIVILEGES for this user)
  * `--dst-host=127.0.0.1` - ClickHouse host
  * `--csvpool` - make pool of csv files (assumes `--mempool` also)
  * `--csvpool-file-path-prefix=qwe_` - put these CSV files having `qwe_` prefix in `CWD`
  * `--mempool-max-flush-interval=60` - flush mempool at least every 60 seconds
  * `--mempool-max-events-num=1000` - flush mempool at least each 1000 events (not rows, but events)
  * `--pump-data` - pump data from MySQL into ClickHouse

## MySQL Migration Case 1 - with Tables Lock

Suppose we have MySQL `airline.ontime` table of the [following structure - clickhouse_mysql_examples/airline_ontime_schema_mysql.sql](../clickhouse_mysql_examples/airline_ontime_schema_mysql.sql) with multiple rows:

```mysql
mysql> SELECT COUNT(*) FROM airline.ontime;
+----------+
| count(*) |
+----------+
|  7694964 |
+----------+
```

MySQL is already configured as [described earlier](#mysql-setup).
Let's migrate existing data to ClickHouse and listen for newly coming data in order to migrate them to CLickHouse on-the-fly.

### MySQL Migration Case 1 - Create ClickHouse Table

Create ClickHouse table description
```bash
clickhouse-mysql \
    --src-host=127.0.0.1 \
    --src-user=reader \
    --src-password=Qwerty1# \
    --create-table-sql-template \
    --with-create-database \
    --src-tables=airline.ontime > create_clickhouse_table_template.sql
```
We have **CREATE TABLE** template stored in `create_clickhouse_table_template.sql` file.
```bash
vim create_clickhouse.sql
```  
Setup sharding field and primary key. These columns must not be `Nullable`
```bash mysql
...cut...
    `Year` UInt16,
...cut...    
    `FlightDate` Date,
...cut...    
    `Month` UInt8,
...cut...
) ENGINE = MergeTree(FlightDate, (FlightDate, Year, Month), 8192)
```

Create table in ClickHouse
```bash
clickhouse-client -mn < create_clickhouse_table_template.sql
```

### MySQL Migration Case 1 - Migrate Existing Data

Lock MySQL in order to avoid new data coming while data migration is running. Keep `mysql` client open during the whole process
```mysql
mysql> FLUSH TABLES WITH READ LOCK;
```

Migrate data
```bash
clickhouse-mysql \
    --src-host=127.0.0.1 \
    --src-user=reader \
    --src-password=Qwerty1# \
    --migrate-table \
    --src-tables=airline.ontime \
    --dst-host=127.0.0.1
```
This may take some time.
Check all data is in ClickHouse 
```mysql
:) select count(*) from airline.ontime;

SELECT count(*)
FROM airline.ontime

┌─count()─┐
│ 7694964 │
└─────────┘
```

### MySQL Migration Case 1 - Listen For New Data

Start `clickhouse-mysql` as a replication slave, so it will listen for new data coming:
```bash
clickhouse-mysql \
    --src-server-id=1 \
    --src-resume \
    --src-wait \
    --nice-pause=1 \
    --src-host=127.0.0.1 \
    --src-user=reader \
    --src-password=Qwerty1# \
    --src-tables=airline.ontime \
    --dst-host=127.0.0.1 \
    --csvpool \
    --csvpool-file-path-prefix=qwe_ \
    --mempool-max-flush-interval=60 \
    --mempool-max-events-num=10000 \
    --pump-data
```

Allow new data to be inserted into MySQL - i.e. unlock tables.

```mysql
mysql> UNLOCK TABLES;
```

Insert some data into MySQL. For example, via [clickhouse_mysql_examples/airline_ontime_mysql_data_import.sh](../clickhouse_mysql_examples/airline_ontime_mysql_data_import.sh) script

```mysql
mysql> SELECT COUNT(*) FROM airline.ontime;
+----------+
| count(*) |
+----------+
| 10259952 |
+----------+
```

Replication will be pumping data from MySQL into ClickHouse in background and in some time we'll see the following picture in ClickHouse:
```mysql
:) select count(*) from airline.ontime;

SELECT count(*)
FROM airline.ontime

┌──count()─┐
│ 10259952 │
└──────────┘
```

## MySQL Migration Case 2 - without Tables Lock
Suppose we'd like to migrate multiple log tables of the same structure named as `log_XXX` - i.e. all of them have `log_` name prefix
into one ClickHouse table named `logunified` of the following structure
```sql
DESCRIBE TABLE logunified

┌─name─┬─type───┬─default_type─┬─default_expression─┐
│ id   │ UInt64 │              │                    │
│ day  │ Date   │              │                    │
│ str  │ String │              │                    │
└──────┴────────┴──────────────┴────────────────────┘
```
Log tables by nature are `INSERT`-only tables. Let's migrate these tables.
 
### MySQL Migration Case 2 - Create ClickHouse Table
Prepare tables templates in `create_clickhouse.sql` file 
```bash
clickhouse-mysql \
    --src-host=127.0.0.1 \
    --src-user=reader \
    --src-password=qwerty \
    --create-table-sql-template \
    --with-create-database \
    --src-tables-prefixes=db.log_ > create_clickhouse_table_template.sql
```
Edit templates
```bash
vim create_clickhouse_table_template.sql
```
And create tables in ClickHouse
```bash

clickhouse-client -mn < create_clickhouse_table_template.sql
```

### MySQL Migration Case 2 - Listen For New Data
```bash
clickhouse-mysql \
    --src-server-id=1 \
    --src-resume \
    --src-wait \
    --nice-pause=1 \
    --log-level=info \
    --src-host=127.0.0.1 \
    --src-user=reader \
    --src-password=qwerty \
    --src-tables-prefixes=log_ \
    --dst-host=127.0.0.1 \
    --dst-table=logunified \
    --csvpool \
    --pump-data
```
Pay attention to 
```bash
    --src-tables-prefixes=log_ \
    --dst-table=logunified \
```
Replication data from multiple tables into one destination table `--dst-table=logunified`. 

Monitor logs for `first row in replication` notification of the following structure:
```bash
INFO:first row in replication db.log_201801_2
column: id=1727834
column: day=2018-01-20
column: str=data event 3
```
These records help us to create SQL statement for Data Migration process. 
Sure, we can peek into MySQL database manually in order to understand what records would be the last to be copied by migration process.

### MySQL Migration Case 2 - Migrate Existing Data

```bash
clickhouse-mysql \
     --src-host=127.0.0.1 \
     --src-user=reader \
     --src-password=qwerty \
     --migrate-table \
     --src-tables-prefixes=db.log_ \
     --src-tables-where-clauses=db.log_201801_1=db.log_201801_1.sql,db.log_201801_2=db.log_201801_2.sql,db.log_201801_3=db.log_201801_3.sql \
     --dst-host=127.0.0.1 \
     --dst-table=logunified \
     --csvpool
```

Pay attention to
```bash
     --src-tables-prefixes=db.log_ \
     --src-tables-where-clauses=db.log_201801_1=db.log_201801_1.sql,db.log_201801_2=db.log_201801_2.sql,db.log_201801_3=db.log_201801_3.sql \
     --dst-table=logunified \
```
Migration subset of data described in `--src-tables-where-clauses` files from multiple tables into one destination table `--dst-table=logunified` 

Values for where clause in  `db.log_201801_1.sql` are fetched from `first row in replication` log: `INFO:first row in replication db.log_201801_1` 
```bash
cat db.log_201801_1.sql 
id < 1727831
```

Result:
```sql
:) select count(*) from logunified;

SELECT count(*)
FROM logunified 

┌──count()─┐
│ 12915568 │
└──────────┘

```

## airline.ontime Test Case

Main Steps
  * Download airline.ontime dataset
  * Create airline.ontime MySQL table
  * Create airline.ontime ClickHouse table
  * Start data reader (utility to migrate data MySQL -> ClickHouse)
  * Start data importer (utility to import data into MySQL)
  * Check how data are loaded into ClickHouse

### airline.ontime Data Set in CSV files
Run [download script](../clickhouse_mysql_examples/airline_ontime_data_download.sh)

You may want to adjust dirs where to keep `ZIP` and `CSV` file

In `airline_ontime_data_download.sh` edit these lines:
```bash
...
ZIP_FILES_DIR="zip"
CSV_FILES_DIR="csv"
...
```
You may want to adjust number of files to download (In case downloading all it may take some time).

Specify year and months range as you wish:
```bash
...
echo "Download files into $ZIP_FILES_DIR"
for year in `seq 1987 2017`; do
	for month in `seq 1 12`; do
...
``` 

```bash
./airline_ontime_data_download.sh
```
Downloading can take some time. 

### airline.ontime MySQL Table
Create MySQL table of the [following structure - clickhouse_mysql_examples/airline_ontime_schema_mysql.sql](../clickhouse_mysql_examples/airline_ontime_schema_mysql.sql):
```bash
mysql -uroot -p < clickhouse_mysql_examples/airline_ontime_schema_mysql.sql
```

### airline.ontime ClickHouse Table
Create ClickHouse table of the [following structure - clickhouse_mysql_examples/airline_ontime_schema_ch.sql](../clickhouse_mysql_examples/airline_ontime_schema_ch.sql):
```bash
clickhouse-client -mn < clickhouse_mysql_examples/airline_ontime_schema_ch.sql
```

### airline.ontime Data Reader
Run [datareader script](../clickhouse_mysql_examples/airline_ontime_data_mysql_to_ch_reader.sh)

You may want to adjust `PYTHON` path and source and target hosts and usernames
```bash
...
PYTHON=python3.6
PYTHON=/home/user/pypy3.5-5.9-beta-linux_x86_64-portable/bin/pypy
...
```
```bash
...
    --src-host=127.0.0.1 \
    --src-user=root \
    --dst-host=127.0.0.1 \
...
```
```bash
./airline_ontime_data_mysql_to_ch_reader.sh
```

### airline.ontime Data Importer
Run [data importer script](../clickhouse_mysql_examples/airline_ontime_mysql_data_import.sh)

You may want to adjust `CSV` files location, number of imported files and MySQL user/password used for import
```bash
...
# looking for csv files in this dir
FILES_TO_IMPORT_DIR="/mnt/nas/work/ontime"

# limit import to this number of files
FILES_TO_IMPORT_NUM=3
...
```
```bash
...
        -u root \
...
```

```bash
./airline_ontime_mysql_data_import.sh
``` 

# Testing

## Testing General Schema

### MySQL Data Types

#### Numeric Types

  * `BIT`  the number of bits per value, from 1 to 64
  * `TINYINT` -128 to 127. The unsigned range is 0 to 255
  * `BOOL`, `BOOLEAN` synonyms for `TINYINT(1)`
  * `SMALLINT`  -32768 to 32767. The unsigned range is 0 to 65535
  * `MEDIUMINT` -8388608 to 8388607. The unsigned range is 0 to 16777215.
  * `INT`, `INTEGER` -2147483648 to 2147483647. The unsigned range is 0 to 4294967295
  * `BIGINT` -9223372036854775808 to 9223372036854775807. The unsigned range is 0 to 18446744073709551615

  * `SERIAL` is an alias for `BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE`.
  * `DEC`, `DECIMAL`, `FIXED`, `NUMERIC` A packed ?exact? fixed-point number
  * `FLOAT` Permissible values are -3.402823466E+38 to -1.175494351E-38, 0, and 1.175494351E-38 to 3.402823466E+38
  * `DOUBLE`, `REAL` Permissible values are -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308


#### Date and Time Types

  * `DATE` The supported range is '1000-01-01' to '9999-12-31'
  * `DATETIME` The supported range is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
  * `TIMESTAMP` The range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999'
  * `TIME` The range is '-838:59:59.000000' to '838:59:59.000000'
  * `YEAR` Values display as 1901 to 2155, and 0000

#### String Types
  * `CHAR` The range of M is 0 to 255. If M is omitted, the length is 1.
  * `VARCHAR` The range of M is 0 to 65,535
  * `BINARY` similar to CHAR
  * `VARBINARY` similar to VARCHAR
  * `TINYBLOB` maximum length of 255
  * `TINYTEXT` maximum length of 255
  * `BLOB` maximum length of 65,535
  * `TEXT` maximum length of 65,535
  * `MEDIUMBLOB` maximum length of 16,777,215
  * `MEDIUMTEXT` maximum length of 16,777,215
  * `LONGBLOB` maximum length of 4,294,967,295 or 4GB
  * `LONGTEXT` maximum length of 4,294,967,295 or 4GB
  * `ENUM` can have a maximum of 65,535 distinct elements
  * `SET` can have a maximum of 64 distinct members

  * `JSON` native JSON data type defined by RFC 7159

---

### ClickHouse Data Types

  * `Date` number of days since 1970-01-01
  * `DateTime` Unix timestamp
  * `Enum8` or `Enum16`. A set of enumerated string values that are stored as `Int8` or `Int16`. The numeric values must be within -128..127 for Enum8 and -32768..32767 for Enum16
  * `Float32`, `Float64`

  * `Int8` -128 127
  * `UInt8` 0 255

  * `Int16` -32768 32767
  * `UInt16` 0 65535

  * `Int32` -2147483648 2147483647
  * `UInt32` 0 4294967295

  * `Int64` -9223372036854775808 9223372036854775807
  * `UInt64` 0 18446744073709551615

  * `FixedString(N)` string of `N` bytes (not characters or code points)
  * `String` The length is not limited. The value can contain an arbitrary set of bytes, including null bytes

---

### MySQL -> ClickHouse Data Types Mapping

#### Numeric Types

  * `BIT` -> ??? (possibly `String`?)
  * `TINYINT` -> `Int8`, `UInt8`
  * `BOOL`, `BOOLEAN` -> `UInt8`
  * `SMALLINT` -> `Int16`, `UInt16`
  * `MEDIUMINT` -> `Int32`, `UInt32`
  * `INT`, `INTEGER` -> `Int32`, `UInt32`
  * `BIGINT` -> `Int64`, `UInt64`

  * `SERIAL` -> `UInt64`
  * `DEC`, `DECIMAL`, `FIXED`, `NUMERIC` -> ???? (possibly `String`?)
  * `FLOAT` -> `Float32`
  * `DOUBLE`, `REAL` -> `Float64`


#### Date and Time Types

  * `DATE` -> `Date` (for valid values) or `String`
  `Date` Allows storing values from just after the beginning of the Unix Epoch 
  to the upper threshold defined by a constant at the compilation stage 
  (currently, this is until the year 2038, but it may be expanded to 2106)
  * `DATETIME` -> `DateTime` (for valid values) or `String`
  * `TIMESTAMP` -> `DateTime`
  * `TIME` -> ????? (possibly `String`?)
  * `YEAR` -> `UInt16`


#### String Types

  * `CHAR` -> `FixedString`
  * `VARCHAR` -> `String`
  * `BINARY` -> `String`
  * `VARBINARY` -> `String`
  * `TINYBLOB` -> `String`
  * `TINYTEXT` -> `String`
  * `BLOB` -> `String`
  * `TEXT` -> `String`
  * `MEDIUMBLOB` -> `String`
  * `MEDIUMTEXT` -> `String`
  * `LONGBLOB` -> `String`
  * `LONGTEXT` -> `String`

#### Set Types
  * `ENUM` -> `Enum8`, `Enum16`
  * `SET` -> `Array(Int8)`

#### Custom Types
  * `JSON` -> ?????? (possibly `String`?)


### MySQL Test Tables

We have to separate test table into several ones because of this error, produced by MySQL:
```text
ERROR 1118 (42000): Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs
```

```mysql
CREATE TABLE datatypes(

    bit_1 BIT(1),
    bit_2 BIT(64),

    tinyint_1   TINYINT          COMMENT '-128 to 127',
    u_tinyint_1 TINYINT UNSIGNED COMMENT '0 to 255',

    bool_1 BOOL,
    bool_2 BOOLEAN,

    smallint_1   SMALLINT           COMMENT '-32768 to 32767',
    u_smallint_1 SMALLINT UNSIGNED  COMMENT '0 to 65535',

    mediumint_1   MEDIUMINT          COMMENT '-8388608 to 8388607',
    u_mediumint_1 MEDIUMINT UNSIGNED COMMENT '0 to 16777215',

    int_1   INT          COMMENT '-2147483648 to 2147483647',
    u_int_1 INT UNSIGNED COMMENT '0 to 4294967295',

    integer_1   INTEGER          COMMENT '-2147483648 to 2147483647',
    u_integer_1 INTEGER UNSIGNED COMMENT '0 to 4294967295',

    bigint_1   BIGINT          COMMENT '-9223372036854775808 to 9223372036854775807',
    u_bigint_1 BIGINT UNSIGNED COMMENT '0 to 18446744073709551615',

    serial_1 SERIAL COMMENT 'i.e. BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE. 0 to 18446744073709551615',

    decimal_1 DECIMAL(3,2) COMMENT 'exact fixed-point number',
    dec_1     DEC(3,2)     COMMENT 'alias for DECIMAL',
    fixed_1   FIXED(3,2)   COMMENT 'alias for DECIMAL',
    numeric_1 NUMERIC(3,2) COMMENT 'alias for DECIMAL',

    float_1   FLOAT          COMMENT '-3.402823466E+38 to -1.175494351E-38, 0, and 1.175494351E-38 to 3.402823466E+38',
    u_float_1 FLOAT UNSIGNED COMMENT '                                      0, and 1.175494351E-38 to 3.402823466E+38',

    double_1   DOUBLE          COMMENT '-1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',
    u_double_1 DOUBLE UNSIGNED COMMENT '                                                      0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',

    real_1   REAL          COMMENT 'alias for          DOUBLE i.e. -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',
    u_real_1 REAL UNSIGNED COMMENT 'alias for UNSIGNED DOUBLE i.e.                                                       0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',

    date_1      DATE      COMMENT '1000-01-01 to 9999-12-31',
    datetime_1  DATETIME  COMMENT '1000-01-01 00:00:00 to 9999-12-31 23:59:59',
    timestamp_1 TIMESTAMP COMMENT '1970-01-01 00:00:01 UTC to 2038-01-19 03:14:07 UTC',
    time_1      TIME      COMMENT '-838:59:59 to 838:59:59',
    year_1      YEAR      COMMENT '1901 to 2155, and 0000',

    char_0 CHAR(0),
    char_1 CHAR(1),
    char_2 CHAR(255),

    varchar_0 VARCHAR(0),
    varchar_1 VARCHAR(1),

    binary_0 BINARY(0)   COMMENT 'similar to CHAR',
    binary_1 BINARY(1)   COMMENT 'similar to CHAR',
    binary_2 BINARY(255) COMMENT 'similar to CHAR',

    varbinary_0 VARBINARY(0) COMMENT 'similar to VARCHAR',
    varbinary_1 VARBINARY(1) COMMENT 'similar to VARCHAR',

    tinyblob_1 TINYBLOB COMMENT 'maximum length of 255 (2^8 ? 1) bytes',
    tinytext_1 TINYTEXT COMMENT 'maximum length of 255 (2^8 ? 1) characters',

    blob_1 BLOB COMMENT 'maximum length of 65,535 (2^16 ? 1) bytes',
    text_1 TEXT COMMENT 'maximum length of 65,535 (2^16 ? 1) characters',

    mediumblob_1 MEDIUMBLOB COMMENT 'maximum length of 16,777,215 (2^24 ? 1) bytes',
    mediumtext_1 MEDIUMTEXT COMMENT 'maximum length of 16,777,215 (2^24 ? 1) characters',

    longblob_1 LONGBLOB COMMENT 'maximum length of 4,294,967,295 or 4GB (2^32 ? 1) bytes',
    longtext_1 LONGTEXT COMMENT 'maximum length of 4,294,967,295 or 4GB (2^32 ? 1) characters'
)
;

CREATE TABLE enum_datatypes(
    enum_1 ENUM('a', 'b', 'c', 'd', 'e', 'f') COMMENT 'can have a maximum of 65,535 distinct elements'
)
;

CREATE TABLE set_datatypes(
    set_1 SET('a', 'b', 'c', 'd', 'e', 'f') COMMENT ' can have a maximum of 64 distinct members'
)
;

CREATE TABLE json_datatypes(
    json_1 JSON
)
;

CREATE TABLE long_varchar_datatypes(
    varchar_2 VARCHAR(65532)
)
;

CREATE TABLE long_varbinary_datatypes(
    varbinary_2 VARBINARY(65532) COMMENT 'similar to VARCHAR'
)
;
```


```mysql
-- in order to be able to set timestamp = '1970-01-01 00:00:01'
set time_zone='+00:00';
```

Insert minimal acceptable values into the test table:

```mysql
-- MIN values
INSERT INTO datatypes SET

    bit_1 = 0b0, -- BIT(1),
    bit_2 = 0b0, -- BIT(64),

    tinyint_1   = -128, -- TINYINT          COMMENT '-128 to 127',
    u_tinyint_1 = 0,    -- TINYINT UNSIGNED COMMENT '0 to 255',

    bool_1 = FALSE, -- BOOL,
    bool_2 = FALSE, -- BOOLEAN,

    smallint_1   = -32768, -- SMALLINT          COMMENT '-32768 to 32767',
    u_smallint_1 = 0,      -- SMALLINT UNSIGNED COMMENT '0 to 65535',

    mediumint_1   = -8388608, -- MEDIUMINT          COMMENT '-8388608 to 8388607',
    u_mediumint_1 = 0,        -- MEDIUMINT UNSIGNED COMMENT '0 to 16777215',

    int_1   = -2147483648, -- INT          COMMENT '-2147483648 to 2147483647',
    u_int_1 = 0,           -- INT UNSIGNED COMMENT '0 to 4294967295',

    integer_1   = -2147483648, -- INTEGER COMMENT '-2147483648 to 2147483647',
    u_integer_1 = 0,           -- INTEGER UNSIGNED COMMENT '0 to 4294967295',

    bigint_1   = -9223372036854775808, -- BIGINT          COMMENT '-9223372036854775808 to 9223372036854775807',
    u_bigint_1 = 0,                    -- BIGINT UNSIGNED COMMENT '0 to 18446744073709551615',

    serial_1 = 0, -- SERIAL COMMENT 'i.e. BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE. 0 to 18446744073709551615',

    decimal_1 = -9.99, -- DECIMAL(3,2) COMMENT 'exact fixed-point number',
    dec_1     = -9.99, -- DEC(3,2)     COMMENT 'alias for DECIMAL',
    fixed_1   = -9.99, -- FIXED(3,2)   COMMENT 'alias for DECIMAL',
    numeric_1 = -9.99, -- NUMERIC(3,2) COMMENT 'alias for DECIMAL',

    float_1   = -3.402823466E+38, -- FLOAT          COMMENT '-3.402823466E+38 to -1.175494351E-38, 0, and 1.175494351E-38 to 3.402823466E+38',
    u_float_1 = 0,                -- FLOAT UNSIGNED COMMENT '                                      0, and 1.175494351E-38 to 3.402823466E+38',

    double_1   = -1.7976931348623157E+308, -- DOUBLE          COMMENT '-1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',
    u_double_1 = 0,                        -- DOUBLE UNSIGNED COMMENT '                                                      0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',

    real_1   = -1.7976931348623157E+308, -- REAL          COMMENT 'alias for          DOUBLE i.e. -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',
    u_real_1 = 0,                        -- REAL UNSIGNED COMMENT 'alias for UNSIGNED DOUBLE i.e.                                                       0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',

    date_1      = '1970-01-01',          -- DATE      COMMENT '1000-01-01 to 9999-12-31',
    datetime_1  = '1970-01-01 00:00:00', -- DATETIME  COMMENT '1000-01-01 00:00:00 to 9999-12-31 23:59:59',
    timestamp_1 = '1970-01-01 00:00:01', -- TIMESTAMP COMMENT '1970-01-01 00:00:01 UTC to 2038-01-19 03:14:07 UTC',
    time_1      = '-838:59:59',          -- TIME      COMMENT '-838:59:59 to 838:59:59',
    year_1      = 1901,                  -- YEAR      COMMENT '1901 to 2155, and 0000',

    char_0 = '',  -- CHAR(0),
    char_1 = '', -- CHAR(1),
    char_2 = '', -- CHAR(255),

    varchar_0 = '', -- VARCHAR(0),
    varchar_1 = '', -- VARCHAR(1),

    binary_0 = '', -- BINARY(0) COMMENT 'similar to CHAR',
    binary_1 = '', -- BINARY(1) COMMENT 'similar to CHAR',
    binary_2 = '', -- BINARY(255) COMMENT 'similar to CHAR',

    varbinary_0 = '', -- VARBINARY(0) COMMENT 'similar to VARCHAR',
    varbinary_1 = '', -- VARBINARY(1) COMMENT 'similar to VARCHAR',

    tinyblob_1 = '', -- TINYBLOB COMMENT 'maximum length of 255 (2^8 ? 1) bytes',
    tinytext_1 = '', -- TINYTEXT COMMENT 'maximum length of 255 (2^8 ? 1) characters',

    blob_1 = '', -- BLOB COMMENT 'maximum length of 65,535 (2^16 ? 1) bytes',
    text_1 = '', -- TEXT COMMENT 'maximum length of 65,535 (2^16 ? 1) characters',

    mediumblob_1 = '', -- MEDIUMBLOB COMMENT 'maximum length of 16,777,215 (2^24 ? 1) bytes',
    mediumtext_1 = '', -- MEDIUMTEXT COMMENT 'maximum length of 16,777,215 (2^24 ? 1) characters',

    longblob_1 = '', -- LONGBLOB COMMENT 'maximum length of 4,294,967,295 or 4GB (2^32 ? 1) bytes',
    longtext_1 = '' -- LONGTEXT COMMENT 'maximum length of 4,294,967,295 or 4GB (2^32 ? 1) characters'
;

INSERT INTO enum_datatypes SET
    enum_1 = NULL -- ENUM('a', 'b', 'c', 'd', 'e', 'f') COMMENT 'can have a maximum of 65,535 distinct elements'
;

INSERT INTO set_datatypes SET
    set_1 = '' -- SET('a', 'b', 'c', 'd', 'e', 'f') COMMENT 'can have a maximum of 64 distinct members'
;

INSERT INTO json_datatypes SET
    json_1 = '{}' -- JSON
;

INSERT INTO long_varchar_datatypes SET
    varchar_2 = ""
;

INSERT INTO long_varbinary_datatypes SET
    varbinary_2 = ""
;
```

Insert maximum acceptable values into the test table:

```mysql
-- MAX values
INSERT INTO datatypes SET

    bit_1 = 0b1, -- BIT(1),
    bit_2 = 0b1111111111111111111111111111111111111111111111111111111111111111, -- BIT(64),

    tinyint_1   = 127, -- TINYINT          COMMENT '-128 to 127',
    u_tinyint_1 = 255, -- TINYINT UNSIGNED COMMENT '0 to 255',

    bool_1 = TRUE, -- BOOL,
    bool_2 = TRUE, -- BOOLEAN,

    smallint_1   = 32767, -- SMALLINT          COMMENT '-32768 to 32767',
    u_smallint_1 = 65535, -- SMALLINT UNSIGNED COMMENT '0 to 65535',

    mediumint_1   =  8388607, -- MEDIUMINT          COMMENT '-8388608 to 8388607',
    u_mediumint_1 = 16777215, -- MEDIUMINT UNSIGNED COMMENT '0 to 16777215',

    int_1   = 2147483647, -- INT          COMMENT '-2147483648 to 2147483647',
    u_int_1 = 4294967295, -- INT UNSIGNED COMMENT '0 to 4294967295',

    integer_1   = 2147483647, -- INTEGER COMMENT '-2147483648 to 2147483647',
    u_integer_1 = 4294967295, -- INTEGER UNSIGNED COMMENT '0 to 4294967295',

    bigint_1   =  9223372036854775807, -- BIGINT          COMMENT '-9223372036854775808 to 9223372036854775807',
    u_bigint_1 = 18446744073709551615, -- BIGINT UNSIGNED COMMENT '0 to 18446744073709551615',

    serial_1 = 18446744073709551615, -- SERIAL COMMENT 'i.e. BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE. 0 to 18446744073709551615',

    decimal_1 = 9.99, -- DECIMAL(3,2) COMMENT 'exact fixed-point number',
    dec_1     = 9.99, -- DEC(3,2)     COMMENT 'alias for DECIMAL',
    fixed_1   = 9.99, -- FIXED(3,2)   COMMENT 'alias for DECIMAL',
    numeric_1 = 9.99, -- NUMERIC(3,2) COMMENT 'alias for DECIMAL',

    float_1   = 3.402823466E+38, -- FLOAT          COMMENT '-3.402823466E+38 to -1.175494351E-38, 0, and 1.175494351E-38 to 3.402823466E+38',
    u_float_1 = 3.402823466E+38, -- FLOAT UNSIGNED COMMENT '                                      0, and 1.175494351E-38 to 3.402823466E+38',

    double_1   = 1.7976931348623157E+308, -- DOUBLE          COMMENT '-1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',
    u_double_1 = 1.7976931348623157E+308, -- DOUBLE UNSIGNED COMMENT '                                                      0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',

    real_1   = 1.7976931348623157E+308, -- REAL          COMMENT 'alias for          DOUBLE i.e. -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',
    u_real_1 = 1.7976931348623157E+308, -- REAL UNSIGNED COMMENT 'alias for UNSIGNED DOUBLE i.e.                                                       0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',

    date_1      = '2149-06-01',          -- DATE      COMMENT '1000-01-01 to 9999-12-31',
    datetime_1  = '2106-02-01 23:59:59', -- DATETIME  COMMENT '1000-01-01 00:00:00 to 9999-12-31 23:59:59',
    timestamp_1 = '2038-01-19 03:14:07', -- TIMESTAMP COMMENT '1970-01-01 00:00:01 UTC to 2038-01-19 03:14:07 UTC',
    time_1      = '838:59:59',           -- TIME      COMMENT '-838:59:59 to 838:59:59',
    year_1      = 2155,                  -- YEAR      COMMENT '1901 to 2155, and 0000',

    char_0 = '',  -- CHAR(0),
    char_1 = 'a', -- CHAR(1),
    char_2 = 'abcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcde', -- CHAR(255),

    varchar_0 = '', -- VARCHAR(0),
    varchar_1 = 'a', -- VARCHAR(1),

    binary_0 = '',  -- BINARY(0) COMMENT 'similar to CHAR',
    binary_1 = 'a', -- BINARY(1) COMMENT 'similar to CHAR',
    binary_2 = 'abcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcdefghijklmnopqrstuvwxyabcde', -- BINARY(255) COMMENT 'similar to CHAR',

    varbinary_0 = '',  -- VARBINARY(0) COMMENT 'similar to VARCHAR',
    varbinary_1 = 'a', -- VARBINARY(1) COMMENT 'similar to VARCHAR',

    tinyblob_1 = 'a', -- TINYBLOB COMMENT 'maximum length of 255 (2^8 ? 1) bytes',
    tinytext_1 = 'a', -- TINYTEXT COMMENT 'maximum length of 255 (2^8 ? 1) characters',

    blob_1 = 'a', -- BLOB COMMENT 'maximum length of 65,535 (2^16 ? 1) bytes',
    text_1 = 'a', -- TEXT COMMENT 'maximum length of 65,535 (2^16 ? 1) characters',

    mediumblob_1 = 'a', -- MEDIUMBLOB COMMENT 'maximum length of 16,777,215 (2^24 ? 1) bytes',
    mediumtext_1 = 'a', -- MEDIUMTEXT COMMENT 'maximum length of 16,777,215 (2^24 ? 1) characters',

    longblob_1 = 'a', -- LONGBLOB COMMENT 'maximum length of 4,294,967,295 or 4GB (2^32 ? 1) bytes',
    longtext_1 = 'a'  -- LONGTEXT COMMENT 'maximum length of 4,294,967,295 or 4GB (2^32 ? 1) characters'
;

INSERT INTO enum_datatypes SET
    enum_1 = 'a' -- ENUM('a', 'b', 'c', 'd', 'e', 'f') COMMENT 'can have a maximum of 65,535 distinct elements'
;

INSERT INTO set_datatypes SET
    set_1 = 'a,b,c' -- SET('a', 'b', 'c', 'd', 'e', 'f') COMMENT 'can have a maximum of 64 distinct members',
;

INSERT INTO json_datatypes SET
    json_1 = '{"a":1, "b":2, "c":3}' -- JSON
;

INSERT INTO long_varchar_datatypes SET
    varchar_2 = "abc"
;

INSERT INTO long_varbinary_datatypes SET
    varbinary_2 = "abc"
;
```

### ClickHouse Test Tables

```sql
CREATE TABLE datatypes(
    bit_1 Nullable(String), -- bit_1 BIT(1),
    bit_2 Nullable(String), -- bit_2 BIT(64),

    tinyint_1   Nullable(Int8),  -- tinyint_1   TINYINT          COMMENT '-128 to 127',
    u_tinyint_1 Nullable(UInt8), -- u_tinyint_1 TINYINT UNSIGNED COMMENT '0 to 255',

    bool_1 Nullable(UInt8), -- bool_1 BOOL,
    bool_2 Nullable(UInt8), -- bool_2 BOOLEAN,

    smallint_1   Nullable(Int16),  -- smallint_1   SMALLINT           COMMENT '-32768 to 32767',
    u_smallint_1 Nullable(UInt16), -- u_smallint_1 SMALLINT UNSIGNED  COMMENT '0 to 65535',

    mediumint_1   Nullable(Int32),  -- mediumint_1   MEDIUMINT          COMMENT '-8388608 to 8388607',
    u_mediumint_1 Nullable(UInt32), -- u_mediumint_1 MEDIUMINT UNSIGNED COMMENT '0 to 16777215',

    int_1   Nullable(Int32),  -- int_1   INT          COMMENT '-2147483648 to 2147483647',
    u_int_1 Nullable(UInt32), -- u_int_1 INT UNSIGNED COMMENT '0 to 4294967295',

    integer_1   Nullable(Int32),  -- integer_1   INTEGER          COMMENT '-2147483648 to 2147483647',
    u_integer_1 Nullable(UInt32), -- u_integer_1 INTEGER UNSIGNED COMMENT '0 to 4294967295',

    bigint_1   Nullable(Int64),  -- bigint_1   BIGINT          COMMENT '-9223372036854775808 to 9223372036854775807',
    u_bigint_1 Nullable(UInt64), -- u_bigint_1 BIGINT UNSIGNED COMMENT '0 to 18446744073709551615',

    serial_1 Nullable(UInt64), -- serial_1 SERIAL COMMENT 'i.e. BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE. 0 to 18446744073709551615',

    decimal_1 Nullable(String), -- decimal_1 DECIMAL(3,2) COMMENT 'exact fixed-point number',
    dec_1     Nullable(String), -- dec_1     DEC(3,2)     COMMENT 'alias for DECIMAL',
    fixed_1   Nullable(String), -- fixed_1   FIXED(3,2)   COMMENT 'alias for DECIMAL',
    numeric_1 Nullable(String), -- numeric_1 NUMERIC(3,2) COMMENT 'alias for DECIMAL',

    float_1   Nullable(Float32), -- float_1   FLOAT          COMMENT '-3.402823466E+38 to -1.175494351E-38, 0, and 1.175494351E-38 to 3.402823466E+38',
    u_float_1 Nullable(Float32), -- u_float_1 FLOAT UNSIGNED COMMENT '                                      0, and 1.175494351E-38 to 3.402823466E+38',

    double_1   Nullable(Float64), -- double_1   DOUBLE          COMMENT '-1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',
    u_double_1 Nullable(Float64), -- u_double_1 DOUBLE UNSIGNED COMMENT '                                                      0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',

    real_1   Nullable(Float64), -- real_1   REAL          COMMENT 'alias for          DOUBLE i.e. -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',
    u_real_1 Nullable(Float64), -- u_real_1 REAL UNSIGNED COMMENT 'alias for UNSIGNED DOUBLE i.e.                                                       0, and 2.2250738585072014E-308 to 1.7976931348623157E+308',

    date_1      Nullable(Date),     -- date_1      DATE      COMMENT '1000-01-01 to 9999-12-31',
    datetime_1  Nullable(DateTime), -- datetime_1  DATETIME  COMMENT '1000-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999',
    timestamp_1 Nullable(DateTime), -- timestamp_1 TIMESTAMP COMMENT '1970-01-01 00:00:01.000000 UTC to 2038-01-19 03:14:07.999999 UTC',
    time_1      Nullable(String),   -- time_1      TIME      COMMENT '-838:59:59.000000 to 838:59:59.000000',
    year_1      Nullable(UInt16),   -- year_1      YEAR      COMMENT '1901 to 2155, and 0000',

    char_0 Nullable(FixedString(1)),   -- char_0 CHAR(0),
    char_1 Nullable(FixedString(1)),   -- char_1 CHAR(1),
    char_2 Nullable(FixedString(255)), -- char_2 CHAR(255),

    varchar_0 Nullable(String), -- varchar_0 VARCHAR(0),
    varchar_1 Nullable(String), -- varchar_1 VARCHAR(1),

    binary_0 Nullable(String), -- binary_0 BINARY(0)   COMMENT 'similar to CHAR',
    binary_1 Nullable(String), -- binary_1 BINARY(1)   COMMENT 'similar to CHAR',
    binary_2 Nullable(String), -- binary_2 BINARY(255) COMMENT 'similar to CHAR',

    varbinary_0 Nullable(String), -- varbinary_0 VARBINARY(0) COMMENT 'similar to VARCHAR',
    varbinary_1 Nullable(String), -- varbinary_1 VARBINARY(1) COMMENT 'similar to VARCHAR',

    tinyblob_1 Nullable(String), -- tinyblob_1 TINYBLOB COMMENT 'maximum length of 255 (2^8 ? 1) bytes',
    tinytext_1 Nullable(String), -- tinytext_1 TINYTEXT COMMENT 'maximum length of 255 (2^8 ? 1) characters',

    blob_1 Nullable(String), -- blob_1 BLOB COMMENT 'maximum length of 65,535 (2^16 ? 1) bytes',
    text_1 Nullable(String), -- text_1 TEXT COMMENT 'maximum length of 65,535 (2^16 ? 1) characters',

    mediumblob_1 Nullable(String), -- mediumblob_1 MEDIUMBLOB COMMENT 'maximum length of 16,777,215 (2^24 ? 1) bytes',
    mediumtext_1 Nullable(String), -- mediumtext_1 MEDIUMTEXT COMMENT 'maximum length of 16,777,215 (2^24 ? 1) characters',

    longblob_1 Nullable(String), -- longblob_1 LONGBLOB COMMENT 'maximum length of 4,294,967,295 or 4GB (2^32 ? 1) bytes',
    longtext_1 Nullable(String)  -- longtext_1 LONGTEXT COMMENT 'maximum length of 4,294,967,295 or 4GB (2^32 ? 1) characters',

) ENGINE = Log
;

CREATE TABLE enum_datatypes(
    enum_1 Enum16('a'=1, 'b'=2, 'c'=3, 'd'=4, 'e'=5, 'f'=6) -- enum_1 ENUM('a', 'b', 'c', 'd', 'e', 'f') COMMENT 'can have a maximum of 65,535 distinct elements',
) ENGINE = Memory
;

CREATE TABLE set_datatypes(
    set_1 Array(Enum16('a'=1, 'b'=2, 'c'=3, 'd'=4, 'e'=5, 'f'=6)) -- set_1 SET('a', 'b', 'c', 'd', 'e', 'f') COMMENT ' can have a maximum of 64 distinct members',
) ENGINE = Memory
;

CREATE TABLE set_datatypes(
    set_1 String -- set_1 SET('a', 'b', 'c', 'd', 'e', 'f') COMMENT ' can have a maximum of 64 distinct members',
) ENGINE = Memory
;


CREATE TABLE json_datatypes(
    json_1 String -- json_1 JSON
) ENGINE = Memory
;

CREATE TABLE long_varchar_datatypes(
    varchar_2 String
) ENGINE = Memory
;

CREATE TABLE long_varbinary_datatypes(
    varbinary_2 String
) ENGINE = Memory
;
```
