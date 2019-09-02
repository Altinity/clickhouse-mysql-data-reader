# clickhouse-mysql 2019-09-03

## improvements
* fix --src-tables-where-clauses to accept both filenames (for long where-clauses) and where-cluases themselves (for shorted clauses)

## bugfixes
* fix --src-port CLI option
* ensure UTF8 for source migration

# clickhouse-mysql 2019-03-25

## new features
* added new CLI option `--dst-schema` - make table full name change to `schema`.`db__table` 
* added new CLI option `--dst-cluster` - support table create on cluster
* added new CLI option `--dst-distribute` - make table full name change to `schema_all`.`db__table_all`,and engine change to Distributed

# clickhouse-mysql 2018-03-14

## new features
* added new CLI option `--create-table-sql` - make attempt to prepare ready-to-use **CREATE TABLE** statement
* added new CLI option `--pump-data` - specifies that we'd like to pump data into ClickHouse. Was default behaviour previously 
* added new CLI option `--install` - Install service file(s)
* added new CLI option `--dst-create-table` - tries to automatically create target table in ClickHouse before any data inserted
  
## improvements
* modified/added new CLI option `--with-create-database` - used in combination with `--create-table-sql*` options in order to add **CREATE DATABASE** statement in additon to **CREATE TABLE**
* modified/added new CLI option `--create-table-json-template` - prepare JSON **CREATE TABLE** data
* modified/added new CLI option `--migrate-table` - was called `--table-migrate` previously
* modified/added new CLI option `--create-table-sql-template` - prepare **CREATE TABLE** template

## bugfixes
* config files vs CLI options order fixed
 
