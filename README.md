# clickhouse-mysql-data-reader
utility to read mysql data


pip install mysql-replication

you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation

grant replication client, replication slave, super on *.* to 'reader'@'localhost' identified by 'qwerty';
flush privileges;


https://github.com/noplay/python-mysql-replication
https://github.com/mymarilyn/clickhouse-driver

=======
MySQL data types

Numeric Types

BIT  the number of bits per value, from 1 to 64
TINYINT -128 to 127. The unsigned range is 0 to 255
BOOL, BOOLEAN synonyms for TINYINT(1)
SMALLINT  -32768 to 32767. The unsigned range is 0 to 65535
MEDIUMINT -8388608 to 8388607. The unsigned range is 0 to 16777215.
INT, INTEGER -2147483648 to 2147483647. The unsigned range is 0 to 4294967295
BIGINT  -9223372036854775808 to 9223372036854775807. The unsigned range is 0 to 18446744073709551615

SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
DEC, DECIMAL, FIXED, NUMERIC A packed ?exact? fixed-point number
FLOAT  Permissible values are -3.402823466E+38 to -1.175494351E-38, 0, and 1.175494351E-38 to 3.402823466E+38
DOUBLE, REAL  Permissible values are -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308


Date and Time Types

DATE The supported range is '1000-01-01' to '9999-12-31'
DATETIME The supported range is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
TIMESTAMP The range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999'
TIME The range is '-838:59:59.000000' to '838:59:59.000000'
YEAR  Values display as 1901 to 2155, and 0000


String Types
CHAR The range of M is 0 to 255. If M is omitted, the length is 1.
VARCHAR The range of M is 0 to 65,535
BINARY similar to CHAR
VARBINARY similar to VARCHAR
TINYBLOB  maximum length of 255
TINYTEXT maximum length of 255
BLOB maximum length of 65,535
TEXT maximum length of 65,535
MEDIUMBLOB maximum length of 16,777,215
MEDIUMTEXT maximum length of 16,777,215
LONGBLOB maximum length of 4,294,967,295 or 4GB
LONGTEXT maximum length of 4,294,967,295 or 4GB
ENUM can have a maximum of 65,535 distinct elements
SET can have a maximum of 64 distinct members


JSON native JSON data type defined by RFC 7159

=========
CH data types

Date number of days since 1970-01-01
DateTime Unix timestamp
Enum8 or Enum16. A set of enumerated string values that are stored as Int8 or Int16. The numeric values must be within -128..127 for Enum8 and -32768..32767 for Enum16
FixedString(N) string of N bytes (not characters or code points)
Float32, Float64
Int8	-128	127
Int16	-32768	32767
Int32	-2147483648	2147483647
Int64	-9223372036854775808	9223372036854775807
UInt8	0	255
UInt16	0	65535
UInt32	0	4294967295
UInt64	0	18446744073709551615
String The length is not limited. The value can contain an arbitrary set of bytes, including null bytes


