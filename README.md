# clickhouse-mysql-data-reader
utility to read mysql data


pip install mysql-replication

you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation

CREATE USER 'reader'@'localhost' IDENTIFIED BY 'qwerty';
CREATE USER 'reader'@'127.0.0.1' IDENTIFIED BY 'qwerty';
CREATE USER 'reader'@'*' IDENTIFIED BY 'qwerty';
grant replication client, replication slave, super on *.* to 'reader'@'localhost';
grant replication client, replication slave, super on *.* to 'reader'@'127.0.0.1';
grant replication client, replication slave, super on *.* to 'reader'@'*';
flush privileges;

grant replication client, replication slave, super on *.* to 'reader'@'localhost' identified by 'qwerty';
grant replication client, replication slave, super on *.* to 'reader'@'127.0.0.1' identified by 'qwerty';
grant replication client, replication slave, super on *.* to 'reader'@'*'         identified by 'qwerty';
flush privileges;


https://github.com/noplay/python-mysql-replication
https://github.com/mymarilyn/clickhouse-driver

[mysqld]
server-id		 = 1
log_bin			 = /var/log/mysql/mysql-bin.log
expire_logs_days = 10
max_binlog_size  = 100M
binlog-format    = row #Very important if you want to receive write, update and delete row events

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
Float32, Float64

Int8	-128	127
UInt8	0	255

Int16	-32768	32767
UInt16	0	65535

Int32	-2147483648	2147483647
UInt32	0	4294967295

Int64	-9223372036854775808	9223372036854775807
UInt64	0	18446744073709551615

FixedString(N) string of N bytes (not characters or code points)
String The length is not limited. The value can contain an arbitrary set of bytes, including null bytes


==========================
MySQL -> CH data types mapping

Numeric Types

BIT  -> ??? (possibly String?)
TINYINT -> Int8 UInt8
BOOL, BOOLEAN -> UInt8
SMALLINT  -> Int16 UInt16
MEDIUMINT -> Int32 UInt32
INT, INTEGER -> Int32 UInt32
BIGINT -> Int64 UInt64

SERIAL -> UInt64
DEC, DECIMAL, FIXED, NUMERIC -> ???? (possibly String?)
FLOAT -> Float32
DOUBLE, REAL -> Float64


Date and Time Types

DATE -> Date (for valid values) or String (Date Allows storing values from just after the beginning of the Unix Epoch to the upper threshold defined by a constant at the compilation stage (currently, this is until the year 2038, but it may be expanded to 2106))
DATETIME -> DateTime (for valid values) or String
TIMESTAMP -> DateTime
TIME -> ????? (possibly String?)
YEAR  -> UInt16


String Types

CHAR -> FixedString
VARCHAR -> String
BINARY -> String
VARBINARY -> String
TINYBLOB -> String
TINYTEXT -> String
BLOB -> String
TEXT -> String
MEDIUMBLOB -> String
MEDIUMTEXT -> String
LONGBLOB -> String
LONGTEXT -> String

ENUM -> Enum8 Enum16
SET -> Array(Int8)

JSON -> ?????? (possibly String?)



ERROR 1118 (42000): Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs


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

ERROR 1118 (42000): Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs

CREATE TABLE long_varchar_datatypes(
    varchar_2 VARCHAR(65532)
)
;

CREATE TABLE long_varbinary_datatypes(
    varbinary_2 VARBINARY(65532) COMMENT 'similar to VARCHAR'
)
;



-- in order to be able to set timestamp = '1970-01-01 00:00:01'
set time_zone='+00:00';

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

===========================

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

) ENGINE = Memory
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

MySQL section

mysqlimport --ignore-lines=1 \
            --fields-terminated-by=, \
            --fields-enclosed-by=\" \
            --local -u root \
            -p Database \
             TableName.csv


ls|sort|head -n 100
i=1
for file in $(ls *.csv|sort|head -n 100); do
    echo "$i. Copy $file"
    cp -f $file ontime
    echo "$i. Import $file"
    mysqlimport --ignore-lines=1 --fields-terminated-by=, --fields-enclosed-by=\" --local -u root airline ontime
    rm -f ontime
    i=$((i+1))
done



 load data local infile 'file.csv' into table table
 fields terminated by ','
 enclosed by '"'
 lines terminated by '\n'
 (column1, column2, column3,...)


CREATE DATABASE IF NOT EXISTS `airline`;
CREATE TABLE IF NOT EXISTS `airline`.`ontime` (
  `Year`                 SMALLINT UNSIGNED, -- UInt16,
  `Quarter`              TINYINT UNSIGNED, -- UInt8,
  `Month`                TINYINT UNSIGNED, -- UInt8,
  `DayofMonth`           TINYINT UNSIGNED, -- UInt8,
  `DayOfWeek`            TINYINT UNSIGNED, -- UInt8,
  `FlightDate`           DATE, -- Date,
  `UniqueCarrier`        LONGTEXT, -- String,
  `AirlineID`            INTEGER UNSIGNED, -- UInt32,
  `Carrier`              LONGTEXT, -- String,
  `TailNum`              LONGTEXT, -- String,
  `FlightNum`            LONGTEXT, -- String,
  `OriginAirportID`      INTEGER UNSIGNED, -- UInt32,
  `OriginAirportSeqID`   INTEGER UNSIGNED, -- UInt32,
  `OriginCityMarketID`   INTEGER UNSIGNED, -- UInt32,
  `Origin`               LONGTEXT, -- String,
  `OriginCityName`       LONGTEXT, -- String,
  `OriginState`          LONGTEXT, -- String,
  `OriginStateFips`      LONGTEXT, -- String,
  `OriginStateName`      LONGTEXT, -- String,
  `OriginWac`            INTEGER UNSIGNED, -- UInt32,
  `DestAirportID`        INTEGER UNSIGNED, -- UInt32,
  `DestAirportSeqID`     INTEGER UNSIGNED, -- UInt32,
  `DestCityMarketID`     INTEGER UNSIGNED, -- UInt32,
  `Dest`                 LONGTEXT, -- String,
  `DestCityName`         LONGTEXT, -- String,
  `DestState`            LONGTEXT, -- String,
  `DestStateFips`        LONGTEXT, -- String,
  `DestStateName`        LONGTEXT, -- String,
  `DestWac`              INTEGER UNSIGNED, -- UInt32,
  `CRSDepTime`           INTEGER UNSIGNED, -- UInt32,
  `DepTime`              INTEGER UNSIGNED, -- UInt32,
  `DepDelay`             FLOAT, -- Float32,
  `DepDelayMinutes`      FLOAT, -- Float32,
  `DepDel15`             FLOAT, -- Float32,
  `DepartureDelayGroups` INTEGER, -- Int32,
  `DepTimeBlk`           LONGTEXT, -- String,
  `TaxiOut`              FLOAT, -- Float32,
  `WheelsOff`            INTEGER UNSIGNED, -- UInt32,
  `WheelsOn`             INTEGER UNSIGNED, -- UInt32,
  `TaxiIn`               FLOAT, -- Float32,
  `CRSArrTime`           INTEGER UNSIGNED, -- UInt32,
  `ArrTime`              INTEGER UNSIGNED, -- UInt32,
  `ArrDelay`             FLOAT, -- Float32,
  `ArrDelayMinutes`      FLOAT, -- Float32,
  `ArrDel15`             FLOAT, -- Float32,
  `ArrivalDelayGroups`   INTEGER, -- Int32,
  `ArrTimeBlk`           LONGTEXT, -- String,
  `Cancelled`            FLOAT, -- Float32,
  `CancellationCode`     LONGTEXT, -- String,
  `Diverted`             FLOAT, -- Float32,
  `CRSElapsedTime`       FLOAT, -- Float32,
  `ActualElapsedTime`    FLOAT, -- Float32,
  `AirTime`              FLOAT, -- Float32,
  `Flights`              FLOAT, -- Float32,
  `Distance`             FLOAT, -- Float32,
  `DistanceGroup`        FLOAT, -- Float32,
  `CarrierDelay`         FLOAT, -- Float32,
  `WeatherDelay`         FLOAT, -- Float32,
  `NASDelay`             FLOAT, -- Float32,
  `SecurityDelay`        FLOAT, -- Float32,
  `LateAircraftDelay`    FLOAT, -- Float32,
  `FirstDepTime`         LONGTEXT, -- String,
  `TotalAddGTime`        LONGTEXT, -- String,
  `LongestAddGTime`      LONGTEXT, -- String,
  `DivAirportLandings`   LONGTEXT, -- String,
  `DivReachedDest`       LONGTEXT, -- String,
  `DivActualElapsedTime` LONGTEXT, -- String,
  `DivArrDelay`          LONGTEXT, -- String,
  `DivDistance`          LONGTEXT, -- String,
  `Div1Airport`          LONGTEXT, -- String,
  `Div1AirportID`        INTEGER UNSIGNED, -- UInt32,
  `Div1AirportSeqID`     INTEGER UNSIGNED, -- UInt32,
  `Div1WheelsOn`         LONGTEXT, -- String,
  `Div1TotalGTime`       LONGTEXT, -- String,
  `Div1LongestGTime`     LONGTEXT, -- String,
  `Div1WheelsOff`        LONGTEXT, -- String,
  `Div1TailNum`          LONGTEXT, -- String,
  `Div2Airport`          LONGTEXT, -- String,
  `Div2AirportID`        INTEGER UNSIGNED, -- UInt32,
  `Div2AirportSeqID`     INTEGER UNSIGNED, -- UInt32,
  `Div2WheelsOn`         LONGTEXT, -- String,
  `Div2TotalGTime`       LONGTEXT, -- String,
  `Div2LongestGTime`     LONGTEXT, -- String,
  `Div2WheelsOff`        LONGTEXT, -- String,
  `Div2TailNum`          LONGTEXT, -- String,
  `Div3Airport`          LONGTEXT, -- String,
  `Div3AirportID`        INTEGER UNSIGNED, -- UInt32,
  `Div3AirportSeqID`     INTEGER UNSIGNED, -- UInt32,
  `Div3WheelsOn`         LONGTEXT, -- String,
  `Div3TotalGTime`       LONGTEXT, -- String,
  `Div3LongestGTime`     LONGTEXT, -- String,
  `Div3WheelsOff`        LONGTEXT, -- String,
  `Div3TailNum`          LONGTEXT, -- String,
  `Div4Airport`          LONGTEXT, -- String,
  `Div4AirportID`        INTEGER UNSIGNED, -- UInt32,
  `Div4AirportSeqID`     INTEGER UNSIGNED, -- UInt32,
  `Div4WheelsOn`         LONGTEXT, -- String,
  `Div4TotalGTime`       LONGTEXT, -- String,
  `Div4LongestGTime`     LONGTEXT, -- String,
  `Div4WheelsOff`        LONGTEXT, -- String,
  `Div4TailNum`          LONGTEXT, -- String,
  `Div5Airport`          LONGTEXT, -- String,
  `Div5AirportID`        INTEGER UNSIGNED, -- UInt32,
  `Div5AirportSeqID`     INTEGER UNSIGNED, -- UInt32,
  `Div5WheelsOn`         LONGTEXT, -- String,
  `Div5TotalGTime`       LONGTEXT, -- String,
  `Div5LongestGTime`     LONGTEXT, -- String,
  `Div5WheelsOff`        LONGTEXT, -- String,
  `Div5TailNum`          LONGTEXT  -- String
);
