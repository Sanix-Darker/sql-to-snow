import re

# General RegExes
comment_line_re = re.compile('^\s*--.*$', re.IGNORECASE)
whitespace_line_re = re.compile('^\s*$', re.IGNORECASE)
comma_line_re = re.compile('^\s*,\s*$', re.IGNORECASE)

# RegExes for mysql dialect that Snowflake doesn't support
engine_re = re.compile('(.*)(engine\s*=[a-zA-Z]*\s*(?:DEFAULT)?)(.*)', re.IGNORECASE)

# RegExes for Oracle dialect that Snowflake doesn't support

# VARCHAR2(n BYTE) => VARCHAR(n)
varchar2_re = re.compile('(.*)(VARCHAR2\((\d+)(\s+.+)?\))(.*)', re.IGNORECASE)

# CHAR(n BYTE) => CHAR(n)
char_re = re.compile('(.*)(CHAR\((\d+)(\s+.+)\))(.*)', re.IGNORECASE)

# DEFAULT SYSDATE => deleted (OK only because data loaded from table should already have date)
# Snowflake DEFAULT must be literal
default_sysdate_re = re.compile('(.*)\ (DEFAULT SYSDATE)\ (.*)', re.IGNORECASE)

# SYSDATE => CURRENT_TIMESTAMP()
# sysdate_re = re.compile('(.*)\ (SYSDATE)\ (.*)', re.IGNORECASE)
sysdate_re = re.compile('(.*[,\(\s])(SYSDATE)([,\)\s].*)', re.IGNORECASE)

# SEGMENT CREATION type => ignore
segment_creation_re = re.compile('(.*)\ (SEGMENT\s+CREATION\s+(?:IMMEDIATE|DEFERRED))(.*)', re.IGNORECASE)

# NOT NULL ENABLE => NOT NULL
not_null_enable_re = re.compile('(.*)(NOT\s+NULL\s+ENABLE)(.*)', re.IGNORECASE)

# find prior period, e.g. trunc(col,'MM')-1 => dateadd('MM', -1, trunc(col, 'MM'))
prior_period_re = re.compile('(.*)(TRUNC\(\s*(.+?),\s*(\'.+?\')\s*\)\s*(-?\s*\d+))(.*)', re.IGNORECASE)

# add months, e.g. add_months(trunc(col, 'MM'), -5) => dateadd(month, -5, col)
add_months_re = re.compile('(.*)(ADD_MONTHS\(\s*TRUNC\(\s*(.+?),\s*(\'.+?\')\s*\),\s*(-?\s*\d+))(.*)', re.IGNORECASE)

# STORAGE => ignore through end of clause
storage_multiline_re = re.compile('(^\s*)(STORAGE\s*\(\s*)(.*)', re.IGNORECASE)

# PARTITION BY RANGE => ignore through end of clause
partition_by_range_multiline_re = re.compile('(^\s*)(PARTITION BY RANGE\s*\(\s*)(.*)', re.IGNORECASE)

# PARTITION ... VALUES => ignore through end of clause
partition_values_multiline_re = re.compile('(^\s*)(PARTITION .* VALUES LESS THAN\s*\(.*\))(.*)', re.IGNORECASE)

# LOB ... STORE AS => ignore through end of clause
lob_multiline_re = re.compile('(^\s*)(LOB \(.*\) STORE AS )(.*)', re.IGNORECASE)

# PCTFREE n => ignore
pctfree_re = re.compile('(.*)(PCTFREE\s+\S+)(.*)', re.IGNORECASE)

# PCTIUSED n => ignore
pctused_re = re.compile('(.*)(PCTUSED\s+\S+)(.*)', re.IGNORECASE)

# INITRANS n => ignore
initrans_re = re.compile('(.*)(INITRANS\s+\S+)(.*)', re.IGNORECASE)

# MAXTRANS n => ignore
maxtrans_re = re.compile('(.*)(MAXTRANS\s+\S+)(.*)', re.IGNORECASE)

# TABLESPACE n => ignore
tablespace_re = re.compile('(.*)(TABLESPACE\s+\S+)(.*)', re.IGNORECASE)

# LOGGING => ignore
logging_re = re.compile('(.*)(LOGGING)(.*)', re.IGNORECASE)

# NOCOMPRESS => ignore
nocompress_re = re.compile('(.*)(NOCOMPRESS)(.*)', re.IGNORECASE)

# CACHE => ignore
cache_re = re.compile('(.*\s+)(CACHE\s+)(.*)', re.IGNORECASE)

# USINGnn => ignore (e.g. USING90, USING10)
usingnn_re = re.compile('(.*\s+)(USING\d\d\s+)(.*)', re.IGNORECASE)

# COMPUTE STATISTICS => ignore
compute_statistics_re = re.compile('(.*)(COMPUTE STATISTICS)(.*)', re.IGNORECASE)

# Empty Comma => ignore (dropping out clauses can leave an empty comma)
empty_comma_re = re.compile('(\s*)(,)\s+(--.*)', re.IGNORECASE)

# RegExes for SQL-Server dialect that Snowflake doesn't support

# NULL (explicit NULL constraint) -- ignore
null_constraint_re = re.compile('(.*)((?<!NOT)\s+NULL(?!::))(.*)', re.IGNORECASE)
is_null_condition_re = re.compile('.*IS NULL.*', re.IGNORECASE)

# NVARCHAR => VARCHAR
nvarchar_re = re.compile('(.*)\ (NVARCHAR)(.*)', re.IGNORECASE)

# NVARCHAR => VARCHAR
nchar_re = re.compile('(.*)\ (NCHAR)(.*)', re.IGNORECASE)

# TEXTIMAGE_ON PRIMARY => ignore
textimageon_primary_re = re.compile('(.*)\ (TEXTIMAGE_ON PRIMARY)(.*)', re.IGNORECASE)

# USE => USE DATABASE
use_re = re.compile('(.*)(USE\s)(.*)', re.IGNORECASE)

# CREATE TABLE => CREATE OR REPLACE TABLE
createtable_re = re.compile('(.*)(CREATE\sTABLE\s)(.*)', re.IGNORECASE)

# ON PRIMARY => ignore
on_primary_re = re.compile('(.*)\ (ON PRIMARY)(.*)', re.IGNORECASE)

# DATETIME => TIMESTAMP
datetime_re = re.compile('(.*)\ (DATETIME)(.*)', re.IGNORECASE)

# BIT => BOOLEAN
bit_re = re.compile('(.*)\ (BIT)\s*(?:\([0-9]\))(.*)', re.IGNORECASE)

# Constraint Primary key => ignore
constraint_primarykey_re = re.compile('(.*)(CONSTRAINT\s+.*PRIMARY KEY)(.*)', re.IGNORECASE)

# Constraint Primary key => ignore
constraint_primarykey_end_re = re.compile(
    '(.*)(WITH\s+.*PAD_INDEX\s+.*STATISTICS_NORECOMPUTE\s.*IGNORE_DUP_KEY\s.*ON)(.*)', re.IGNORECASE)

# Constraint UNIQUE key => ignore
constraint_unique_re = re.compile('(.*)(CONSTRAINT\s+.*UNIQUE)(.*)', re.IGNORECASE)

# END all constraints => ignore
constraint_end_re = re.compile('(.*)(WITH\s+.*PAD_INDEX\s+.*STATISTICS_NORECOMPUTE\s.*IGNORE_DUP_KEY\s.*ON)(.*)',
                               re.IGNORECASE)

# ALTER TABLE...ADD CONSTRAINT => ignore
addconstraint_re = re.compile('(.*)(ALTER\s+TABLE\s+.*ADD\s+CONSTRAINT)(.*)', re.IGNORECASE)

uniqueidentifier_re = re.compile('(.*)(uniqueidentifier)(.*)', re.IGNORECASE)

go_re = re.compile('(.*)(^GO\s*$)(.*)')

nonclustered_re = re.compile('(.*)(NONCLUSTERED)(.*)', re.IGNORECASE)

max_re = re.compile('(.*)(\\(max\\))(.*)', re.IGNORECASE)

ASC_re = re.compile('(.*)(\s+ASC\s+)(.*)')

DESC_re = re.compile('(.*)(\s+DESC\s+)(.*)')

# RegExes for Redshift dialect that Snowflake doesn't support

# DISTKEY(col) => ignore
# DISTKEY => ignore
distkey_re = re.compile('(.*\s+)(DISTKEY\s*(?:\(.*?\))?)(.*)', re.IGNORECASE)

# SORTKEY(col) => ignore
# SORTKEY => ignore
sortkey_re = re.compile('(.*\s+)(SORTKEY\s*(?:\(.*?\))?)(,?.*)', re.IGNORECASE)

# SORTKEY => ignore through end of statement
sortkey_multiline_re = re.compile('(^\s*)(SORTKEY\s*\(?\s*$)(.*)', re.IGNORECASE)

# ENCODE type => ignore
encode_re = re.compile('(.*)(\sENCODE\s+.+?)((?:,|\s+|$).*)', re.IGNORECASE)

# DISTSTYLE type => ignore
diststyle_re = re.compile('(.*)(\s*DISTSTYLE\s+.+?)((?:,|\s+|$).*)', re.IGNORECASE)

# 'now'::character varying => current_timestamp
now_character_varying_re = re.compile('(.*)(\'now\'::(?:character varying|text))(.*)', re.IGNORECASE)

# bpchar => char
bpchar_re = re.compile('(.*)(bpchar)(.*)', re.IGNORECASE)

# character varying => varchar
character_varying_re = re.compile('(.*)(character varying)(.*)')

# interleaved => ignore
interleaved_re = re.compile('(.*)(interleaved)(.*)', re.IGNORECASE)

# identity(start, 0, ([0-9],[0-9])::text) => identity(start, 1)
identity_re = re.compile('(.*)\s*DEFAULT\s*"?identity"?\(([0-9]*),.*?(?:.*?::text)\)(.*)', re.IGNORECASE)

# trunc((CURRENT_TIMESTAMP)::timestamp) => date_trunc('DAY', CURRENT_TIMESTAMP)     # Redshift 'now' will have been
# resolved by now_character_varying_re to CURRENT_TIMESTAMP
trunc_re = re.compile('(.*)((?:trunc\(\()(.*)(?:\)::timestamp.*\)))(.*)', re.IGNORECASE)

# RegExes for Netezza dialect that Snowflake doesn't support
# casting syntax
# INT4(expr) => expr::INTEGER
int4_re = re.compile('(.*)\ (INT4\s*\((.*?)\))(.*)', re.IGNORECASE)

# RegExes for common/standard types that Snowflake doesn't support
# bigint_re = re.compile('(.*)\ ((?:BIGINT|TINYINT|SMALLINT)\s*\(.*\))(.*)', re.IGNORECASE)
# smallint_re = re.compile('(.*)\ (SMALLINT)(.*)', re.IGNORECASE)
floatN_re = re.compile('(.*)\ (FLOAT\d+)(.*)', re.IGNORECASE)

# CREATE [type] INDEX => ignore through end of statement
index_re = re.compile('(.*)(CREATE(?:\s+(?:UNIQUE|BITMAP))?\ INDEX)(.*)', re.IGNORECASE)

# ALTER TABLE ... ADD PRIMARY KEY => ignore
pk_re = re.compile('(.*)(ALTER\s+TABLE\s+.*ADD\s+PRIMARY\s+KEY)(.*)', re.IGNORECASE)

# SET ... TO => ignore
set_re = re.compile('(.*)(SET\s+.*TO)(.*)', re.IGNORECASE)

statement_term_re = re.compile('(.*);(.*)', re.IGNORECASE)
clause_term_re = re.compile('(.*)\)(.*)', re.IGNORECASE)

# Regexes for Aurora dialect that Snowflake doesn't support
otherint_re = re.compile('(.*)\ ((?:BIGINT|TINYINT|SMALLINT|MEDIUMINT)\s*\(.*\))(.*)', re.IGNORECASE)

key_re = re.compile('(.*)(,.*KEY.*\(.*\)\s*)(.*)', re.IGNORECASE)

unique_key_re = re.compile('(.*)(\s*UNIQUE KEY.*?(\(.*?\)))(.*)', re.IGNORECASE)

int_re = re.compile('(.*)\ (INT\s*\((?:.*?)\))(.*)', re.IGNORECASE)

charset_re = re.compile('(.*)((?:DEFAULT)?(?:CHARACTER SET|CHARSET)\s*=?\s*utf8)(.*)', re.IGNORECASE)

auto_increment_re = re.compile('(.*)(auto_increment)(.*)', re.IGNORECASE)

decimal_re = re.compile('(.*)(decimal\(([0-9]*),([0-9]*)\))(.*)', re.IGNORECASE)

float_double_re = re.compile('(.*)((float|double)\([0-9]*,[0-9]*\))(.*)', re.IGNORECASE)

text_types_re = re.compile('(.*)((?:LONG|MEDIUM)TEXT)(.*)', re.IGNORECASE)

uncommented_set_re = re.compile('(.*)(^SET)(.*)', re.IGNORECASE)

unsigned_re = re.compile('(.*)(unsigned)(.*)', re.IGNORECASE)

default_zero_re = re.compile('(.*)(default\s*\'0\')(.*)', re.IGNORECASE)

default_zero_date_re = re.compile('(.*)(default\s*\'0000-00-00\')(?:\s+|$)(.*)', re.IGNORECASE)

default_zero_ts_re = re.compile('(.*)(default\s*\'0000-00-00 00:00:00(?:\.0*)?\')(?:\s+|$)(.*)', re.IGNORECASE)

binary_default_re = re.compile('(.*)(BINARY.*?)(DEFAULT.*)', re.IGNORECASE)

# Regexes for Memsql dialect that Snowflake doesn't support
collate_re = re.compile('(.*)(COLLATE\s?=?\s?[a-zA-Z0-9_]*)(.*)', re.IGNORECASE)

inline_index_re = re.compile('(.*)(\s+INDEX\s*[a-zA-Z0-9_]*\s*(?:\(.*?\))?)(.*)', re.IGNORECASE)

enum_re = re.compile('(.*)(ENUM\(.*\))(.*)', re.IGNORECASE)

on_update_re = re.compile('(.*)(ON UPDATE CURRENT_TIMESTAMP)(.*)', re.IGNORECASE)

json_re = re.compile('(.*)(JSON)(.*)', re.IGNORECASE)
blob_re = re.compile('(.*)(BLOB)(.*)', re.IGNORECASE)
clob_re = re.compile('(.*)(CLOB)(.*)', re.IGNORECASE)
