#!/usr/bin/python3

import sys, time
from os import system
from app.regex_snow import *
import csv


# Convert source SQL to Snowflake SQL
def make_snow(sqlin, sqlout, no_comments=True):
    # processing mode
    comment_lines = None
    term_re = None

    for line in sqlin:
        # state variables
        comment = None

        sql = line.rstrip()
        sql = sql.replace('[', '').replace(']', '')
        sql = sql.replace('`', '')

        # if current line is already fully commented, don't bother with any matching
        result = comment_line_re.match(sql)
        if result:
            write_line(sqlout, sql, comment)
            continue

        # if current line is already all whitespace, don't bother with any matching
        result = whitespace_line_re.match(sql)
        if result:
            write_line(sqlout, sql, comment)
            continue

        # if we're commenting out multiple lines, check if this is the last
        if comment_lines:
            result = term_re.match(sql)
            if result:
                comment_lines = None
                term_re = None
            comment = append_comment(comment, sql, no_comments)
            sql = None
            write_line(sqlout, sql, comment)
            continue

        # ENGINE => ignore
        result = engine_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # VARCHAR2(n BYTE) => VARCHAR(n)
        result = varchar2_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)  # varchar2 clause
            cnt = result.group(3)
            discard = result.group(4)
            post = result.group(5)
            sql = '{0}{1}({2}){3}'.format(pre, clause[0:7], cnt, post)
            comment = append_comment(comment, clause, no_comments)

        # CHAR(n BYTE) => CHAR(n)
        result = char_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)  # char clause
            cnt = result.group(3)
            discard = result.group(4)
            post = result.group(5)
            sql = '{0}{1}({2}){3}'.format(pre, clause[0:4], cnt, post)
            comment = append_comment(comment, clause, no_comments)

        # DEFAULT SYSDATE => deleted (OK only because data loaded from table should already have date)
        result = default_sysdate_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # NVARCHAR => VARCHAR
        result = nvarchar_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} VARCHAR {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # NCHAR => CHAR
        result = nchar_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} CHAR {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # DATETIME => TIMESTAMP
        result = datetime_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} TIMESTAMP {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # BIGINT => INTEGER
        # result = bigint_re.match(sql)
        # if result:
        #    pre = result.group(1)
        #    clause = result.group(2)
        #    post = result.group(3)
        #    sql = '{0} INTEGER {1}\t\t-- {2}'.format(pre, post, clause)

        # SMALLINT => INTEGER
        # result = smallint_re.match(sql)
        # if result:
        #    pre = result.group(1)
        #    clause = result.group(2)
        #    post = result.group(3)
        #    sql = '{0} INTEGER {1}\t\t-- {2}'.format(pre, post, clause)

        # BIT => BOOLEAN
        result = bit_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} BOOLEAN {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # Primary Key Constraint => ignore through end of statement
        result = constraint_primarykey_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} {2} {1}'.format(pre, post, clause)

        # UNIQUE Key Constraint => ignore through end of statement
        result = constraint_unique_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}/* {2} {1}'.format(pre, post, clause)

        # End Key Constraint => ignore through end of statement
        result = constraint_primarykey_end_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0} {1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)

        # End Key Constraint => ignore through end of statement
        result = constraint_end_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} {2} {1}*/'.format(pre, post, clause)

        # ALTER TABLE...ADD CONSTRAINT => ignore through end of statement
        result = addconstraint_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0} {1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)
            comment_lines = 1
            term_re = statement_term_re

        # FLOAT8 => FLOAT
        result = floatN_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} FLOAT {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # # NULL (without NOT) => implicit nullable
        # result = null_constraint_re.match(sql)
        # if result and is_null_condition_re.match(sql):
        #     # we are in query or DML, so not looking at a constraint
        #     result = None
        # if result:
        #     pre = result.group(1)
        #     clause = result.group(2)
        #     post = result.group(3)
        #     sql = '{0}{1}\t\t-- {2}'.format(pre, post, clause)

        # TEXTIMAGEON PRIMARY => ignore
        result = textimageon_primary_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # USE => USE DATABASE
        result = use_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}USE DATABASE {1};'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # USE => USE DATABASE
        result = createtable_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}CREATE OR REPLACE TABLE {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # ON PRIMARY => ignore
        result = on_primary_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # DISTKEY(col) => ignore
        result = distkey_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # SORTKEY => ignore through end of statement
        result = sortkey_multiline_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{1} {0}'.format(post, clause)
            comment = append_comment(comment, string, no_comments)
            comment_lines = 1
            term_re = statement_term_re

        # SORTKEY(col) => ignore
        result = sortkey_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # KEY(col) => ignore
        result = key_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # UNIQUE KEY(col) => UNIQUE(col)
        result = unique_key_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            expression = result.group(3)
            post = result.group(4)
            sql = '{0} UNIQUE {2} {1}'.format(pre, post, expression)
            comment = append_comment(comment, clause, no_comments)

        # character set utf8 => ignore
        result = charset_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # auto_increment => autoincrement
        result = auto_increment_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}AUTOINCREMENT{1}'.format(pre, post);
            comment = append_comment(comment, clause, no_comments)

        # unsigned => ignore
        result = unsigned_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # default '0' => default 0
        result = default_zero_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}DEFAULT 0{1}'.format(pre, post, clause);
            comment = append_comment(comment, clause, no_comments)

        # default '0000-00-00' => default '0000-00-00'::date
        result = default_zero_date_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}::DATE{2}'.format(pre, clause, post);
            comment = append_comment(comment, clause, no_comments)

        # default '0000-00-00 00:00:00' => default '0000-00-00 00:00:00'::timestamp
        result = default_zero_ts_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}::TIMESTAMP{2}'.format(pre, clause, post);
            comment = append_comment(comment, clause, no_comments)

        # binary default => binary ignore default
        result = binary_default_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # decimal(n>38,m) => decimal(38,m)
        result = decimal_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            precision = result.group(3)
            scale = result.group(4)
            post = result.group(5)
            if int(precision) > 38:
                precision = 38
                sql = '{0}DECIMAL({3},{4}){1}'.format(pre, post, precision, scale)
                comment = append_comment(comment, clause, no_comments)

        # float|double(n,m) => float|double
        result = float_double_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            coltype = result.group(3)
            post = result.group(4)
            sql = '{0}{1}{2}'.format(pre, coltype, post)
            comment = append_comment(comment, clause, no_comments)

        # smallint|bigint|tinyint => integer
        result = otherint_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} INTEGER {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # uniqueidentifier => string
        result = uniqueidentifier_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} string {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # uniqueidentifier => string
        result = nonclustered_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # max => ignore
        result = max_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # ASC => ignore
        result = ASC_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # DESC => ignore
        result = DESC_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # uniqueidentifier => string
        result = go_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # longtext => string
        result = text_types_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} STRING {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # SET ... = ; => ignore
        result = uncommented_set_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{1}{0}'.format(post, clause)
            comment = append_comment(comment, string, no_comments)

        # ENCODE type => ignore
        result = encode_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # DISTSTYLE type => ignore
        result = diststyle_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # 'now'::(character varying|text) => current_timestamp
        result = now_character_varying_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}CURRENT_TIMESTAMP{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # bpchar => char
        result = bpchar_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}char{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # character varying => varchar
        result = character_varying_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}varchar{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # interleaved => ignore
        result = interleaved_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # redshift identity syntax => identity
        result = identity_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} IDENTITY({1},1) {2}'.format(pre, clause, post)

        # redshift date trunc syntax => date_trunc
        result = trunc_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            timespec = result.group(3)
            post = result.group(4)
            sql = '{0}DATE_TRUNC(\'DAY\', {1}) {2}'.format(pre, timespec, post)
            comment = append_comment(comment, clause, no_comments)

        result = int_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0} INTEGER {1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # SEGMENT CREATION type => ignore
        result = segment_creation_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0} {1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)
            # comment_lines = 1
            # term_re = statement_term_re

        # INDEX CREATION => ignore through end of statement
        result = index_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0} {1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)
            comment_lines = 1
            term_re = statement_term_re
            write_line(sqlout, sql, comment)
            continue

        # ALTER TABLE ... ADD PRIMARY KEY => ignore
        result = pk_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0} {1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)
            comment_lines = 1
            term_re = statement_term_re

        # SET ... TO => ignore
        result = set_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0} {1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)
            comment_lines = 1
            term_re = statement_term_re

        # NOT NULL ENABLE => NOT NULL
        result = not_null_enable_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}NOT NULL{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # COLLATE => ignore
        result = collate_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # Inline INDEX => ignore
        result = inline_index_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # enum => string
        result = enum_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}STRING{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # on update => ignore
        result = on_update_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # json => variant
        result = json_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}VARIANT{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # blob => string
        result = blob_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}STRING{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # clob => string (should we treat differently than blob?)
        result = clob_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}STRING{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # STORAGE => ignore through end of clause
        result = storage_multiline_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0}{1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)
            comment_lines = 1
            term_re = clause_term_re

        # PARTITION BY RANGE => ignore through end of clause
        result = partition_by_range_multiline_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0}{1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)
            comment_lines = 1
            term_re = clause_term_re

        # PARTITION ... VALUES => ignore through end of clause
        result = partition_values_multiline_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0}{1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)
            comment_lines = 1
            term_re = clause_term_re

        # LOB ... STORE AS => ignore through end of clause
        result = lob_multiline_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = pre
            string = '{0}{1}'.format(clause, post)
            comment = append_comment(comment, string, no_comments)
            comment_lines = 1
            term_re = clause_term_re

        # PCTFREE n => ignore
        result = pctfree_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # PCTUSED n => ignore
        result = pctused_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # INITRANS n => ignore
        result = initrans_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # MAXTRANS n => ignore
        result = maxtrans_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # TABLESPACE n => ignore
        result = tablespace_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # LOGGING => ignore
        result = logging_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # NOCOMPRESS => ignore
        result = nocompress_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # CACHE => ignore
        result = cache_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # USINGnn => ignore
        result = usingnn_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # COMPUTE STATISTICS => ignore
        result = compute_statistics_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # Empty Comma => ignore
        result = empty_comma_re.match(sql)
        if result:
            pre = result.group(1)
            clause = result.group(2)
            post = result.group(3)
            sql = '{0}{1}'.format(pre, post)
            comment = append_comment(comment, clause, no_comments)

        # DML transformations that might appear multiple times per line
        dml_repeat = True
        while dml_repeat:
            dml_repeat = False

            # determine prior period
            # e.g. trunc(sysdate,'MM')-1
            result = prior_period_re.match(sql)
            if result:
                pre = result.group(1)
                clause = result.group(2)
                col = result.group(3)
                units = result.group(4)
                offset = result.group(5)
                post = result.group(6)
                sql = '{0}dateadd({4}, {5}, trunc({3}, {4}))'.format(pre, post, clause, col, units, offset)
                comment = append_comment(comment, clause, no_comments)
                dml_repeat = True

            # add_months
            # e.g. add_months(trunc(sysdate, 'MM'), -5) => dateadd('MM', -5, trunc(current_timestamp, 'MM'))
            result = add_months_re.match(sql)
            if result:
                print(sys.stderr, "Snowflake now has add_months() function -- verify can use as-is")
                sys.exit(1)
                pre = result.group(1)
                clause = result.group(2)
                col = result.group(3)
                units = result.group(4)
                offset = result.group(5)
                post = result.group(6)
                sql = '{0}dateadd({4}, {5}, trunc({3}, {4}))'.format(pre, post, clause, col, units, offset)
                comment = append_comment(comment, clause, no_comments)
                dml_repeat = True

            # SYSDATE => CURRENT_TIMESTAMP()
            result = sysdate_re.match(sql)
            if result:
                pre = result.group(1)
                clause = result.group(2)
                post = result.group(3)
                sql = '{0} CURRENT_TIMESTAMP() {1}'.format(pre, post, clause)
                comment = append_comment(comment, clause, no_comments)
                dml_repeat = True

            # INT4(expr) => expr::INTEGER
            result = int4_re.match(sql)
            if result:
                pre = result.group(1)
                clause = result.group(2)
                col = result.group(3)
                post = result.group(4)
                sql = '{0} {3}::integer {1}'.format(pre, post, clause, col)
                comment = append_comment(comment, clause, no_comments)
                dml_repeat = True

        # write out possibly modified line
        result = whitespace_line_re.match(sql)
        if result:
            sql = None  # the mods have reduced this line to empty whitespace
        else:
            result = comma_line_re.match(sql)
            if result:
                sql = None  # the mods have reduced this line to a single vestigial comma
        write_line(sqlout, sql, comment)

        continue


def append_comment(old_comment, new_comment, no_comments):
    if no_comments:
        return None
    if old_comment and new_comment:
        return '{0} // {1}'.format(old_comment, new_comment)
    if not old_comment:
        return new_comment

    return old_comment


def write_line(sqlout, sql, comment):
    if sql is not None:
        sqlout.write(sql)
    if comment:
        sqlout.write('\t\t--// {0}'.format(comment))
    if sql is not None or comment:
        sqlout.write('\n')

    return


def converter_box_snow(sql_query: str):
    # setting incoming and outgoing queries
    qid = time.time()
    ff_in = str(qid) + "_input.sql"
    ff_out = str(qid) + "_output.sql"
    open(ff_in, "w").write(sql_query)

    # we start the conversion
    make_snow(open(ff_in, "r"), open(ff_out, "w"))

    # we get responses
    sql_query = open(ff_in, "r").read()
    snow_query = open(ff_out, "r").read()

    # we remove residual files
    system(f"rm -rf {ff_in} {ff_out}")

    return qid, sql_query, snow_query


def convert_f(value):
    for type in [int, float]:
        try:
            return type(value)
        except ValueError:
            continue
    # All other types failed it is a string
    return value


def converter_box_sql(file_path, schema_name="data_db"):
    qid = time.time()
    table_name = file_path.replace(".", "_").replace("/", "__")
    string_SQL = ''
    try:
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            headers = ','.join(next(reader))
            for row in reader:
                row = [convert_f(x) for x in row].__str__()[1:-1]
                string_SQL += f'INSERT INTO {schema_name}.{table_name}({headers}) VALUES ({row});\n'
    except:
        return ''

    return qid, string_SQL
