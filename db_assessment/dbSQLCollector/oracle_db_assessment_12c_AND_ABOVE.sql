/*
Copyright 2021 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/


/*

Version: 2.0.3
Date: 2022-01-21

*/

define version = '2.0.3'

clear col comp brea
set headsep off
set trimspool on
set lines 32000
set pagesize 50000
set feed off
set underline off
set verify off
set head on
set scan on
set term on
set pause off
set wrap on
set echo off
set appinfo 'OPTIMUS_PRIME'

whenever sqlerror continue
whenever oserror continue

column instnc new_value v_inst noprint
column hostnc new_value v_host noprint
column horanc new_value v_hora noprint
column dbname new_value v_dbname noprint
column dbversion new_value v_dbversion noprint

SELECT host_name     hostnc,
       instance_name instnc
FROM   v$instance
/

SELECT name dbname
FROM   v$database
/

SELECT TO_CHAR(SYSDATE, 'mmddrrhh24miss') horanc
FROM   dual
/

SELECT substr(replace(version,'.',''),0,3) dbversion
from v$instance
/

column min_snapid new_value v_min_snapid noprint
column max_snapid new_value v_max_snapid noprint

SELECT MIN(snap_id) min_snapid,
       MAX(snap_id) max_snapid
FROM   dba_hist_snapshot
WHERE  begin_interval_time > ( SYSDATE - 30 )
/

define v_tag = &v_dbversion._&version._&v_host..&v_dbname..&v_inst..&v_hora..log

spool opdb__awrsnapdetails__&v_tag

WITH vawrsnap as (
SELECT  '&&v_host'
        || '_'
        || '&&v_dbname'
        || '_'
        || '&&v_hora'                                                            AS pkey,
        dbid, instance_number, hour,
        min(snap_id) min_snapid, max(snap_id) max_snapid,
        min(begin_interval_time) min_begin_interval_time, max(begin_interval_time) max_begin_interval_time,
        count(1) cnt,ROUND(SUM(snaps_diff_secs),0) sum_snaps_diff_secs,
        ROUND(avg(snaps_diff_secs),0) avg_snaps_diff_secs,
        ROUND(median(snaps_diff_secs),0) median_snaps_diff_secs,
        ROUND(STATS_MODE(snaps_diff_secs),0) mode_snaps_diff_secs,
        ROUND(min(snaps_diff_secs),0) min_snaps_diff_secs,
        ROUND(max(snaps_diff_secs),0) max_snaps_diff_secs
FROM (
SELECT
       s.snap_id,
       s.dbid,
       s.instance_number,
       s.begin_interval_time,
       s.end_interval_time,
       TO_CHAR(s.begin_interval_time,'hh24') hour,
       ( TO_NUMBER(CAST((end_interval_time) AS DATE) - CAST(
                     (begin_interval_time) AS DATE)) * 60 * 60 * 24 ) snaps_diff_secs
FROM   dba_hist_snapshot s
WHERE  s.snap_id BETWEEN '&&v_min_snapid' AND '&&v_max_snapid'
)
GROUP BY '&&v_host' || '_' || '&&v_dbname' || '_' || '&&v_hora', dbid, instance_number, hour)
SELECT pkey ||','|| dbid ||','|| instance_number ||','|| hour ||','|| min_snapid ||','|| max_snapid ||','|| min_begin_interval_time ||','||
       max_begin_interval_time ||','|| cnt ||','|| sum_snaps_diff_secs ||','|| avg_snaps_diff_secs ||','|| median_snaps_diff_secs ||','||
       mode_snaps_diff_secs ||','|| min_snaps_diff_secs ||','|| max_snaps_diff_secs
FROM vawrsnap;

spool off

spool opdb__opkeylog__&v_tag

with vop as (
select '&&v_tag' pkey, '&&version' opscriptversion, '&&v_dbversion' dbversion, '&&v_host' hostname,
'&&v_dbname' dbname, '&&v_inst' instance_number, '&&v_hora' collection_time
from dual)
select pkey ||' , '|| opscriptversion ||' , '|| dbversion ||' , '|| hostname
       ||' , '|| dbname ||' , '|| instance_number ||' , '|| collection_time
from vop;

spool off

spool opdb__dbsummary__&v_tag

WITH vdbsummary AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                                                            AS pkey,
       (SELECT dbid
        FROM   v$database)                                                      AS dbid,
       (SELECT name
        FROM   v$database)                                                      AS db_name,
       (SELECT cdb
        FROM   v$database)                                                      AS cdb,
       (SELECT version
        FROM   v$instance)                                                      AS dbversion,
       (SELECT banner
        FROM   v$version
        WHERE  ROWNUM < 2)                                                      AS dbfullversion,
       (SELECT log_mode
        FROM   v$database)                                                      AS log_mode,
       (SELECT force_logging
        FROM   v$database)                                                      AS force_logging,
       (SELECT ( TRUNC(AVG(conta) * AVG(bytes) / 1024 / 1024 / 1024) )
        FROM   (SELECT TRUNC(first_time) dia,
                       COUNT(*)          conta
                FROM   v$log_history
                WHERE  first_time >= TRUNC(SYSDATE) - 30
                       AND first_time < TRUNC(SYSDATE)
                GROUP  BY TRUNC(first_time)),
               v$log)                                                           AS redo_gb_per_day,
       (SELECT COUNT(1)
        FROM   gv$instance)                                                     AS rac_dbinstaces,
       (SELECT value
        FROM   nls_database_parameters a
        WHERE  a.parameter = 'NLS_LANGUAGE')
       || '_'
       || (SELECT value
           FROM   nls_database_parameters a
           WHERE  a.parameter = 'NLS_TERRITORY')
       || '.'
       || (SELECT value
           FROM   nls_database_parameters a
           WHERE  a.parameter = 'NLS_CHARACTERSET')                             AS characterset,
       (SELECT platform_name
        FROM   v$database)                                                      AS platform_name,
       (SELECT TO_CHAR(startup_time, 'mm/dd/rr hh24:mi:ss')
        FROM   v$instance)                                                      AS startup_time,
       (SELECT COUNT(1)
        FROM   cdb_users
        WHERE  username NOT IN (SELECT name
                                FROM   SYSTEM.logstdby$skip_support
                                WHERE  action = 0))                             AS user_schemas,
       (SELECT TRUNC(SUM(bytes / 1024 / 1024))
        FROM   v$sgastat
        WHERE  name = 'buffer_cache')                                           buffer_cache_mb,
       (SELECT TRUNC(SUM(bytes / 1024 / 1024))
        FROM   v$sgastat
        WHERE  pool = 'shared pool')                                            shared_pool_mb,
       (SELECT ROUND(value / 1024 / 1024, 0)
        FROM   v$pgastat
        WHERE  name = 'total PGA allocated')                                    AS total_pga_allocated_mb,
       (SELECT ( TRUNC(SUM(bytes) / 1024 / 1024 / 1024) )
        FROM   cdb_data_files)                                                  db_size_allocated_gb,
       (SELECT ( TRUNC(SUM(bytes) / 1024 / 1024 / 1024) )
        FROM   cdb_segments
        WHERE  owner NOT IN ( 'SYS', 'SYSTEM' ))                                AS db_size_in_use_gb,
       (SELECT ( TRUNC(SUM(bytes) / 1024 / 1024 / 1024) )
        FROM   cdb_segments
        WHERE  owner NOT IN ( 'SYS', 'SYSTEM' )
               AND ( owner, segment_name ) IN (SELECT owner,
                                                      table_name
                                               FROM   cdb_tab_columns
                                               WHERE  data_type LIKE '%LONG%')) AS db_long_size_gb,
       (SELECT database_role
        FROM   v$database)                                                      AS dg_database_role,
       (SELECT protection_mode
        FROM   v$database)                                                      AS dg_protection_mode,
       (SELECT protection_level
        FROM   v$database)                                                      AS dg_protection_level
FROM   dual)
SELECT pkey ||' , '|| dbid ||' , '|| db_name ||' , '|| cdb ||' , '|| dbversion ||' , '|| dbfullversion ||' , '|| log_mode ||' , '|| force_logging ||' , '||
       redo_gb_per_day ||' , '|| rac_dbinstaces ||' , '|| characterset ||' , '|| platform_name ||' , '|| startup_time ||' , '|| user_schemas ||' , '||
	   buffer_cache_mb ||' , '|| shared_pool_mb ||' , '|| total_pga_allocated_mb ||' , '|| db_size_allocated_gb ||' , '|| db_size_in_use_gb ||' , '||
	   db_long_size_gb ||' , '|| dg_database_role ||' , '|| dg_protection_mode ||' , '|| dg_protection_level
FROM vdbsummary;

spool off

spool opdb__pdbsinfo__&v_tag

WITH vpdbinfo AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       dbid,
       pdb_id,
       pdb_name,
       status,
       logging
FROM   cdb_pdbs )
SELECT pkey ||' , '|| dbid ||' , '|| pdb_id ||' , '|| pdb_name ||' , '|| status ||' , '|| logging
FROM  vpdbinfo;

spool off

spool opdb__pdbsopenmode__&v_tag

WITH vpdbmode as (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                   AS pkey,
       con_id,
       name,
       open_mode,
       total_size / 1024 / 1024 / 1024 TOTAL_GB
FROM   v$pdbs )
SELECT pkey ||' , '|| con_id ||' , '|| name ||' , '|| open_mode ||' , '|| TOTAL_GB
FROM vpdbmode;

spool off

spool opdb__dbinstances__&v_tag

WITH vdbinst as (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       inst_id,
       instance_name,
       host_name,
       version,
       status,
       database_status,
       instance_role
FROM   gv$instance )
SELECT pkey ||' , '|| inst_id ||' , '|| instance_name ||' , '|| host_name ||' , '||
       version ||' , '|| status ||' , '|| database_status ||' , '|| instance_role
FROM vdbinst;

spool off


spool opdb__usedspacedetails__&v_tag

WITH vused AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       con_id,
       owner,
       segment_type,
       tablespace_name,
       flash_cache,
       inmemory,
       GROUPING(con_id)                          IN_CON_ID,
       GROUPING(owner)                           IN_OWNER,
       GROUPING(segment_type)                    IN_SEGMENT_TYPE,
       GROUPING(tablespace_name)                 IN_TABLESPACE_NAME,
       GROUPING(flash_cache)                     IN_FLASH_CACHE,
       GROUPING(inmemory)                        IN_INMEMORY,
       ROUND(SUM(bytes) / 1024 / 1024 / 1024, 0) GB
       FROM   cdb_segments
       WHERE  owner NOT IN (
                          SELECT name
                          FROM   SYSTEM.logstdby$skip_support
                          WHERE  action=0)
       GROUP  BY grouping sets( ( ), ( con_id ), ( owner ), ( segment_type ),
                    ( tablespace_name ), ( flash_cache ), ( inmemory ), ( con_id, owner ), ( con_id, owner, flash_cache, inmemory ) ))
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| segment_type ||' , '|| tablespace_name ||' , '||
       flash_cache ||' , '|| inmemory ||' , '|| IN_CON_ID ||' , '|| IN_OWNER ||' , '|| IN_SEGMENT_TYPE ||' , '||
	   IN_TABLESPACE_NAME ||' , '|| IN_FLASH_CACHE ||' , '|| GB
FROM vused;

spool off

spool opdb__compressbytable__&v_tag

WITH vtbcompress AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       con_id,
       owner,
       SUM(table_count)                  tab,
       TRUNC(SUM(table_gbytes))          table_gb,
       SUM(partition_count)              part,
       TRUNC(SUM(partition_gbytes))      part_gb,
       SUM(subpartition_count)           subpart,
       TRUNC(SUM(subpartition_gbytes))   subpart_gb,
       TRUNC(SUM(table_gbytes) + SUM(partition_gbytes)
             + SUM(subpartition_gbytes)) total_gbytes
       FROM   (SELECT t.con_id,
                       t.owner,
                       COUNT(*)                        table_count,
                       SUM(bytes / 1024 / 1024 / 1024) table_gbytes,
                       0                               partition_count,
                       0                               partition_gbytes,
                       0                               subpartition_count,
                       0                               subpartition_gbytes
                FROM   cdb_tables t,
                       cdb_segments s
                WHERE  t.con_id = s.con_id
                       AND t.owner = s.owner
                       AND t.table_name = s.segment_name
                       AND s.partition_name IS NULL
                       AND compression = 'ENABLED'
                       AND t.owner NOT IN
                                          (
                                          SELECT name
                                          FROM   SYSTEM.logstdby$skip_support
                                          WHERE  action=0)
                GROUP  BY t.con_id,
                          t.owner
                UNION ALL
                SELECT t.con_id,
                       t.table_owner owner,
                       0,
                       0,
                       COUNT(*),
                       SUM(bytes / 1024 / 1024 / 1024),
                       0,
                       0
                FROM   cdb_tab_partitions t,
                       cdb_segments s
                WHERE  t.con_id = s.con_id
                       AND t.table_owner = s.owner
                       AND t.table_name = s.segment_name
                       AND t.partition_name = s.partition_name
                       AND compression = 'ENABLED'
                       AND t.table_owner NOT IN (
                                                 SELECT name
                                                 FROM   SYSTEM.logstdby$skip_support
                                                 WHERE  action=0)
                GROUP  BY t.con_id,
                          t.table_owner
                UNION ALL
                SELECT t.con_id,
                       t.table_owner owner,
                       0,
                       0,
                       0,
                       0,
                       COUNT(*),
                       SUM(bytes / 1024 / 1024 / 1024)
                FROM   cdb_tab_subpartitions t,
                       cdb_segments s
                WHERE  t.con_id = s.con_id
                       AND t.table_owner = s.owner
                       AND t.table_name = s.segment_name
                       AND t.subpartition_name = s.partition_name
                       AND compression = 'ENABLED'
                       AND t.table_owner NOT IN
                                                 (
                                                 SELECT name
                                                 FROM   SYSTEM.logstdby$skip_support
                                                 WHERE  action=0)
                GROUP  BY t.con_id,
                          t.table_owner)
        GROUP  BY con_id,
                  owner
        HAVING TRUNC(SUM(table_gbytes) + SUM(partition_gbytes)
                     + SUM(subpartition_gbytes)) > 0 )
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| tab ||' , '|| table_gb ||' , '|| part ||' , '||
       part_gb ||' , '|| subpart ||' , '|| subpart_gb ||' , '|| total_gbytes
FROM vtbcompress
ORDER  BY total_gbytes DESC;

spool off

spool opdb__compressbytype__&v_tag

WITH vcompresstype AS (
     SELECT '&&v_host'
            || '_'
            || '&&v_dbname'
            || '_'
            || '&&v_hora' AS pkey,
	        con_id,
            owner,
            TRUNC(SUM(DECODE(compress_for, 'BASIC', gbytes,
                                           0))) basic,
            TRUNC(SUM(DECODE(compress_for, 'OLTP', gbytes,
                                           'ADVANCED', gbytes,
                                           0))) oltp,
            TRUNC(SUM(DECODE(compress_for, 'QUERY LOW', gbytes,
                                           0))) query_low,
            TRUNC(SUM(DECODE(compress_for, 'QUERY HIGH', gbytes,
                                           0))) query_high,
            TRUNC(SUM(DECODE(compress_for, 'ARCHIVE LOW', gbytes,
                                           0))) archive_low,
            TRUNC(SUM(DECODE(compress_for, 'ARCHIVE HIGH', gbytes,
                                           0))) archive_high,
            TRUNC(SUM(gbytes))                  total_gb
     FROM   (SELECT t.con_id,
                    t.owner,
                    t.compress_for,
                    SUM(bytes / 1024 / 1024 / 1024) gbytes
             FROM   cdb_tables t,
                    cdb_segments s
             WHERE  t.con_id = s.con_id
                    AND t.owner = s.owner
                    AND t.table_name = s.segment_name
                    AND s.partition_name IS NULL
                    AND compression = 'ENABLED'
                    AND t.owner NOT IN
                                       (
                                       SELECT name
                                       FROM   SYSTEM.logstdby$skip_support
                                       WHERE  action=0)
             GROUP  BY t.con_id,
                       t.owner,
                       t.compress_for
             UNION ALL
             SELECT t.con_id,
                    t.table_owner,
                    t.compress_for,
                    SUM(bytes / 1024 / 1024 / 1024) gbytes
             FROM   cdb_tab_partitions t,
                    cdb_segments s
             WHERE  t.con_id = s.con_id
                    AND t.table_owner = s.owner
                    AND t.table_name = s.segment_name
                    AND t.partition_name = s.partition_name
                    AND compression = 'ENABLED'
                    AND t.table_owner NOT IN
                                              (
                                              SELECT name
                                              FROM   SYSTEM.logstdby$skip_support
                                              WHERE  action=0)
             GROUP  BY t.con_id,
                       t.table_owner,
                       t.compress_for
             UNION ALL
             SELECT t.con_id,
                    t.table_owner,
                    t.compress_for,
                    SUM(bytes / 1024 / 1024 / 1024) gbytes
             FROM   cdb_tab_subpartitions t,
                    cdb_segments s
             WHERE  t.con_id = s.con_id
                    AND t.table_owner = s.owner
                    AND t.table_name = s.segment_name
                    AND t.subpartition_name = s.partition_name
                    AND compression = 'ENABLED'
                    AND t.table_owner NOT IN
                                              (
                                              SELECT name
                                              FROM   SYSTEM.logstdby$skip_support
                                              WHERE  action=0)
             GROUP  BY t.con_id,
                       t.table_owner,
                       t.compress_for)
     GROUP  BY con_id,
               owner
     HAVING TRUNC(SUM(gbytes)) > 0)
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| basic ||' , '|| oltp ||' , '|| query_low ||' , '||
       query_high ||' , '|| archive_low ||' , '|| archive_high ||' , '|| total_gb
FROM vcompresstype
ORDER BY total_gb DESC;

spool off

spool opdb__spacebyownersegtype__&v_tag

WITH vspaceow AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       a.con_id,
       a.owner,
       DECODE(a.segment_type, 'TABLE', 'TABLE',
                              'TABLE PARTITION', 'TABLE',
                              'TABLE SUBPARTITION', 'TABLE',
                              'INDEX', 'INDEX',
                              'INDEX PARTITION', 'INDEX',
                              'INDEX SUBPARTITION', 'INDEX',
                              'LOB', 'LOB',
                              'LOB PARTITION', 'LOB',
                              'LOBSEGMENT', 'LOB',
                              'LOBINDEX', 'LOB',
                              'OTHERS')         segment_type,
       TRUNC(SUM(a.bytes) / 1024 / 1024 / 1024) total_gb
       FROM   cdb_segments a
       WHERE  a.owner NOT IN
                            (
                            SELECT name
                            FROM   SYSTEM.logstdby$skip_support
                            WHERE  action=0)
       GROUP  BY a.con_id,
                 a.owner,
                 DECODE(a.segment_type, 'TABLE', 'TABLE',
                                         'TABLE PARTITION', 'TABLE',
                                         'TABLE SUBPARTITION', 'TABLE',
                                         'INDEX', 'INDEX',
                                         'INDEX PARTITION', 'INDEX',
                                         'INDEX SUBPARTITION', 'INDEX',
                                         'LOB', 'LOB',
                                         'LOB PARTITION', 'LOB',
                                         'LOBSEGMENT', 'LOB',
                                         'LOBINDEX', 'LOB',
                                         'OTHERS')
       HAVING TRUNC(SUM(a.bytes) / 1024 / 1024 / 1024) >= 1)
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| segment_type ||' , '|| total_gb
FROM vspaceow
ORDER  BY total_gb DESC;


spool off

spool opdb__spacebytablespace__&v_tag

WITH vspctbs AS (
        SELECT '&&v_host'
               || '_'
               || '&&v_dbname'
               || '_'
               || '&&v_hora' AS pkey,
		       b.tablespace_name,
               b.extent_management,
               b.allocation_type,
               b.segment_space_management,
               b.status,
               SUM(estd_ganho_mb) estd_ganho_mb
        FROM   (SELECT b.tablespace_name,
                       b.extent_management,
                       b.allocation_type,
                       b.segment_space_management,
                       b.status,
                       a.initial_extent / 1024                                                                                        inital_kb,
                       a.owner,
                       a.segment_name,
                       a.partition_name,
                       ( a.bytes ) / 1024                                                                                             segsize_kb,
                       TRUNC(( a.initial_extent / 1024 ) / ( ( a.bytes ) / 1024 ) * 100)                                              perc,
                       TRUNC(( ( a.bytes ) / 1024 / 100 ) * TRUNC(( a.initial_extent / 1024 ) / ( ( a.bytes ) / 1024 ) * 100) / 1024) estd_ganho_mb
                FROM   cdb_segments a
                       inner join cdb_tablespaces b
                               ON a.tablespace_name = b.tablespace_name
                WHERE  a.owner NOT IN
                                   (
                                   SELECT name
                                   FROM   SYSTEM.logstdby$skip_support
                                   WHERE  action=0)
                       AND b.allocation_type = 'SYSTEM'
                       AND a.initial_extent = a.bytes) b
        GROUP  BY b.tablespace_name,
                  b.extent_management,
                  b.allocation_type,
                  b.segment_space_management,
                  b.status
        UNION ALL
        SELECT '&&v_host'
               || '_'
               || '&&v_dbname'
               || '_'
               || '&&v_hora' AS pkey,
		       b.tablespace_name,
               b.extent_management,
               b.allocation_type,
               b.segment_space_management,
               b.status,
               SUM(estd_ganho_mb) estd_ganho_mb
        FROM   (SELECT b.tablespace_name,
                       b.extent_management,
                       b.allocation_type,
                       b.segment_space_management,
                       b.status,
                       a.initial_extent / 1024                                                                                        inital_kb,
                       a.owner,
                       a.segment_name,
                       a.partition_name,
                       ( a.bytes ) / 1024                                                                                             segsize_kb,
                       TRUNC(( a.initial_extent / 1024 ) / ( ( a.bytes ) / 1024 ) * 100)                                              perc,
                       TRUNC(( ( a.bytes ) / 1024 / 100 ) * TRUNC(( a.initial_extent / 1024 ) / ( ( a.bytes ) / 1024 ) * 100) / 1024) estd_ganho_mb
                FROM   cdb_segments a
                       inner join cdb_tablespaces b
                               ON a.tablespace_name = b.tablespace_name
                WHERE  a.owner NOT IN
                                   (
                                   SELECT name
                                   FROM   SYSTEM.logstdby$skip_support
                                   WHERE  action=0)
                       AND b.allocation_type != 'SYSTEM') b
        GROUP  BY b.tablespace_name,
                  b.extent_management,
                  b.allocation_type,
                  b.segment_space_management,
                  b.status)
SELECT pkey ||' , '|| tablespace_name ||' , '|| extent_management ||' , '|| allocation_type ||' , '||
       segment_space_management ||' , '|| status ||' , '|| estd_ganho_mb
FROM vspctbs;


spool off

spool opdb__freespaces__&v_tag

WITH vfreespace AS (
        SELECT '&&v_host'
               || '_'
               || '&&v_dbname'
               || '_'
               || '&&v_hora' AS pkey,
		       total.con_id,
               total.ts                                                                          tablespace_name,
               DECODE(total.mb, NULL, 'OFFLINE',
                                dbat.status)                                                     status,
               TRUNC(total.mb / 1024)                                                            total_gb,
               TRUNC(NVL(total.mb - free.mb, total.mb) / 1024)                                   used_gb,
               TRUNC(NVL(free.mb, 0) / 1024)                                                     free_gb,
               DECODE(total.mb, NULL, 0,
                                NVL(ROUND(( total.mb - free.mb ) / ( total.mb ) * 100, 2), 100)) pct_used,
               CASE
                 WHEN ( total.mb IS NULL ) THEN '['
                                                || RPAD(LPAD('OFFLINE', 13, '-'), 20, '-')
                                                ||']'
                 ELSE '['
                      || DECODE(free.mb, NULL, 'XXXXXXXXXXXXXXXXXXXX',
                                         NVL(RPAD(LPAD('X', TRUNC(( 100 - ROUND(( free.mb ) / ( total.mb ) * 100, 2) ) / 5), 'X'), 20, '-'), '--------------------'))
                      ||']'
               END                                                                               AS GRAPH
        FROM   (SELECT con_id,
                       tablespace_name          ts,
                       SUM(bytes) / 1024 / 1024 mb
                FROM   cdb_data_files
                GROUP  BY con_id,
                          tablespace_name) total,
               (SELECT con_id,
                       tablespace_name          ts,
                       SUM(bytes) / 1024 / 1024 mb
                FROM   cdb_free_space
                GROUP  BY con_id,
                          tablespace_name) free,
               cdb_tablespaces dbat
        WHERE  total.ts = free.ts (+)
               AND total.ts = dbat.tablespace_name
               AND total.con_id = free.con_id (+)
               AND total.con_id = dbat.con_id
        UNION ALL
        SELECT '&&v_host'
               || '_'
               || '&&v_dbname'
               || '_'
               || '&&v_hora' AS pkey,
		       sh.con_id,
               sh.tablespace_name,
               'TEMP',
               SUM(sh.bytes_used + sh.bytes_free) / 1024 / 1024                        total_mb,
               SUM(sh.bytes_used) / 1024 / 1024                                        used_mb,
               SUM(sh.bytes_free) / 1024 / 1024                                        free_mb,
               ROUND(SUM(sh.bytes_used) / SUM(sh.bytes_used + sh.bytes_free) * 100, 2) pct_used,
               '['
               ||DECODE(SUM(sh.bytes_free), 0, 'XXXXXXXXXXXXXXXXXXXX',
                                            NVL(RPAD(LPAD('X', ( TRUNC(ROUND(( SUM(sh.bytes_used) / SUM(sh.bytes_used + sh.bytes_free) ) * 100, 2) / 5) ), 'X'), 20, '-'),
                                            '--------------------'))
               ||']'
        FROM   v$temp_space_header sh
        GROUP  BY con_id,
                  tablespace_name)
SELECT pkey ||' , '|| con_id ||' , '|| tablespace_name ||' , '|| status ||' , '|| total_gb ||' , '||
       used_gb ||' , '|| free_gb ||' , '|| pct_used ||' , '|| GRAPH
FROM vfreespace
ORDER  BY graph;

spool off

spool opdb__dblinks__&v_tag

WITH vdbl AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       con_id,
       owner,
       db_link,
       host,
       created
FROM   cdb_db_links
WHERE  owner NOT IN
                     (
                     SELECT name
                     FROM   SYSTEM.logstdby$skip_support
                     WHERE  action=0))
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| db_link ||' , '|| host ||' , '|| created
FROM vdbl;

spool off

spool opdb__dbparameters__&v_tag

WITH vparam AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                                   AS pkey,
       inst_id,
       con_id,
       REPLACE(name, ',', '/')                         name,
       REPLACE(SUBSTR(value, 1, 60), ',', '/')         value,
       REPLACE(SUBSTR(default_value, 1, 30), ',', '/') default_value,
       isdefault
FROM   gv$parameter
ORDER  BY 2,3 )
SELECT pkey ||' , '|| inst_id ||' , '|| con_id ||' , '|| name ||' , '|| value ||' , '|| default_value ||' , '|| isdefault
FROM vparam;

spool off

spool opdb__dbfeatures__&v_tag

WITH vdbf AS(
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                                 AS pkey,
       con_id,
       REPLACE(name, ',', '/')                       name,
       currently_used,
       detected_usages,
       total_samples,
       TO_CHAR(first_usage_date, 'MM/DD/YY HH24:MI') first_usage,
       TO_CHAR(last_usage_date, 'MM/DD/YY HH24:MI')  last_usage,
       aux_count
FROM   cdb_feature_usage_statistics
ORDER  BY name)
SELECT pkey ||' , '|| con_id ||' , '|| name ||' , '|| currently_used ||' , '|| detected_usages ||' , '||
       total_samples ||' , '|| first_usage ||' , '|| last_usage ||' , '|| aux_count
FROM vdbf;

spool off

spool opdb__dbhwmarkstatistics__&v_tag

WITH vhwmst AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       description,
       highwater,
       last_value
FROM   dba_high_water_mark_statistics
ORDER  BY description)
SELECT pkey ||' , '|| description ||' , '|| highwater ||' , '|| last_value
FROM vhwmst;

spool off

spool opdb__cpucoresusage__&v_tag

WITH vcpursc AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                          AS pkey,
       TO_CHAR(timestamp, 'MM/DD/YY HH24:MI') dt,
       cpu_count,
       cpu_core_count,
       cpu_socket_count
FROM   dba_cpu_usage_statistics
ORDER  BY timestamp)
SELECT pkey ||' , '|| dt ||' , '|| cpu_count ||' , '|| cpu_core_count ||' , '|| cpu_socket_count
FROM vcpursc;

spool off

spool opdb__dbobjects__&v_tag

WITH vdbobj AS (
        SELECT '&&v_host'
               || '_'
               || '&&v_dbname'
               || '_'
               || '&&v_hora' AS pkey,
               con_id,
               owner,
               object_type,
               editionable,
               COUNT(1)              count,
               GROUPING(con_id)      in_con_id,
               GROUPING(owner)       in_owner,
               GROUPING(object_type) in_OBJECT_TYPE,
               GROUPING(editionable) in_EDITIONABLE
        FROM   cdb_objects
        WHERE  owner NOT IN
                            (
                            SELECT name
                            FROM   SYSTEM.logstdby$skip_support
                            WHERE  action=0)
        GROUP  BY grouping sets ( ( con_id, object_type ), (
                                  con_id, owner, editionable,
                                             object_type ) ))
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| object_type ||' , '|| editionable ||' , '||
       count ||' , '|| in_con_id ||' , '|| in_owner ||' , '|| in_object_type ||' , '|| in_editionable
FROM vdbobj;

spool off

spool opdb__sourcecode__&v_tag

WITH vsrc AS (
SELECT pkey,
       con_id,
       owner,
       TYPE,
       SUM(nr_lines)       sum_nr_lines,
       COUNT(1)            qt_objs,
       SUM(count_utl)      sum_nr_lines_w_utl,
       SUM(count_dbms)     sum_nr_lines_w_dbms,
       SUM(count_exec_im)  count_exec_im,
       SUM(count_dbms_sql) count_dbms_sql,
       SUM(count_dbms_utl) sum_nr_lines_w_dbms_utl,
       SUM(count_total)    sum_count_total
FROM   (SELECT '&&v_host'
               || '_'
               || '&&v_dbname'
               || '_'
               || '&&v_hora' AS pkey,
               con_id,
               owner,
               name,
               TYPE,
               MAX(line)     NR_LINES,
               COUNT(CASE
                       WHEN LOWER(text) LIKE '%utl_%' THEN 1
                     END)    count_utl,
               COUNT(CASE
                       WHEN LOWER(text) LIKE '%dbms_%' THEN 1
                     END)    count_dbms,
               COUNT(CASE
                       WHEN LOWER(text) LIKE '%dbms_%'
                            AND LOWER(text) LIKE '%utl_%' THEN 1
                     END)    count_dbms_utl,
               COUNT(CASE
                       WHEN LOWER(text) LIKE '%execute%immediate%' THEN 1
                     END)    count_exec_im,
               COUNT(CASE
                       WHEN LOWER(text) LIKE '%dbms_sql%' THEN 1
                     END)    count_dbms_sql,
               COUNT(1)      count_total
        FROM   cdb_source
        WHERE  owner NOT IN
                            (
                            SELECT name
                            FROM   SYSTEM.logstdby$skip_support
                            WHERE  action=0)
        GROUP  BY '&&v_host'
                  || '_'
                  || '&&v_dbname'
                  || '_'
                  || '&&v_hora',
                  con_id,
                  owner,
                  name,
                  TYPE)
GROUP  BY pkey,
          con_id,
          owner,
          TYPE)
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| type ||' , '|| sum_nr_lines ||' , '|| qt_objs ||' , '||
       sum_nr_lines_w_utl ||' , '|| sum_nr_lines_w_dbms ||' , '|| count_exec_im ||' , '|| count_dbms_sql ||' , '|| sum_nr_lines_w_dbms_utl ||' , '|| sum_count_total
FROM vsrc;

spool off

spool opdb__partsubparttypes__&v_tag

WITH vpart AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       con_id,
       owner,
       partitioning_type,
       subpartitioning_type,
       COUNT(1) as cnt
FROM   cdb_part_tables
WHERE  owner NOT IN
                     (
                     SELECT name
                     FROM   SYSTEM.logstdby$skip_support
                     WHERE  action=0)
GROUP  BY '&&v_host'
          || '_'
          || '&&v_dbname'
          || '_'
          || '&&v_hora',
          con_id,
          owner,
          partitioning_type,
          subpartitioning_type)
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| partitioning_type ||' , '|| subpartitioning_type ||' , '|| cnt
FROM vpart;

spool off

spool opdb__indexestypes__&v_tag

WITH vidxtype AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       con_id,
       owner,
       index_type,
       COUNT(1) as cnt
FROM   cdb_indexes
WHERE  owner NOT IN
                     (
                     SELECT name
                     FROM   SYSTEM.logstdby$skip_support
                     WHERE  action=0)
GROUP  BY '&&v_host'
          || '_'
          || '&&v_dbname'
          || '_'
          || '&&v_hora',
          con_id,
          owner,
          index_type)
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| index_type ||' , '|| cnt
FROM vidxtype;

spool off

spool opdb__datatypes__&v_tag

WITH vdtype AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       con_id,
       owner,
       data_type,
       COUNT(1) as cnt
FROM   cdb_tab_columns
WHERE  owner NOT IN
                     (
                     SELECT name
                     FROM   SYSTEM.logstdby$skip_support
                     WHERE  action=0)
GROUP  BY '&&v_host'
          || '_'
          || '&&v_dbname'
          || '_'
          || '&&v_hora',
          con_id,
          owner,
          data_type)
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| data_type ||' , '|| cnt
FROM vdtype;

spool off

spool opdb__tablesnopk__&v_tag

WITH vnopk AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'              AS pkey,
       con_id,
       owner,
       SUM(pk)                    pk,
       SUM(uk)                    uk,
       SUM(ck)                    ck,
       SUM(ri)                    ri,
       SUM(vwck)                  vwck,
       SUM(vwro)                  vwro,
       SUM(hashexpr)              hashexpr,
       SUM(suplog)                suplog,
       COUNT(DISTINCT table_name) num_tables,
       COUNT(1)                   total_cons
FROM   (SELECT a.con_id,
               a.owner,
               a.table_name,
               DECODE(b.constraint_type, 'P', 1,
                                         NULL) pk,
               DECODE(b.constraint_type, 'U', 1,
                                         NULL) uk,
               DECODE(b.constraint_type, 'C', 1,
                                         NULL) ck,
               DECODE(b.constraint_type, 'R', 1,
                                         NULL) ri,
               DECODE(b.constraint_type, 'V', 1,
                                         NULL) vwck,
               DECODE(b.constraint_type, 'O', 1,
                                         NULL) vwro,
               DECODE(b.constraint_type, 'H', 1,
                                         NULL) hashexpr,
               DECODE(b.constraint_type, 'F', 1,
                                         NULL) refcolcons,
               DECODE(b.constraint_type, 'S', 1,
                                         NULL) suplog
        FROM   cdb_tables a
               left outer join cdb_constraints b
                            ON a.con_id = b.con_id
                               AND a.owner = b.owner
                               AND a.table_name = b.table_name)
GROUP  BY '&&v_host'
          || '_'
          || '&&v_dbname'
          || '_'
          || '&&v_hora',
          con_id,
          owner)
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| pk ||' , '|| uk ||' , '|| ck ||' , '||
       ri ||' , '|| vwck ||' , '|| vwro ||' , '|| hashexpr ||' , '|| suplog ||' , '|| num_tables ||' , '|| total_cons
FROM vnopk;

spool off

spool opdb__systemstats__&v_tag

WITH vpsystat AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora' AS pkey,
       sname,
       pname,
       pval1,
       pval2
FROM   sys.aux_stats$)
SELECT pkey ||' , '|| sname ||' , '|| pname ||' , '|| pval1 ||' , '|| pval2
FROM vpsystat;

spool off

spool opdb__patchlevel__&v_tag

WITH vpatch AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                            AS pkey,
       TO_CHAR(action_time, 'DD-MON-YYYY HH24:MI:SS') AS action_time,
       action,
       status,
       description,
       patch_id
FROM   sys.dba_registry_sqlpatch
ORDER by action_time)
SELECT pkey ||' , '|| '12c+' ||' , '|| action_time ||' , '|| action ||' , '|| status ||' , '|| description ||' , '|| patch_id ||' , '|| 'N/A'
FROM vpatch;

spool off

spool opdb__alertlog__&v_tag

WITH valert AS (
         SELECT '&&v_host'
                || '_'
                || '&&v_dbname'
                || '_'
                || '&&v_hora'                            AS pkey,
		        TO_CHAR(A.originating_timestamp, 'dd/mm/yyyy hh24:mi:ss')               MESSAGE_TIME,
                REPLACE(REPLACE(SUBSTR(a.message_text, 0, 180), ',', ';'), '\n', '   ') message_text,
                SUBSTR(a.host_id, 0, 30)                                                host_id,
                a.con_id,
                SUBSTR(a.component_id, 0, 30)                                           component_id,
                a.message_type,
                a.message_level,
                SUBSTR(a.message_id, 0, 30)                                             message_id,
                a.message_group
         FROM   v$diag_alert_ext A
         ORDER  BY A.originating_timestamp DESC)
SELECT pkey ||' , '|| MESSAGE_TIME ||' , '|| message_text ||' , '|| host_id ||' , '|| con_id ||' , '||
       component_id ||' , '|| message_type ||' , '|| message_level ||' , '|| message_id ||' , '|| message_group
FROM valert
WHERE ROWNUM < 5001;

spool off

spool opdb__awrhistsysmetrichist__&v_tag

WITH vsysmetric AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                            AS pkey,
       hsm.dbid,
       hsm.instance_number,
       TO_CHAR(hsm.begin_time, 'hh24')          hour,
       hsm.metric_name,
       hsm.metric_unit,
       AVG(hsm.value)                           avg_value,
       STATS_MODE(hsm.value)                    mode_value,
       MEDIAN(hsm.value)                        median_value,
       MIN(hsm.value)                           min_value,
       MAX(hsm.value)                           max_value,
       SUM(hsm.value)                           sum_value,
       PERCENTILE_CONT(0.5)
         within GROUP (ORDER BY hsm.value DESC) AS "PERC50",
       PERCENTILE_CONT(0.25)
         within GROUP (ORDER BY hsm.value DESC) AS "PERC75",
       PERCENTILE_CONT(0.10)
         within GROUP (ORDER BY hsm.value DESC) AS "PERC90",
       PERCENTILE_CONT(0.05)
         within GROUP (ORDER BY hsm.value DESC) AS "PERC95",
       PERCENTILE_CONT(0)
         within GROUP (ORDER BY hsm.value DESC) AS "PERC100"
FROM   dba_hist_sysmetric_history hsm
       inner join dba_hist_snapshot dhsnap
               ON hsm.snap_id = dhsnap.snap_id
                  AND hsm.instance_number = dhsnap.instance_number
                  AND hsm.dbid = dhsnap.dbid
WHERE  hsm.snap_id BETWEEN '&&v_min_snapid' AND '&&v_max_snapid'
GROUP  BY '&&v_host'
          || '_'
          || '&&v_dbname'
          || '_'
          || '&&v_hora',
          hsm.dbid,
          hsm.instance_number,
          TO_CHAR(hsm.begin_time, 'hh24'),
          hsm.metric_name,
          hsm.metric_unit--, dhsnap.STARTUP_TIME
ORDER  BY hsm.dbid,
          hsm.instance_number,
          hsm.metric_name,
          TO_CHAR(hsm.begin_time, 'hh24'))
SELECT pkey ||' , '|| dbid ||' , '|| instance_number ||' , '|| hour ||' , '|| metric_name ||' , '||
       metric_unit ||' , '|| avg_value ||' , '|| mode_value ||' , '|| median_value ||' , '|| min_value ||' , '|| max_value ||' , '||
	   sum_value ||' , '|| PERC50 ||' , '|| PERC75 ||' , '|| PERC90 ||' , '|| PERC95 ||' , '|| PERC100
FROM vsysmetric;

spool off

spool opdb__awrhistsysmetricsumm__&v_tag

WITH vsysmetricsumm AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                            AS pkey,
       hsm.dbid,
       hsm.instance_number,
       TO_CHAR(hsm.begin_time, 'hh24')          hour,
       hsm.metric_name,
       hsm.metric_unit,
       hsm.AVERAGE                           avg_value,
       null                                  mode_value,
       null                                  median_value,
       MINVAL                                min_value,
       MAXVAL                                max_value,
       null                                  sum_value,
       null                                  "PERC50",
       null                                  "PERC75",
       null                                  "PERC90",
       hsm.AVERAGE+(2*hsm.STANDARD_DEVIATION) "PERC95",
       MAXVAL                                 "PERC100"
FROM   DBA_HIST_SYSMETRIC_SUMMARY hsm
WHERE  hsm.snap_id BETWEEN '&&v_min_snapid' AND '&&v_max_snapid'),
vsysmetricsummperhour as (
    SELECT pkey,
       hsm.dbid,
       hsm.instance_number,
       TO_CHAR(hsm.begin_time, 'hh24')          hour,
       hsm.metric_name,
       hsm.metric_unit,
       AVG(hsm.PERC95)                           avg_value,
       STATS_MODE(hsm.PERC95)                    mode_value,
       MEDIAN(hsm.PERC95)                        median_value,
       MIN(hsm.PERC95)                           min_value,
       MAX(hsm.PERC95)                           max_value,
       SUM(hsm.PERC95)                           sum_value,
       PERCENTILE_CONT(0.5)
         within GROUP (ORDER BY hsm.PERC95 DESC) AS "PERC50",
       PERCENTILE_CONT(0.25)
         within GROUP (ORDER BY hsm.PERC95 DESC) AS "PERC75",
       PERCENTILE_CONT(0.10)
         within GROUP (ORDER BY hsm.PERC95 DESC) AS "PERC90",
       PERCENTILE_CONT(0.05)
         within GROUP (ORDER BY hsm.PERC95 DESC) AS "PERC95",
       PERCENTILE_CONT(0)
         within GROUP (ORDER BY hsm.PERC95 DESC) AS "PERC100"
    FROM vsysmetricsumm hsm
    GROUP  BY pkey,
            hsm.dbid,
            hsm.instance_number,
            TO_CHAR(hsm.begin_time, 'hh24'),
            hsm.metric_name,
            hsm.metric_unit--, dhsnap.STARTUP_TIME
)
SELECT pkey ||' , '|| dbid ||' , '|| instance_number ||' , '|| hour ||' , '|| metric_name ||' , '||
       metric_unit ||' , '|| avg_value ||' , '|| mode_value ||' , '|| median_value ||' , '|| min_value ||' , '|| max_value ||' , '||
	   sum_value ||' , '|| PERC50 ||' , '|| PERC75 ||' , '|| PERC90 ||' , '|| PERC95 ||' , '|| PERC100
FROM vsysmetricsummperhour; 

spool off

spool opdb__awrhistosstat__&v_tag

WITH v_osstat_all
     AS (SELECT os.dbid,
                     os.instance_number,
                     TO_CHAR(snap.begin_interval_time, 'hh24') hh24,
                     os.stat_name,
                     os.value cumulative_value,
                     os.delta_value,
                     ( TO_NUMBER(CAST(end_interval_time AS DATE) - CAST(begin_interval_time AS DATE)) * 60 * 60 * 24 )
                        snap_total_secs,
                     PERCENTILE_CONT(0.5)
                       within GROUP (ORDER BY os.delta_value DESC) over (
                         PARTITION BY os.dbid, os.instance_number,
                       TO_CHAR(snap.begin_interval_time, 'hh24'), os.stat_name) AS
                     "PERC50",
                     PERCENTILE_CONT(0.25)
                       within GROUP (ORDER BY os.delta_value DESC) over (
                         PARTITION BY os.dbid, os.instance_number,
                       TO_CHAR(snap.begin_interval_time, 'hh24'), os.stat_name) AS
                     "PERC75",
                     PERCENTILE_CONT(0.1)
                       within GROUP (ORDER BY os.delta_value DESC) over (
                         PARTITION BY os.dbid, os.instance_number,
                       TO_CHAR(snap.begin_interval_time, 'hh24'), os.stat_name) AS
                     "PERC90",
                     PERCENTILE_CONT(0.05)
                       within GROUP (ORDER BY os.delta_value DESC) over (
                         PARTITION BY os.dbid, os.instance_number,
                       TO_CHAR(snap.begin_interval_time, 'hh24'), os.stat_name) AS
                     "PERC95",
                     PERCENTILE_CONT(0)
                       within GROUP (ORDER BY os.delta_value DESC) over (
                         PARTITION BY os.dbid, os.instance_number,
                       TO_CHAR(snap.begin_interval_time, 'hh24'), os.stat_name) AS
                     "PERC100"
              FROM (SELECT s.*,
                    NVL(DECODE(GREATEST(value, NVL(LAG(value)
                    OVER (
                    PARTITION BY s.dbid, s.instance_number, s.stat_name
                    ORDER BY s.snap_id), 0)), value, value - LAG(value)
                       OVER (
                       PARTITION BY s.dbid, s.instance_number, s.stat_name
                       ORDER BY s.snap_id),
                    0), 0) AS delta_value
                    FROM dba_hist_osstat s
                    WHERE s.snap_id BETWEEN '&&v_min_snapid' AND '&&v_max_snapid'
                    ORDER BY snap_id) os
                     inner join dba_hist_snapshot snap
                             ON os.snap_id = snap.snap_id
              WHERE  os.snap_id BETWEEN '&&v_min_snapid' AND '&&v_max_snapid'),
vossummary AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'        AS pkey,
       dbid,
       instance_number,
       hh24,
       stat_name,
       SUM(snap_total_secs) hh24_total_secs,
       AVG(cumulative_value) cumulative_value,
       AVG(delta_value)           avg_value,
       STATS_MODE(delta_value)    mode_value,
       MEDIAN(delta_value)        median_value,
       AVG(perc50)          PERC50,
       AVG(perc75)          PERC75,
       AVG(perc90)          PERC90,
       AVG(perc95)          PERC95,
       AVG(perc100)         PERC100,
       MIN(delta_value)           min_value,
       MAX(delta_value)           max_value,
       SUM(delta_value)           sum_value,
       COUNT(1)             count
FROM   v_osstat_all
GROUP  BY '&&v_host'
          || '_'
          || '&&v_dbname'
          || '_'
          || '&&v_hora',
          dbid,
          instance_number,
          hh24,
          stat_name)
SELECT pkey ||' , '|| dbid ||' , '|| instance_number ||' , '|| hh24 ||' , '|| stat_name ||' , '|| hh24_total_secs ||' , '||
       cumulative_value ||' , '|| avg_value ||' , '|| mode_value ||' , '|| median_value ||' , '|| PERC50 ||' , '|| PERC75 ||' , '|| PERC90 ||' , '|| PERC95 ||' , '|| PERC100 ||' , '||
	     min_value ||' , '|| max_value ||' , '|| sum_value ||' , '|| count
FROM vossummary;

spool off

spool opdb__awrhistcmdtypes__&v_tag

WITH vcmdtype AS(
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                          AS pkey,
	   a.con_id,
       TO_CHAR(c.begin_interval_time, 'hh24') hh24,
       b.command_type,
       COUNT(1)                               cnt,
       AVG(buffer_gets_delta)                 AVG_BUFFER_GETS,
       AVG(elapsed_time_delta)                AVG_ELASPED_TIME,
       AVG(rows_processed_delta)              AVG_ROWS_PROCESSED,
       AVG(executions_delta)                  AVG_EXECUTIONS,
       AVG(cpu_time_delta)                    AVG_CPU_TIME,
       AVG(iowait_delta)                      AVG_IOWAIT,
       AVG(clwait_delta)                      AVG_CLWAIT,
       AVG(apwait_delta)                      AVG_APWAIT,
       AVG(ccwait_delta)                      AVG_CCWAIT,
       AVG(plsexec_time_delta)                AVG_PLSEXEC_TIME
FROM   dba_hist_sqlstat a
       inner join dba_hist_sqltext b
               ON ( a.con_id = b.con_id
                    AND a.sql_id = b.sql_id )
       inner join dba_hist_snapshot c
               ON ( a.snap_id = c.snap_id )
WHERE  a.snap_id BETWEEN '&&v_min_snapid' AND '&&v_max_snapid'
GROUP  BY '&&v_host'
          || '_'
          || '&&v_dbname'
          || '_'
          || '&&v_hora',
		  a.con_id,
          TO_CHAR(c.begin_interval_time, 'hh24'),
          b.command_type)
SELECT pkey ||' , '|| con_id ||' , '|| hh24 ||' , '|| command_type ||' , '|| cnt ||' , '|| avg_buffer_gets ||' , '|| avg_elasped_time ||' , '||
       avg_rows_processed ||' , '|| avg_executions ||' , '|| avg_cpu_time ||' , '|| avg_iowait ||' , '|| avg_clwait ||' , '||
	   avg_apwait ||' , '|| avg_ccwait ||' , '|| avg_plsexec_time
FROM vcmdtype;

spool off

spool opdb__dbahistsystimemodel__&v_tag

WITH vtimemodel AS (
SELECT
      '&&v_host' || '_' || '&&v_dbname' || '_' || '&&v_hora' as pkey,
      dbid,
      instance_number,
      hour,
      stat_name,
       COUNT(1)                             cnt,
       AVG(value)                           avg_value,
       STATS_MODE(value)                    mode_value,
       MEDIAN(value)                        median_value,
       MIN(value)                           min_value,
       MAX(value)                           max_value,
       SUM(value)                           sum_value,
       PERCENTILE_CONT(0.5)
         within GROUP (ORDER BY value DESC) AS "PERC50",
       PERCENTILE_CONT(0.25)
         within GROUP (ORDER BY value DESC) AS "PERC75",
       PERCENTILE_CONT(0.10)
         within GROUP (ORDER BY value DESC) AS "PERC90",
       PERCENTILE_CONT(0.05)
         within GROUP (ORDER BY value DESC) AS "PERC95",
       PERCENTILE_CONT(0)
         within GROUP (ORDER BY value DESC) AS "PERC100"
FROM (
SELECT
       s.snap_id,
       s.dbid,
       s.instance_number,
       s.begin_interval_time,
       to_char(s.begin_interval_time,'hh24') hour,
       g.stat_name,
       NVL(DECODE(GREATEST(value, NVL(LAG(value)
                                        over (
                                          PARTITION BY s.dbid, s.instance_number, g.stat_name
                                          ORDER BY s.snap_id), 0)), value, value - LAG(value)
                                                                                     over (
                                                                                       PARTITION BY s.dbid, s.instance_number, g.stat_name
                                                                                       ORDER BY s.snap_id),
                                                                    0), 0) AS value
FROM   dba_hist_snapshot s,
       dba_hist_sys_time_model g
WHERE  s.snap_id = g.snap_id
       AND s.snap_id BETWEEN '&&v_min_snapid' AND '&&v_max_snapid'
       AND s.instance_number = g.instance_number
       AND s.dbid = g.dbid
)
GROUP BY
      '&&v_host' || '_' || '&&v_dbname' || '_' || '&&v_hora',
      dbid,
      instance_number,
      hour,
      stat_name)
SELECT pkey ||','|| dbid ||','|| instance_number ||','|| hour ||','|| stat_name ||','|| cnt ||','||
       avg_value ||','|| mode_value ||','|| median_value ||','|| min_value ||','|| max_value ||','||
	   sum_value ||','|| perc50 ||','|| perc75 ||','|| perc90 ||','|| perc95 ||','|| perc100
FROM vtimemodel;

spool off

spool opdb__dbahistsysstat__&v_tag

WITH vsysstat AS (
SELECT
       '&&v_host' || '_' || '&&v_dbname' || '_' || '&&v_hora' as pkey,
       dbid,
       instance_number,
       hour,
       stat_name,
       COUNT(1)                             cnt,
       AVG(value)                           avg_value,
       STATS_MODE(value)                    mode_value,
       MEDIAN(value)                        median_value,
       MIN(value)                           min_value,
       MAX(value)                           max_value,
       SUM(value)                           sum_value,
       PERCENTILE_CONT(0.5)
         within GROUP (ORDER BY value DESC) AS "PERC50",
       PERCENTILE_CONT(0.25)
         within GROUP (ORDER BY value DESC) AS "PERC75",
       PERCENTILE_CONT(0.10)
         within GROUP (ORDER BY value DESC) AS "PERC90",
       PERCENTILE_CONT(0.05)
         within GROUP (ORDER BY value DESC) AS "PERC95",
       PERCENTILE_CONT(0)
         within GROUP (ORDER BY value DESC) AS "PERC100"
FROM (
SELECT
       s.snap_id,
       s.dbid,
       s.instance_number,
       s.begin_interval_time,
       to_char(s.begin_interval_time,'hh24') hour,
       g.stat_name,
       NVL(DECODE(GREATEST(value, NVL(LAG(value)
                                        over (
                                          PARTITION BY s.dbid, s.instance_number, g.stat_name
                                          ORDER BY s.snap_id), 0)), value, value - LAG(value)
                                                                                     over (
                                                                                       PARTITION BY s.dbid, s.instance_number, g.stat_name
                                                                                       ORDER BY s.snap_id),
                                                                    0), 0) AS VALUE
FROM   dba_hist_snapshot s,
       dba_hist_sysstat g
WHERE  s.snap_id = g.snap_id
       AND s.snap_id BETWEEN '&&v_min_snapid' AND '&&v_max_snapid'
       AND s.instance_number = g.instance_number
       AND s.dbid = g.dbid
       AND (LOWER(stat_name) LIKE '%db%time%'
       or LOWER(stat_name) LIKE '%redo%time%'
       or LOWER(stat_name) LIKE '%parse%time%'
       or LOWER(stat_name) LIKE 'phy%'
       or LOWER(stat_name) LIKE '%cpu%'
      -- or LOWER(stat_name) LIKE '%hcc%'
       or LOWER(stat_name) LIKE 'cell%phy%'
       or LOWER(stat_name) LIKE 'cell%smart%'
       or LOWER(stat_name) LIKE 'cell%mem%'
       or LOWER(stat_name) LIKE 'cell%flash%'
       or LOWER(stat_name) LIKE '%db%block%'
       or LOWER(stat_name) LIKE '%execute%'
      -- or LOWER(stat_name) LIKE '%lob%'
       or LOWER(stat_name) LIKE 'user%')
)
GROUP BY
          '&&v_host' || '_' || '&&v_dbname' || '_' || '&&v_hora',
          dbid,
          instance_number,
          hour,
          stat_name)
SELECT pkey ||','|| dbid ||','|| instance_number ||','|| hour ||','|| stat_name ||','|| cnt ||','||
       avg_value ||','|| mode_value ||','|| median_value ||','|| min_value ||','|| max_value ||','||
	   sum_value ||','|| perc50 ||','|| perc75 ||','|| perc90 ||','|| perc95 ||','|| perc100
FROM vsysstat;

spool off

spool opdb__dbservicesinfo__&v_tag

WITH vservices AS (
SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                          AS pkey,
       con_id,
       pdb,
       service_id,
       name service_name,
       network_name,
       TO_CHAR(creation_date, 'dd/mm/yyyy hh24:mi:ss') creation_date,
       failover_method,
       failover_type,
       failover_retries,
       failover_delay,
       goal
FROM cdb_services
ORDER BY NAME)
SELECT pkey ||' , '|| con_id ||' , '|| pdb ||' , '|| service_id ||' , '|| service_name ||' , '|| network_name ||' , '|| creation_date ||' , '||
       failover_method ||' , '|| failover_type ||' , '|| failover_retries ||' , '|| failover_delay ||' , '|| goal
FROM vservices;

spool off

spool opdb__usrsegatt__&v_tag

WITH vuseg AS (
 SELECT '&&v_host'
       || '_'
       || '&&v_dbname'
       || '_'
       || '&&v_hora'                          AS pkey,
        con_id,
        owner,
        segment_name,
        segment_type,
        tablespace_name
 FROM cdb_segments
 WHERE tablespace_name IN ('SYS', 'SYSTEM')
 AND owner NOT IN
 (SELECT name
  FROM system.logstdby$skip_support
  WHERE action=0))
SELECT pkey ||' , '|| con_id ||' , '|| owner ||' , '|| segment_name ||' , '|| segment_type ||' , '|| tablespace_name
FROM vuseg;

spool off
