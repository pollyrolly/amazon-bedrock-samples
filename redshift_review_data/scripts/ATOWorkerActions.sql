WITH latest_worker_action AS
(
  SELECT table_id,
         TYPE,
         trim(status) as status,
         eventtime,
         ROW_NUMBER() OVER (PARTITION BY table_id,TYPE ORDER BY eventtime DESC) AS rnum
  FROM svl_auto_worker_action w
)
SELECT trim(t.database_name) AS dbname,
       t.schema_name AS namespace,
       t.table_name,
       w.type,
       w.status AS latest_status,
       w.eventtime
FROM latest_worker_action w
  INNER JOIN (SELECT DISTINCT(stv_tbl_perm.id) AS table_id
                    ,TRIM(pg_database.datname) AS database_name
                    ,TRIM(pg_namespace.nspname) AS schema_name
                    ,TRIM(relname) AS table_name
                FROM stv_tbl_perm
              INNER JOIN pg_database on pg_database.oid = stv_tbl_perm.db_id
              INNER JOIN pg_class on pg_class.oid = stv_tbl_perm.id
              INNER JOIN pg_namespace on pg_namespace.oid = pg_class.relnamespace
               WHERE schema_name NOT IN ('pg_internal', 'pg_catalog','pg_automv')) t
          ON (t.table_id = w.table_id AND w.rnum = 1)
ORDER BY 1,2,3,4;
