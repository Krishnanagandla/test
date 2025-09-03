CREATE OR REPLACE PROCEDURE EDW_DEV.EDW_APPL.CLONE_DB_OWNER_PRIVS2("DBNAME" VARCHAR, "SOURCE_ENV" VARCHAR, "DEST_ENV" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS 
'
/*
Definition: Procedure resets ownership privileges in the cloned database.
Params:
   DBNAME (STRING): Name of the cloned database.
   SOURCE_ENV (STRING): Name of the source environment (PROD)
   DEST_ENV (STRING): Name of the destination environment (TEST,DEV)

*/
var SQLSTRING = "select ''grant ownership on table '' || table_catalog || ''.'' || table_schema || ''.'' || table_name || '' to role '' || \\
case when table_owner=''FR_SA_PROD_MAT_BI_PROD'' then ''FR_SA_PROD_MAT_BI_DEV''  \\
when table_catalog in (''DEV_AUDIT_TEST'') then ''FR_ITBI_DW_ETL''  \\
when table_catalog in (''DEV_DSL_TEST'') then ''FR_ITBI_DW_ETL''  \\
when table_catalog in (''DEV_EDW_TEST'') then ''FR_ITBI_DW_ETL''  \\
when table_catalog in (''DEV_MART_TEST'') then ''FR_ITBI_DW_ETL''  \\
when table_catalog in (''DEV_OUT_TEST'') then ''FR_ITBI_DW_ETL''  \\
when table_catalog in (''DEV_STG_TEST'') then ''FR_ITBI_DW_ETL''  \\
when table_catalog in (''DEV_BI_TEST'') then ''FR_ITBI_DW_BI''  \\
else null end || '' copy current grants;'' from snowflake.account_usage.tables where table_catalog = ''DEST_DB'' and deleted is null;";

SQLSTRING = SQLSTRING.replaceAll("DEST_DB", DBNAME);
SQLSTRING = SQLSTRING.replaceAll(SOURCE_ENV, DEST_ENV);

var SQLSTMT = snowflake.createStatement( {sqlText: SQLSTRING} );
var results = SQLSTMT.execute();
// Loop through results and execute individual statements.
while( results.next() ){
   var newRole = results.getColumnValue(1);
   var sql_command = `select count(*) as cnt
                   from snowflake.account_usage.roles
                   where name = upper(:1)`;

    var stmt = snowflake.createStatement({sqlText: sql_command, binds:[newRole]});
    var rs = stmt.execute();
    rs.next();  // move to first row
    var exists = rs.getColumnValue(1); // boolean
    if (exists) {
        var SQLPERM = results.getColumnValue(2);
        snowflake.execute( {sqlText: SQLPERM });
    }   
}

return "DONE!"
'
;

call EDW_DEV.EDW_APPL.CLONE_DB_OWNER_PRIVS2('DB_PROD', 'PROD', 'DEV');
