CREATE OR REPLACE PROCEDURE EDW_DEV.EDW_APPL.CLONE_DB_OWNER_PRIVS("DBNAME" VARCHAR, "SOURCE_ENV" VARCHAR, "DEST_ENV" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
/*
Definition: Procedure resets ownership privileges in the cloned database.
Params:
   DBNAME (STRING): Name of the cloned database.
   SOURCE_ENV (STRING): Name of the source environment (PROD)
   DEST_ENV (STRING): Name of the destination environment (TEST,DEV)

*/
var SQLSTRING = "SELECT REPLACE(GRANTEE,''@'',''#''), ''GRANT OWNERSHIP ON '' || OBJECT_TYPE || '' '' || \\
CASE IFNULL(OBJECT_CATALOG,''.'') \\
   WHEN ''.'' THEN '''' \\
   ELSE REPLACE(OBJECT_CATALOG,''@'',''#'') || ''.'' \\
   END  || \\
CASE IFNULL(OBJECT_SCHEMA,''.'') \\
   WHEN ''.'' THEN '''' \\
   ELSE OBJECT_SCHEMA || ''.'' \\
   END || \\
CASE WHEN OBJECT_TYPE=''DATABASE'' THEN REPLACE(OBJECT_NAME,''@'',''#'') \\
   ELSE OBJECT_NAME \\
   END || \\
'' TO ROLE ''|| REPLACE(GRANTEE,''@'',''#'') || '' REVOKE CURRENT GRANTS;'' \\
FROM $.INFORMATION_SCHEMA.OBJECT_PRIVILEGES \\
WHERE (OBJECT_CATALOG=''$'' OR OBJECT_NAME = ''$'') AND PRIVILEGE_TYPE = ''OWNERSHIP'' \\
;";

SQLSTRING = SQLSTRING.replaceAll("$", DBNAME);
SQLSTRING = SQLSTRING.replaceAll("@", SOURCE_ENV);
SQLSTRING = SQLSTRING.replaceAll("#", DEST_ENV);

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

';
