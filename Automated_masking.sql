create database mydb;
create schema mydb.my_schema;
create table mydb.my_schema.hr_data ( account_number number(38,0), First_name varchar, last_name varchar, dob date, salary number(38,9), address varchar, pin number(28,0));
insert into mydb.my_schema.hr_data values(91102001031,'Aditya1','joshi1','1983-12-03',999999.00, 'pune abc2232', 411024);
insert into mydb.my_schema.hr_data values(9110200145,'Aditya2','joshi2','1983-12-04',9999454.00, 'pune abc2231', 411021);
create table mydb.my_schema.EXTRACT_SEMANTIC_CATEGORIES_response(table_name varchar, response_json variant);

STEP 1) Get the classification data for entire env, its a one time proces.

CREATE OR REPLACE PROCEDURE  MYDB.MY_SCHEMA.CLASSIFY()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var status=''SUCCESS'';

var sql_command2 ="SELECT DISTINCT CONCAT(TABLE_CATALOG , \'.\' , TABLE_SCHEMA ,\'.\' , TABLE_NAME) as TBL FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES WHERE DELETED IS NULL AND TABLE_CATALOG IN (\'MYDB\',\'MYDB2\')  AND TABLE_SCHEMA NOT LIKE \'PARADIME_%\'   AND TABLE_SCHEMA NOT LIKE \'FIVETRAN_%\'   AND TABLE_SCHEMA NOT LIKE \'%ELEMENTARY%\' AND CONCAT(TABLE_CATALOG , \'.\' , TABLE_SCHEMA ,\'.\' , TABLE_NAME)  NOT IN (SELECT TBL FROM MYDB.MY_SCHEMA.TBL_DETAILS);";

var stmt = snowflake.createStatement({  sqlText: sql_command2 });
var rs = stmt.execute();
    try {
            while (rs.next())    
                {      
                    var VAR_table_name = rs.getColumnValue(''TBL'');        
                    var var_SQL_stmt = "INSERT INTO MYDB.MY_SCHEMA.TBL_DETAILS SELECT \'"+ VAR_table_name +"\' , EXTRACT_SEMANTIC_CATEGORIES(\'"+VAR_table_name+"\');"
                   
                    var stmt = snowflake.createStatement({ sqlText: var_SQL_stmt });
                    var rs1 = stmt.execute();        
                    rs1.next();
                    ret_res= rs1.getColumnValue(1);
                }
        } catch(err)
                {
                        var status=''Failed'';
                }    
return status';

use role tag_admin;
-- Creating masking policys for demo
-- Masking Policy for String Datatype
CREATE OR REPLACE MASKING POLICY account_name_mask AS (val string) RETURNS string ->
  CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') 
       THEN val
	   ELSE '***MASKED***'
  END;
  
-- Masking Policy for Numeric Datatype
CREATE OR REPLACE MASKING POLICY account_number_mask AS (val number) RETURNS number ->
  CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') 
	   THEN val
	   ELSE -1
  END;
-- Creating TAG for demo
CREATE TAG MYDB.MY_SCHEMA.DEMO_TAG ALLOWED_VALUES "IDENTIFIER","QUASI_IDENTIFIER","SENSITIVE","INSENSITIVE";
SHOW TAGS ;


CREATE VIEW MYDB.MY_SCHEMA.EXTRACT_SEMANTIC_CATEGORIES_RESPONSE_VW
as 
SELECT TABLE_NAME ,
    FLAT.KEY  AS COLUMN_NAME,
    FLAT.value:"recommendation":"privacy_category"::VARCHAR as privacy_category,
    FLAT.value:"recommendation":"semantic_category"::VARCHAR as semantic_category,
    FLAT.value:"recommendation":"confidence"::VARCHAR as confidence,
    FLAT.value:"recommendation":"coverage"::NUMBER(10,2) as coverage,
    FLAT.value:"details"::variant as details,
    FLAT.value:"alternates"::VARIANT as alternates
  FROM mydb.my_schema.EXTRACT_SEMANTIC_CATEGORIES_response ,
       LATERAL FLATTEN( INPUT => RESPONSE_JSON ) FLAT
  WHERE FLAT.value:"recommendation":"privacy_category"::VARCHAR IS NOT NULL ;
  
  
-- Associate tag with object
CREATE OR REPLACE PROCEDURE  MYDB.MY_SCHEMA.ASSOCIATE_TAG()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var status=''SUCCESS'';

var sql_command2 ="SELECT TABLE_NAME ,  COLUMN_NAME, PRIVACY_CATEGORY FROM  mydb.my_schema.EXTRACT_SEMANTIC_CATEGORIES_response_vw WHERE TABLE_NAME NOT IN (SELECT DISTINCT CONCAT(OBJECT_DATABASE,\'.\',OBJECT_SCHEMA,\'.\',OBJECT_NAME) FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES WHERE OBJECT_DELETED IS NULL )";

var stmt = snowflake.createStatement({  sqlText: sql_command2 });
var rs = stmt.execute();
    try {
            while (rs.next())    
                {      
                    var VAR_table_name = rs.getColumnValue(''TABLE_NAME'');   
					var VAR_column_name = rs.getColumnValue(''COLUMN_NAME''); 
					var VAR_PRIVACY_CATEGORY = rs.getColumnValue(''PRIVACY_CATEGORY''); 

					var var_SQL_stmt = "ALTER TABLE " + VAR_table_name + " MODIFY COLUMN " +VAR_column_name + " SET TAG DEMO_TAG=\'" + VAR_PRIVACY_CATEGORY +"\';"					

                    var stmt = snowflake.createStatement({ sqlText: var_SQL_stmt });
                    var rs1 = stmt.execute();        
                    rs1.next();
                    ret_res= rs1.getColumnValue(1);
                }
        } catch(err)
                {
                        var status=''Failed'';
                }    
return status';