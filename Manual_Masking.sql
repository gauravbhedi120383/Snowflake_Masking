-- MANUAL DEMO

use role accountadmin;
--demo db,scham and table creation
create database mydb;
create schema mydb.my_schema;
create table mydb.my_schema.hr_data ( account_number number(38,0), First_name varchar, last_name varchar, dob date, salary number(38,9), address varchar, pin number(28,0));
insert into mydb.my_schema.hr_data values(91102001031,'Aditya1','joshi1','1983-12-03',999999.00, 'pune abc2232', 411024);
insert into mydb.my_schema.hr_data values(9110200145,'Aditya2','joshi2','1983-12-04',9999454.00, 'pune abc2231', 411021);
create table mydb.my_schema.EXTRACT_SEMANTIC_CATEGORIES_response(table_name varchar, response_json variant);
CALL  EXTRACT_SEMANTIC_CATEGORIES('mydb.my_schema.hr_data');
insert into mydb.my_schema.EXTRACT_SEMANTIC_CATEGORIES_response( SELECT 'mydb.my_schema.hr_data', * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
select * from mydb.my_schema.EXTRACT_SEMANTIC_CATEGORIES_response;
--Flatening the json response
SELECT TABLE_NAME ,
    FLAT.KEY,
    FLAT.value:"recommendation":"privacy_category"::VARCHAR as privacy_category,
    FLAT.value:"recommendation":"semantic_category"::VARCHAR as semantic_category,
    FLAT.value:"recommendation":"confidence"::VARCHAR as confidence,
    FLAT.value:"recommendation":"coverage"::NUMBER(10,2) as coverage,
    FLAT.value:"details"::variant as details,
    FLAT.value:"alternates"::VARIANT as alternates
  FROM mydb.my_schema.EXTRACT_SEMANTIC_CATEGORIES_response ,
       LATERAL FLATTEN( INPUT => RESPONSE_JSON ) FLAT
  WHERE FLAT.value:"recommendation":"privacy_category"::VARCHAR IS NOT NULL;
  
  grant ownership on database mydb to role public copy current grants;
  grant ownership on schema  mydb.my_schema to role public copy current grants;
  grant ownership on all tables  in schema mydb.my_schema to role public copy current grants;

-- Creation of new role
use role securityadmin;
create role if not exists tag_admin comment = "Role to Manage Snowflake Tags";
GRANT USAGE ON DATABASE mydb TO ROLE tag_admin;
GRANT USAGE ON SCHEMA mydb.my_schema TO ROLE tag_admin;
grant create masking policy on schema mydb.my_schema to role tag_admin;
grant create tag on schema mydb.my_schema to role tag_admin;

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

-- Associate tag with object 
ALTER TABLE MYDB.MY_SCHEMA.HR_DATA MODIFY COLUMN ACCOUNT_NUMBER  SET TAG DEMO_TAG='IDENTIFIER';
ALTER TABLE MYDB.MY_SCHEMA.HR_DATA MODIFY COLUMN DOB  SET TAG DEMO_TAG='QUASI_IDENTIFIER';
ALTER TABLE MYDB.MY_SCHEMA.HR_DATA MODIFY COLUMN SALARY  SET TAG DEMO_TAG='SENSITIVE' ;

SELECT SYSTEM$GET_TAG('TAG_NAME', 'TABLE.COLUMN', 'DOMAIN');
SELECT SYSTEM$GET_TAG('DEMO_TAG', 'HR_DATA.SALARY', 'COLUMN');


SELECT * FROM MYDB.MY_SCHEMA.HR_DATA;
--O/P UNMASKED DATA

ALTER TAG  MYDB.MY_SCHEMA.DEMO_TAG
 SET MASKING POLICY account_name_mask,
	 MASKING POLICY account_number_mask;
	 
SELECT * FROM MYDB.MY_SCHEMA.HR_DATA;
--O/P MASKED DATA