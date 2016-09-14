# Migrating Relational Data From Oracle To Cassandra

<H1>WORK IN PROGRESS - come back later</H1>  

--
<br>
<H2>Fix VBox Shared Clipboard</H2>
My clipboard didn't seem to work but this fixes it.

In Virtual Box mount the Guest Additions CD and run it when prompted for the root password.

Restart the machine. 

Then:
<pre>
# killall VBoxClient
# VBoxClient-all
</pre>
I find that I have to enable the bidirectional clipboard I have to run the last two commands every time I start the machine.

<br>
<br>

<H1>Set Up DataStax Components</H1>
<br>
<h2>Add The DataStax Repo</H2>
As root create ```/etc/yum.repos.d/datastax.repo```
<pre># vi /etc/yum.repos.d/datastax.repo
</pre>

<br>
Paste in these lines:
<pre>
[datastax] 
name = DataStax Repo for DataStax Enterprise
baseurl=https://datastaxrepo_gmail.com:utJVKEg4lKeaWTX@rpm.datastax.com/enterprise
enabled=1
gpgcheck=0
</pre>
<br>

<h2>Import The DataStax Repo Key</H2>
<pre>
rpm --import http://rpm.datastax.com/rpm/repo_key 
</pre>
<br>


<h2>Install DSE Components</H2>
<h3>DSE Platform</h3>
<pre>
# yum install dse-full-5.0.1-1
</pre>

<h3>DataStax OpsCenter</h3>
<pre>
# yum install opscenter --> 6.0.2.1
</pre>

<h3>DataStax OpsCenter Agent</h3>
<pre>
# yum install datastax-agent --> 6.0.2.1
</pre>
<br>

<h2>Enable Search & Analytics</h2>

We want to use Search (Solr) and Analytics (Spark) so we need to delete the default datacentre and restart the cluster (if its already running) in SearchAnalytics mode.
<br>

Stop the service if it's running.
<pre>
#  service dse stop
Stopping DSE daemon : dse                                  [  OK  ]
</pre>
<br>

Enable Solr and Spark by changing the flag from "0" to "1" in:
<pre>
# vi /etc/default/dse
</pre>
e.g.:
<pre>
# Start the node in DSE Search mode
SOLR_ENABLED=1

# Start the node in Spark mode
SPARK_ENABLED=1
</pre>
<br>

Delete the old (Cassandra-only) datacentre databases if they exist:
<pre>
# rm -rf /var/lib/cassandra/data/*
# rm -rf /var/lib/cassandra/saved_caches/*
# rm -rf /var/lib/cassandra/commitlog/*
# rm -rf /var/lib/cassandra/hints/*
</pre>
<br>

Remove the old system.log if it exists:
<pre>
# rm -rf /var/log/cassandra/system.log 
</pre>
<br>

<br>
<H2>Identify Spark Master</h2>
We use the new DSE 5.0 format for the dse tool to get the address of the Spark Master in our cluster. 
As we are using a single node for our cluster it will be no surprise that the Spark Master is also on our single node!
<pre>
$ dse client-tool spark master-address
spark://127.0.0.1:7077
</pre> 
OK, let's go look at our source data in the Oracle database.


<br>
<h1>Oracle Database</h1>

<br>
<h2>Create HR User For The Sample Database</h2>
Allow local User ID's to be created:
<pre>
SQL> alter session set "_ORACLE_SCRIPT"=true; 
</pre>

Create HR User using the Oracle-supplied scripts:
<pre>
@?/demo/schema/human_resources/hr_main.sql
</pre>

Respond with
- hr
- users
- temp
- <your sys password>
- $ORACLE_HOME/demo/schema/log/

You should see
<pre>
PL/SQL procedure successfully completed.
</pre>
<br>

<h2>Investigate The HR Schema</h2>

<h3>What Tables Do We Have?</h3>
<pre>
SQL> connect hr/hr
SQL> set lines 180

SQL> select table_name from user_tables;

TABLE_NAME
------------------------
REGIONS
COUNTRIES
LOCATIONS
DEPARTMENTS
JOBS
EMPLOYEES
JOB_HISTORY

7 rows selected.
</pre>

<h3>Table EMPLOYEES</h3>
<pre>
SQL> desc employees

 Name                    Null?    Type
 ----------------------- -------- ----------------
 EMPLOYEE_ID             NOT NULL NUMBER(6)
 FIRST_NAME                       VARCHAR2(20)
 LAST_NAME               NOT NULL VARCHAR2(25)
 EMAIL                   NOT NULL VARCHAR2(25)
 PHONE_NUMBER                     VARCHAR2(20)
 HIRE_DATE               NOT NULL DATE
 JOB_ID                  NOT NULL VARCHAR2(10)
 SALARY                           NUMBER(8,2)
 COMMISSION_PCT                   NUMBER(2,2)
 MANAGER_ID                       NUMBER(6)
 DEPARTMENT_ID                    NUMBER(4)
</pre>

<h3>Table JOBS</h3>
<pre>
SQL> desc jobs

 Name                    Null?    Type
 ----------------------- -------- ----------------
 JOB_ID                  NOT NULL VARCHAR2(10)
 JOB_TITLE               NOT NULL VARCHAR2(35)
 MIN_SALARY                       NUMBER(6)
 MAX_SALARY                       NUMBER(6)
</pre>

<h3>Table DEPARTMENTS</h3>

<pre>
SQL> desc departments

 Name                    Null?    Type
 ----------------------- -------- ----------------
 DEPARTMENT_ID           NOT NULL NUMBER(4)
 DEPARTMENT_NAME         NOT NULL VARCHAR2(30)
 MANAGER_ID                       NUMBER(6)
 LOCATION_ID                       NUMBER(4)
</pre>

<h3>Table LOCATIONS</h3>

<pre>
SQL> desc locations

 Name                    Null?    Type
 ----------------------- -------- ----------------
 LOCATION_ID             NOT NULL NUMBER(4)
 STREET_ADDRESS                   VARCHAR2(40)
 POSTAL_CODE                      VARCHAR2(12)
 CITY                    NOT NULL VARCHAR2(30)
 STATE_PROVINCE                   VARCHAR2(25)
 COUNTRY_ID                       CHAR(2)
</pre>

<h3>Table COUNTRIES</h3>

<pre>
SQL> desc countries

 Name                    Null?    Type
 ----------------------- -------- ----------------
 COUNTRY_ID              NOT NULL CHAR(2)
 COUNTRY_NAME                     VARCHAR2(40)
 REGION_ID                        NUMBER
</pre>

<h2>Explore The HR Data</h2>

In SQLPlus we can select employees from the employees table, for example:
<pre>
SQL> select * from employees;

EMPLOYEE_ID FIRST_NAME           LAST_NAME            EMAIL                PHONE_NUMBER         HIRE_DATE JOB_ID         SALARY COMMISSION_PCT MANAGER_ID DEPARTMENT_ID
----------- -------------------- -------------------- -------------------- -------------------- --------- ---------- ---------- -------------- ---------- -------------
        120 Matthew              Weiss                MWEISS               650.123.1234         18-JUL-04 ST_MAN           8000                       100            50
        121 Adam                 Fripp                AFRIPP               650.123.2234         10-APR-05 ST_MAN           8200                       100            50
        122 Payam                Kaufling             PKAUFLIN             650.123.3234         01-MAY-03 ST_MAN           7900                       100            50
        123 Shanta               Vollman              SVOLLMAN             650.123.4234         10-OCT-05 ST_MAN           6500                       100            50
        124 Kevin                Mourgos              KMOURGOS             650.123.5234         16-NOV-07 ST_MAN           5800                       100            50
        125 Julia                Nayer                JNAYER               650.124.1214         16-JUL-05 ST_CLERK         3200                       120            50
        126 Irene                Mikkilineni          IMIKKILI             650.124.1224         28-SEP-06 ST_CLERK         2700                       120            50
</pre>


We want to focus on employees reporting to a manager with ID=121:

<pre>
SQL> select * from employees where manager_id=121;

EMPLOYEE_ID FIRST_NAME           LAST_NAME            EMAIL                PHONE_NUMBER         HIRE_DATE JOB_ID         SALARY COMMISSION_PCT MANAGER_ID DEPARTMENT_ID
----------- -------------------- -------------------- -------------------- -------------------- --------- ---------- ---------- -------------- ---------- -------------
        129 Laura                Bissot               LBISSOT              650.124.5234         20-AUG-05 ST_CLERK         3300                       121            50
        130 Mozhe                Atkinson             MATKINSO             650.124.6234         30-OCT-05 ST_CLERK         2800                       121            50
        131 James                Marlow               JAMRLOW              650.124.7234         16-FEB-05 ST_CLERK         2500                       121            50
        132 TJ                   Olson                TJOLSON              650.124.8234         10-APR-07 ST_CLERK         2100                       121            50
        184 Nandita              Sarchand             NSARCHAN             650.509.1876         27-JAN-04 SH_CLERK         4200                       121            50
        185 Alexis               Bull                 ABULL                650.509.2876         20-FEB-05 SH_CLERK         4100                       121            50
        186 Julia                Dellinger            JDELLING             650.509.3876         24-JUN-06 SH_CLERK         3400                       121            50
        187 Anthony              Cabrio               ACABRIO              650.509.4876         07-FEB-07 SH_CLERK         3000                       121            50
</pre>

Who is that manager?

<pre>
SQL> select * from employees where employee_id=121;

EMPLOYEE_ID FIRST_NAME           LAST_NAME            EMAIL                PHONE_NUMBER         HIRE_DATE JOB_ID         SALARY COMMISSION_PCT MANAGER_ID DEPARTMENT_ID
----------- -------------------- -------------------- -------------------- -------------------- --------- ---------- ---------- -------------- ---------- -------------
        121 Adam                 Fripp                AFRIPP               650.123.2234         10-APR-05 ST_MAN           8200                       100            50
</pre>

What is HIS job?
<pre>
SQL> select * from jobs where job_id='ST_MAN';

JOB_ID     JOB_TITLE                           MIN_SALARY MAX_SALARY
---------- ----------------------------------- ---------- ----------
ST_MAN     Stock Manager                             5500       8500
</pre>

Who is <b>HIS</b> boss?

<pre>
EMPLOYEE_ID FIRST_NAME           LAST_NAME            EMAIL                PHONE_NUMBER         HIRE_DATE JOB_ID         SALARY COMMISSION_PCT MANAGER_ID DEPARTMENT_ID
----------- -------------------- -------------------- -------------------- -------------------- --------- ---------- ---------- -------------- ---------- -------------
        100 Steven               King                 SKING                515.123.4567         17-JUN-03 AD_PRES         24000                                      90
</pre>
They work in Department=50 - what is that?
<pre>
SQL> select * from departments where department_id=50;

DEPARTMENT_ID DEPARTMENT_NAME                MANAGER_ID LOCATION_ID
------------- ------------------------------ ---------- -----------
           50 Shipping                              121        1500
</pre>
Where is location=1500?
<pre>
SQL> select * from locations where location_id=1500;

LOCATION_ID STREET_ADDRESS                           POSTAL_CODE  CITY                           STATE_PROVINCE            CO
----------- ---------------------------------------- ------------ ------------------------------ ------------------------- --
       1500 2011 Interiors Blvd                      99236        South San Francisco            California                US
</pre>
What country is that in?
<pre>
SQL> select * from countries where country_id='US';

CO COUNTRY_NAME                              REGION_ID
-- ---------------------------------------- ----------
US United States of America                          2

1 row selected.

SQL> select * from regions where region_id=2;

 REGION_ID REGION_NAME
---------- -------------------------
         2 Americas
</pre>


<br>
<h1>Using Spark To Read Oracle Data</h1>
The objective of this exercise is to use the DatFrame capability introduced in Apache Spark 1.3 to load data from tables in an Oracle database (12c) via Oracle's JDBC thin driver, 
 and generate a result set, joining tables where necessary.
 
At this point ensure that you have an Oracle database tns listener running. This usually runs on port 1521.

In my example the database name is orcl.
We will connect from Spark, using the Oracle jdbc driver, to the orcl database on TNS port 1521.
<br>


<h2>Download Oracle ojdbc7.jar</h2>
We have to add the Oracle JDBC jar file to our Spark classpath so that Spark knows how to talk to the Oracle database.

You can download the ojdbc JAR file from:

http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html

I've used ojdbc7.jar which is certified for use with both JDK7 and JDK8
In my Oracle SampleAppv607 VM Firefox downloaded this to /app/oracle/downloads/

<br>
<h2>Using The Oracle JDBC Driver</h2>
For this test we only need the jdbc driver file on our single (SparkMaster) node.
In a bigger cluster we would need to distribute it to the slave nodes too.

<h3>Update Executor Path For ojdbc7.jar In spark-defaults.conf</h3>
Add the classpath for the ojdbc7.jar file for the executors (the path for the driver seems be required on the command line at run time as well, see below).
<pre>
# vi /etc/dse/spark/spark-defaults.conf
</pre>
Add the following lines pointing to the location of your ojdbc7.jar file:
<pre>
spark.driver.extraClassPath = /app/oracle/downloads/ojdbc7.jarr
spark.executor.extraClassPath = /app/oracle/downloads/ojdbc7.jar
</pre>

<h3>Restart DSE</h3>
<pre>
$ sudo service dse stop
$ sudo service dse start
</pre>


<h2>Start The Spark REPL</h2>
I'm passing to the path to the ojdbc7.jar file on the command line (shouldn't be needed as the driver path is defined in the spark-defaults.conf file now, but it seems not to work without it).
<pre>
$ dse spark --driver-class-path /app/oracle/downloads/ojdbc7.jar -deprecation
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.2
      /_/
Using Scala version 2.10.5 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_77)
Type in expressions to have them evaluated.
Type :help for more information.
Initializing SparkContext with MASTER: spark://127.0.0.1:7077
Created spark context..
Spark context available as sc.
Hive context available as sqlContext. Will be initialized on first use.
</pre>
<br>
We import those classes again:
<pre lang="scala">
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._
</pre>
<br>
And try the data load from Oracle again - all good and no errors:
<pre lang="scala">
scala> val employees = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "employees"))
<console>:67: warning: method load in class SQLContext is deprecated: Use read.format(source).options(options).load(). This will be removed in Spark 2.0.
         val employees = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "employees"))


employees: org.apache.spark.sql.DataFrame = [EMPLOYEE_ID: decimal(6,0), FIRST_NAME: string, LAST_NAME: string, EMAIL: string, PHONE_NUMBER: string, HIRE_DATE: timestamp, JOB_ID: string, SALARY: decimal(8,2), COMMISSION_PCT: decimal(2,2), MANAGER_ID: decimal(6,0), DEPARTMENT_ID: decimal(4,0)]
</pre>


<h2>Examining The DatFrame</h2>

<pre lang="scala">
scala> employees.printSchema()
root
 |-- EMPLOYEE_ID: decimal(6,0) (nullable = false)
 |-- FIRST_NAME: string (nullable = true)
 |-- LAST_NAME: string (nullable = false)
 |-- EMAIL: string (nullable = false)
 |-- PHONE_NUMBER: string (nullable = true)
 |-- HIRE_DATE: timestamp (nullable = false)
 |-- JOB_ID: string (nullable = false)
 |-- SALARY: decimal(8,2) (nullable = true)
 |-- COMMISSION_PCT: decimal(2,2) (nullable = true)
 |-- MANAGER_ID: decimal(6,0) (nullable = true)
 |-- DEPARTMENT_ID: decimal(4,0) (nullable = true)
</pre>

<pre lang="scala">
scala> employees.show()
+-----------+-----------+----------+--------+------------+--------------------+----------+--------+--------------+----------+-------------+
|EMPLOYEE_ID| FIRST_NAME| LAST_NAME|   EMAIL|PHONE_NUMBER|           HIRE_DATE|    JOB_ID|  SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+-----------+----------+--------+------------+--------------------+----------+--------+--------------+----------+-------------+
|        100|     Steven|      King|   SKING|515.123.4567|2003-06-16 23:00:...|   AD_PRES|24000.00|          null|      null|           90|
|        101|      Neena|   Kochhar|NKOCHHAR|515.123.4568|2005-09-20 23:00:...|     AD_VP|17000.00|          null|       100|           90|
|        102|        Lex|   De Haan| LDEHAAN|515.123.4569|2001-01-13 00:00:...|     AD_VP|17000.00|          null|       100|           90|
|        103|  Alexander|    Hunold| AHUNOLD|590.423.4567|2006-01-03 00:00:...|   IT_PROG| 9000.00|          null|       102|           60|
|        104|      Bruce|     Ernst|  BERNST|590.423.4568|2007-05-20 23:00:...|   IT_PROG| 6000.00|          null|       103|           60|
|        105|      David|    Austin| DAUSTIN|590.423.4569|2005-06-24 23:00:...|   IT_PROG| 4800.00|          null|       103|           60|
|        106|      Valli| Pataballa|VPATABAL|590.423.4560|2006-02-05 00:00:...|   IT_PROG| 4800.00|          null|       103|           60|
|        107|      Diana|   Lorentz|DLORENTZ|590.423.5567|2007-02-07 00:00:...|   IT_PROG| 4200.00|          null|       103|           60|
|        108|      Nancy| Greenberg|NGREENBE|515.124.4569|2002-08-16 23:00:...|    FI_MGR|12008.00|          null|       101|          100|
|        109|     Daniel|    Faviet| DFAVIET|515.124.4169|2002-08-15 23:00:...|FI_ACCOUNT| 9000.00|          null|       108|          100|
|        110|       John|      Chen|   JCHEN|515.124.4269|2005-09-27 23:00:...|FI_ACCOUNT| 8200.00|          null|       108|          100|
|        111|     Ismael|   Sciarra|ISCIARRA|515.124.4369|2005-09-29 23:00:...|FI_ACCOUNT| 7700.00|          null|       108|          100|
|        112|Jose Manuel|     Urman| JMURMAN|515.124.4469|2006-03-07 00:00:...|FI_ACCOUNT| 7800.00|          null|       108|          100|
|        113|       Luis|      Popp|   LPOPP|515.124.4567|2007-12-07 00:00:...|FI_ACCOUNT| 6900.00|          null|       108|          100|
|        114|        Den|  Raphaely|DRAPHEAL|515.127.4561|2002-12-07 00:00:...|    PU_MAN|11000.00|          null|       100|           30|
|        115|  Alexander|      Khoo|   AKHOO|515.127.4562|2003-05-17 23:00:...|  PU_CLERK| 3100.00|          null|       114|           30|
|        116|     Shelli|     Baida|  SBAIDA|515.127.4563|2005-12-24 00:00:...|  PU_CLERK| 2900.00|          null|       114|           30|
|        117|      Sigal|    Tobias| STOBIAS|515.127.4564|2005-07-23 23:00:...|  PU_CLERK| 2800.00|          null|       114|           30|
|        118|        Guy|    Himuro| GHIMURO|515.127.4565|2006-11-15 00:00:...|  PU_CLERK| 2600.00|          null|       114|           30|
|        119|      Karen|Colmenares|KCOLMENA|515.127.4566|2007-08-09 23:00:...|  PU_CLERK| 2500.00|          null|       114|           30|
+-----------+-----------+----------+--------+------------+--------------------+----------+--------+--------------+----------+-------------+
only showing top 20 rows
</pre>

Cassandra Data Model
--------------------
Cassandra is a NoSQL distributed database so we cannot use the table joins that are in the relational Oracle HR database schema. 
We need to de-normalise the HR schema, removing foreign keys and look up tables - these are relational concepts.

For this exercise I will focus on the EMPLOYEES, JOBS and DEPARTMENTS tables.

Cassandra data modeling is a query-driven process - you first decide the queries that you wish to run, then build your data model around those queries. 
This is how Cassandra can ensure that your queries with scale with the cluster (this is usually the opposite of the relational world where you start with 
your data, decide what queries you want to run against it, then index it to the eyeballs to support those queries).

Remember that in a relational database maintaining those indexes is very expensive operation that becomes more expensive as the volume of data grows.
In contrast...<<<

In the Cassandra world we start with the queries and then design the data model to support those queries. 
We want to pull the source data from Oracle and move it to the tables in Cassandra. We will perform data transformations in Spark and SparkSQL to achieve this.

Our queries are:
- Query all employees on EMPLOYEE_ID
- Query all departments, optionally returning employees by department
- Query all jobs, optionally returning  employees by job
- Query all managers, optionally returning  employees by manager

In Cassandra we will create the following tables:
- 1 table for employees, similar to HR.EMPLOYEES but without the foreign keys to JOB_ID, MANAGER_ID and DEPARTMENT_ID. 
   The original DEPARTMENTS table has a FK MANAGER_ID to the EMPLOYEES table, so we will also add a clustering column MANAGES_DEPT_ID so that we can identify all departments for which an employee is a manager
   We will not make this clustering column part of the partitioning primary key because we may want to search on 
- We will replace the HR.DEPARTMENTS lookup table - we will use EMPLOYEES_BY_DEPARTMENT with a PK on DEPARTMENT_ID, clustered on EMPLOYEE_ID
- We will replace the HR.JOBS lookup table - we will use EMPLOYEES_BY_JOB with a PK on JOB_ID, clustered on EMPLOYEE_ID
- We will replace the FK on MANAGER_ID in the EMPLOYEES table - instead we will use an EMPLOYEES_BY_MANAGER table with a PK on MANAGER, clustered on EMPLOYEE_ID

<pre>
CREATE KEYSPACE IF NOT EXISTS HR WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
USE HR;
</pre>
This table will store information on employees, and for each employee there is a one to many relationship for the departments that employee manages. We could use a clustering column for the department(s) managed by that employee.

For each employee their first name, last name, email address etc is static data - so we can define them as static columns that belong to the partition key, not the clustering columns.

<pre>
DROP TABLE IF EXISTS EMPLOYEES;
CREATE TABLE employees (
 EMPLOYEE_ID            	bigint,
 FIRST_NAME             	text  static,
 LAST_NAME              	text static,
 EMAIL                  	text static,
 PHONE_NUMBER           	text static,
 HIRE_DATE              	text static,
 SALARY                 	decimal static,
 COMMISSION_PCT         	decimal static,
 MANAGES_DEPT_ID     		bigint,            <-- clustering cols start here
 MANAGES_DEPT_NAME	        text,
 PRIMARY KEY ((EMPLOYEE_ID), MANAGES_DEPT_ID));
</pre>

Alternatively we can completely denormalise and move the department data to a separate table. Data duplication in Cassandra is not a bad thing. It's quicker than updating indexes and disk is cheap, and Cassandra writes are fast!

So we could move the manager data out of the EMPLOYEES table and use the EMPLOYEES table purely for employee personal data - and put the department manager details in another table altogether.


<h3>Query all employees on EMPLOYEE_ID</h3>
This table satisfies query 1:
<pre>
DROP TABLE IF EXISTS EMPLOYEES;
CREATE TABLE employees (
 EMPLOYEE_ID            	bigint,
 FIRST_NAME             	text ,
 LAST_NAME              	text,
 EMAIL                  	text,
 PHONE_NUMBER           	text,
 HIRE_DATE              	text,
 SALARY                 	decimal,
 COMMISSION_PCT         	decimal,
 PRIMARY KEY (EMPLOYEE_ID);
</pre>

<h3>Query all departments, optionally returning employees by department</h3>

We have a table for employees stored by department - EMPLOYEE_ID is the clustering column.

<pre>
DROP TABLE IF EXISTS EMPLOYEES_BY_DEPT;;
CREATE TABLE employees_by_dept (
 DEPT_ID            	bigint,
 DEPARTMENT_NAME        text static,
 EMPLOYEE_ID            bigint,
 FIRST_NAME             text ,
 LAST_NAME              text,
 PRIMARY KEY (DEPT_ID, EMPLOYEE_ID));
</pre>


<h3>Query all jobs, optionally returning  employees by job</h3>


<h3>Query all managers, optionally returning  employees by manager</h3>












<H1>test data</H1>
<pre>
truncate table employees;
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (1,'simon',101,'sales');
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (2,'bill',102,'sales');
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (3,'jon',103,'sales');
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (4,'mary',104,'sales');
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (5,'jane',105,'sales');
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (6,'mike',106,'sales');
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (7,'ian',107,'sales');
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (8,'nige',108,'sales');
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (9,'steve',109,'sales');
insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (10,'julia',110,'sales');

insert into employees (employee_id, first_name, manages_dept_id, manages_dept_name) values (1,'simon',111,'pre-sales');
</pre>














??pip install cassandra-driver??





