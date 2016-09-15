# Migrating Relational Data From Oracle To Cassandra - using Spark DataFrames, SparkSQL and the spark-cassandra connector
The objective of this exercise is to demonstrate how to migrate data from Oracle to Cassandra. I'll be using the DatFrame capability introduced in Apache Spark 1.3 to load data from tables in an Oracle database (12c) via Oracle's JDBC thin driver, to generate a result set, joining tables where necessary.
The data will then be saved to Cassandra.
 
<h2>Pre-requisites</h2>
<h3> DataStax Enterprise (release 5.0.2 at time of publication)</h3>
You'll need a working installation of DataStax Enterprise.

- Ubuntu/Debian - https://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/install/installDEBdse.html
- Red Hat/Fedora/CentOS/Oracle Linux - https://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/install/installRHELdse.html

To setup your environment, you'll also need the following resources:

- Python 2.7
- For Red Hat, CentOS and Fedora, install EPEL (Extra Packages for Enterprise Linux).
- An Oracle database:
 - In my example the database name is orcl
 - A running Oracle tns listener. In my example I'm using the default port of 1521.
 - You are able to make a tns connection to the Oracle database e.g. <b>"sqlplus user/password@service_name"</b>.
- The Oracle thin JDBC driver. You can download the ojdbc JAR file from:
http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html
I've used ojdbc7.jar which is certified for use with both JDK7 and JDK8
In my 12c Oracle VM Firefox this downloaded to /app/oracle/downloads/ so you'll see the path referenced in the instructions below.


As we will connect from Spark, using the Oracle jdbc driver, to the "orcl" database on TNS port 1521, all these components must be working correctly.
<br>

Now on to installing DataStax Enterprise and playing with some data!
--
<H1>Set Up DataStax Components</H1>
<br>
DSE installation instructions for DSE are provided at the top of this doc. I'll show the instructions for Red Hat/CentOS/Fedora here. I'm using an Oracle Enterprise Linux VirtualBox instance.
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

<h2>Import The DataStax Repo Key</H2>
<pre>
rpm --import http://rpm.datastax.com/rpm/repo_key 
</pre>


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

Remove the old system.log if it exists:
<pre>
# rm -rf /var/log/cassandra/system.log 
</pre>

Now restart DSE:
<pre>
$ sudo service DSE restart
</pre>

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
We're going to create our source data for this exercise using the scripts supplied by Oracle. 
We'll use the demo HR schema. 
<br>
<h2>Create HR User For The Sample Database</h2>
Log into SQLPlus as the sys or system user:
<pre>
$ sqlplus / as sysdba

SQL*Plus: Release 12.1.0.2.0 Production on Wed Sep 14 02:57:42 2016

Copyright (c) 1982, 2014, Oracle.  All rights reserved.
...
SQL> 
</pre>
<h3>Set _Oracle_SCRIPT</h3>
Run this command to allow local User ID's to be created:
<pre>
SQL> alter session set "_ORACLE_SCRIPT"=true; 
</pre>

<h3>Create HR User</h3>
Create the HR user by using the Oracle-supplied script. The "@?" is a substitute for $ORACLE_HOME.
<pre>
SQL> @?/demo/schema/human_resources/hr_main.sql
</pre>

Respond with these parameters
- hr
- users
- temp
- [your sys password]
- $ORACLE_HOME/demo/schema/log/

You should see
<pre>
PL/SQL procedure successfully completed.
</pre>
<br>

<h2>Investigate The HR Schema</h2>

<h3>What Tables Do We Have?</h3>
Log into SQL Plus as the HR user. 
> If you're still logged in as sys you can change to the HR account by typing "connect hr/hr").

![alt text] (https://raw.githubusercontent.com/simonambridge/Oracle_to_Cassandra/master/Oracle_to_Cassandra_OSchema.png)

<pre>
$ sqlplus hr/hr
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

There are seven tables in the HR schema. I'm going to look at five of them.

<h3>1. Table EMPLOYEES</h3>
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

<h3>2. Table JOBS</h3>
<pre>
SQL> desc jobs

 Name                    Null?    Type
 ----------------------- -------- ----------------
 JOB_ID                  NOT NULL VARCHAR2(10)
 JOB_TITLE               NOT NULL VARCHAR2(35)
 MIN_SALARY                       NUMBER(6)
 MAX_SALARY                       NUMBER(6)
</pre>

<h3>3. Table DEPARTMENTS</h3>

<pre>
SQL> desc departments

 Name                    Null?    Type
 ----------------------- -------- ----------------
 DEPARTMENT_ID           NOT NULL NUMBER(4)
 DEPARTMENT_NAME         NOT NULL VARCHAR2(30)
 MANAGER_ID                       NUMBER(6)
 LOCATION_ID                       NUMBER(4)
</pre>

<h3>4. Table LOCATIONS</h3>

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

<h3>5. Table COUNTRIES</h3>

<pre>
SQL> desc countries

 Name                    Null?    Type
 ----------------------- -------- ----------------
 COUNTRY_ID              NOT NULL CHAR(2)
 COUNTRY_NAME                     VARCHAR2(40)
 REGION_ID                        NUMBER
</pre>
<br>
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

Let's walk through the schema to get familiar with the dat that we're going to migrate.
For a moment let's just focus on employees reporting to a manager with ID=121:

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

A little relational reminder. Who <b>is</b> that manager with ID=121?

<pre>
SQL> select * from employees where employee_id=121;

EMPLOYEE_ID FIRST_NAME           LAST_NAME            EMAIL                PHONE_NUMBER         HIRE_DATE JOB_ID         SALARY COMMISSION_PCT MANAGER_ID DEPARTMENT_ID
----------- -------------------- -------------------- -------------------- -------------------- --------- ---------- ---------- -------------- ---------- -------------
        121 Adam                 Fripp                AFRIPP               650.123.2234         10-APR-05 ST_MAN           8200                       100            50
</pre>

...and what is <b>HIS</b> job?
<pre>
SQL> select * from jobs where job_id='ST_MAN';

JOB_ID     JOB_TITLE                           MIN_SALARY MAX_SALARY
---------- ----------------------------------- ---------- ----------
ST_MAN     Stock Manager                             5500       8500
</pre>

...and who is <b>HIS</b> boss?

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
It's in Location 1500. Where is location=1500?
<pre>
SQL> select * from locations where location_id=1500;

LOCATION_ID STREET_ADDRESS                           POSTAL_CODE  CITY                           STATE_PROVINCE            CO
----------- ---------------------------------------- ------------ ------------------------------ ------------------------- --
       1500 2011 Interiors Blvd                      99236        South San Francisco            California                US
</pre>
And column CO value is "US" - I wonder what country that is in?
<pre>
SQL> select * from countries where country_id='US';

CO COUNTRY_NAME                              REGION_ID
-- ---------------------------------------- ----------
US United States of America                          2

1 row selected.
</pre>
And the US is in the Americas:
<pre>
SQL> select * from regions where region_id=2;

 REGION_ID REGION_NAME
---------- -------------------------
         2 Americas
</pre>


<br>
<h1>Using Spark To Read Oracle Data</h1>
So all is looking good on the Oracle side. Now time to turn to Cassandra and Spark.

<h2>Download Oracle ojdbc7.jar</h2>
We have to add the Oracle JDBC jar file to our Spark classpath so that Spark knows how to talk to the Oracle database.


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
<h2>Import Some Classes</h2>
We import some classes:
<pre lang="scala">
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._
</pre>
<br>
<h2>Read Oracle Data Into A Spark DataFrame</h2>

And try the data load from Oracle again - all good and no errors:
<pre lang="scala">
scala> val employees = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "employees"))
<console>:67: warning: method load in class SQLContext is deprecated: Use read.format(source).options(options).load(). This will be removed in Spark 2.0.
         val employees = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "employees"))
</pre>
The Spark REPL responds with:
<pre>
employees: org.apache.spark.sql.DataFrame = [EMPLOYEE_ID: decimal(6,0), FIRST_NAME: string, LAST_NAME: string, EMAIL: string, PHONE_NUMBER: string, HIRE_DATE: timestamp, JOB_ID: string, SALARY: decimal(8,2), COMMISSION_PCT: decimal(2,2), MANAGER_ID: decimal(6,0), DEPARTMENT_ID: decimal(4,0)]
</pre>


<h2>Examining The DatFrame</h2>
We can use the nifty printSchema DataFrame method to show us the schema in the DataFrame that we created.
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
We can read the data from the DataFrame using the show() method:
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

Again, this time with Departments:
<pre lang="scala">
scala> val departments = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "departments"))
<console>:67: warning: method load in class SQLContext is deprecated: Use read.format(source).options(options).load(). This will be removed in Spark 2.0.
         val departments = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "departments"))                                      ^
</pre>
REPL responds:
<pre lang="scala">
departments: org.apache.spark.sql.DataFrame = [DEPARTMENT_ID: decimal(4,0), DEPARTMENT_NAME: string, MANAGER_ID: decimal(6,0), LOCATION_ID: decimal(4,0)]
</pre>
View Departments schema:
<pre lang="scala">
scala> departments.printSchema()
root
 |-- DEPARTMENT_ID: decimal(4,0) (nullable = false)
 |-- DEPARTMENT_NAME: string (nullable = false)
 |-- MANAGER_ID: decimal(6,0) (nullable = true)
 |-- LOCATION_ID: decimal(4,0) (nullable = true)
</pre>
Read records from teh Departments DataFrame:

<pre lang="scala">
scala> departments.show()
+-------------+--------------------+----------+-----------+
|DEPARTMENT_ID|     DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-------------+--------------------+----------+-----------+
|           10|      Administration|       200|       1700|
|           20|           Marketing|       201|       1800|
|           30|          Purchasing|       114|       1700|
|           40|     Human Resources|       203|       2400|
|           50|            Shipping|       121|       1500|
|           60|                  IT|       103|       1400|
|           70|    Public Relations|       204|       2700|
|           80|               Sales|       145|       2500|
|           90|           Executive|       100|       1700|
|          100|             Finance|       108|       1700|
|          110|          Accounting|       205|       1700|
|          120|            Treasury|      null|       1700|
|          130|       Corporate Tax|      null|       1700|
|          140|  Control And Credit|      null|       1700|
|          150|Shareholder Services|      null|       1700|
|          160|            Benefits|      null|       1700|
|          170|       Manufacturing|      null|       1700|
|          180|        Construction|      null|       1700|
|          190|         Contracting|      null|       1700|
|          200|          Operations|      null|       1700|
+-------------+--------------------+----------+-----------+
only showing top 20 rows
</pre>


<H2>Multi-Table Joins In SparkSQL</h2>
We may well need to perform some element of data transformation when we migrate data from a relational database to a NoSQL database like Apache Cassandra. Transformation typically involves deduplication of data and to do this we frequently want to join data between tables. Let's see how we do that in Spark.

<h3>Create SparkSQL Tables From The DataFrames</h3>
<pre lang="scala">
scala> employees.registerTempTable("empTable")

scala> departments.registerTempTable("deptTable")
</pre>
Now query empTable and deptTable joining on DEPARTMENT_ID:
<pre lang="scala">
scala> val depts_by_emp = sqlContext.sql("SELECT employee_id, department_name FROM empTable e, deptTable d where e.department_id=d.department_id")
depts_by_emp: org.apache.spark.sql.DataFrame = [employee_id: decimal(6,0), department_name: string]
</pre>
We can look at the results of the query:
<pre>
scala> depts_by_emp.show()
+-----------+---------------+                                                   
|employee_id|department_name|
+-----------+---------------+
|        203|Human Resources|
|        120|       Shipping|
|        121|       Shipping|
|        122|       Shipping|
|        123|       Shipping|
|        124|       Shipping|
|        125|       Shipping|
|        126|       Shipping|
|        127|       Shipping|
|        128|       Shipping|
|        129|       Shipping|
|        130|       Shipping|
|        131|       Shipping|
|        132|       Shipping|
|        133|       Shipping|
|        134|       Shipping|
|        135|       Shipping|
|        136|       Shipping|
|        137|       Shipping|
|        138|       Shipping|
+-----------+---------------+
only showing top 20 rows
</pre>
<br>



<h2>Cassandra Data Modelling</h2>
Some basics:
- Cassandra is a NoSQL distributed database so we cannot use the table joins that are in the relational Oracle HR database schema. 
- We need to de-normalise the HR schema, removing foreign keys and look-up tables - these are relational concepts that don't exist in the distributed world.

For this exercise I will focus on the EMPLOYEES, JOBS and DEPARTMENTS tables.

- Cassandra data modelling is a query-driven process - you first decide the queries that you wish to run, then build your data model around those queries. 
- This is how Cassandra can ensure that your queries with scale with the cluster (this is usually the opposite of the relational world where you start with your data, decide what queries you want to run against it, then index it to the eyeballs to support those queries).

Remember that in a relational database maintaining those indexes is very expensive operation that becomes more expensive as the volume of data grows.

- In the Cassandra world we start with the queries and then design the data model to support those queries. 
- We want to pull the source data from Oracle and move it to the tables in Cassandra. 
- We will perform data transformations on the in-flight data in Spark and SparkSQL to achieve this.

Our queries are:
- Query all employees on EMPLOYEE_ID
- Query all departments, optionally returning employees by department
- Query all jobs, optionally returning  employees by job
- Query all managers, optionally returning  employees by manager

In Cassandra we will create the following tables:
- A table for employees, similar to HR.EMPLOYEES but without the foreign keys to JOB_ID, MANAGER_ID and DEPARTMENT_ID. 
- We will replace the HR.DEPARTMENTS lookup table - we will use EMPLOYEES_BY_DEPARTMENT with a PK on DEPARTMENT_ID, clustered on EMPLOYEE_ID
- We will replace the HR.JOBS lookup table - we will use EMPLOYEES_BY_JOB with a PK on JOB_ID, clustered on EMPLOYEE_ID
- We will replace the FK on MANAGER_ID in the EMPLOYEES table - instead we will use an EMPLOYEES_BY_MANAGER table with a PK on MANAGER, clustered on EMPLOYEE_ID

<h3>Create HR KeySpace In Cassandra</h3>
First thing we need to do in Cassandra is create a keyspace to contain the tables that we will create:
<pre>
CREATE KEYSPACE IF NOT EXISTS HR WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
USE HR;
</pre>
This table will store information on employees, and for each employee there is a one to many relationship for the departments that employee manages. We could use a clustering column for the department(s) managed by that employee.

<h3>Create Employees Table</h3>
For each employee their first name, last name, email address etc is static data - so we can define them as static columns that belong to the partition key, not the clustering columns.

<pre>
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

So we'll move the manager data out of the EMPLOYEES table and use the EMPLOYEES table purely for employee personal data - and put the department manager details in another table altogether.


<h3>Query 1: Query All Employees on EMPLOYEE_ID</h3>
This table satisfies query 1:
<pre lang="sql">
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
 PRIMARY KEY (EMPLOYEE_ID));
</pre>


If you try to save to Cassandra now using the Spark-Cassandra connector it will fail with an error message saying that columns don't exist e.g. "EMPLOYEE_ID", "FIRST_NAME". This is because the connector expects the column case to match in the dataframe and in the Cassandra table. In cassandra theyre in lower case so the dataframe must match.
Here's our schema again:

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
We can create a list of column names in lower case matching the dataframe order, creating a new dataframe "emps_lc" in the process.
We rename the columns in the dataframe like this:

<pre lang="scala">
scala> val newNames = Seq("employee_id", "first_name", "last_name", "email","phone_number","hire_date","job_Id","salary","commission_pct","manager_id","department_id")

newNames: Seq[String] = List(employee_id, first_name, last_name, email, phone_number, hire_date, job_Id, salary, commission_pct, manager_id, department_id)

scala> val emps_lc = employees.toDF(newNames: _*)

emps_lc: org.apache.spark.sql.DataFrame = [employee_id: decimal(6,0), first_name: string, last_name: string, email: string, phone_number: string, hire_date: timestamp, job_Id: string, salary: decimal(8,2), commission_pct: decimal(2,2), manager_id: decimal(6,0), department_id: decimal(4,0)]
</pre>

The schema in our new dataframe is in lower case:
<pre lang="scala">
scala> emps_lc.printSchema()
root
 |-- employee_id: decimal(6,0) (nullable = false)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = false)
 |-- email: string (nullable = false)
 |-- phone_number: string (nullable = true)
 |-- hire_date: timestamp (nullable = false)
 |-- job_Id: string (nullable = false)
 |-- salary: decimal(8,2) (nullable = true)
 |-- commission_pct: decimal(2,2) (nullable = true)
 |-- manager_id: decimal(6,0) (nullable = true)
 |-- department_id: decimal(4,0) (nullable = true)
</pre>


There are some columns in the dataframe that we don't need for this step. We simply create a new dataframe containing the columns that we do want to use.

We should still have the SparkSQL table empTable that we defined earlier based on the employees dataframe. We'll select the columns that we want for the Cassandra table into a new dataframe - you can call dataframes what you want:

<pre lang="scala">
scala> val emps_lc_subset = sqlContext.sql("SELECT employee_id, first_name, last_name, email, phone_number, hire_date, salary, commission_pct FROM empTable")

emps_lc_subset: org.apache.spark.sql.DataFrame = [employee_id: decimal(6,0), first_name: string, last_name: string, email: string, phone_number: string, hire_date: timestamp, salary: decimal(8,2), commission_pct: decimal(2,2)]

</pre>

And we have the schema we're looking for to match the Cassandra target table.
<pre lang="scala">
scala> emps_lc_subset.printSchema()
root
 |-- employee_id: decimal(6,0) (nullable = false)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = false)
 |-- email: string (nullable = false)
 |-- phone_number: string (nullable = true)
 |-- hire_date: timestamp (nullable = false)
 |-- job_id: string (nullable = false)
 |-- salary: decimal(8,2) (nullable = true)
 |-- commission_pct: decimal(2,2) (nullable = true)
</pre>
<h3>Write the dataframe to Cassandra</h3>
<pre lang="scala">
scala> emps_lc_subset.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "employees", "keyspace" -> "hr")).save()
</pre>

And in cqlsh the records are there:
<pre>
cqlsh:hr> select * from employees;

 employee_id | commission_pct | email    | first_name  | hire_date                | last_name   | phone_number       | salary
-------------+----------------+----------+-------------+--------------------------+-------------+--------------------+----------
         111 |           null | ISCIARRA |      Ismael | 2005-09-30 00:00:00-0400 |     Sciarra |       515.124.4369 |  7700.00
         163 |           0.15 |  DGREENE |    Danielle | 2007-03-19 00:00:00-0400 |      Greene | 011.44.1346.229268 |  9500.00
         148 |           0.30 | GCAMBRAU |      Gerald | 2007-10-15 00:00:00-0400 |   Cambrault | 011.44.1344.619268 | 11000.00
         197 |           null |  KFEENEY |       Kevin | 2006-05-23 00:00:00-0400 |      Feeney |       650.507.9822 |  3000.00
         102 |           null |  LDEHAAN |         Lex | 2001-01-13 00:00:00-0500 |     De Haan |       515.123.4569 | 17000.00
         153 |           0.20 |   COLSEN | Christopher | 2006-03-30 00:00:00-0500 |       Olsen | 011.44.1344.498718 |  8000.00
         129 |           null |  LBISSOT |       Laura | 2005-08-20 00:00:00-0400 |      Bissot |       650.124.5234 |  3300.00
         160 |           0.30 |   LDORAN |      Louise | 2005-12-15 00:00:00-0500 |       Doran | 011.44.1345.629268 |  7500.00
         107 |           null | DLORENTZ |       Diana | 2007-02-07 00:00:00-0500 |     Lorentz |       590.423.5567 |  4200.00
         136 |           null | HPHILTAN |       Hazel | 2008-02-06 00:00:00-0500 |  Philtanker |       650.127.1634 |  2200.00
         188 |           null |   KCHUNG |       Kelly | 2005-06-14 00:00:00-0400 |       Chung |       650.505.1876 |  3800.00
         134 |           null |  MROGERS |     Michael | 2006-08-26 00:00:00-0400 |      Rogers |       650.127.1834 |  2900.00
         181 |           null |  JFLEAUR |        Jean | 2006-02-23 00:00:00-0500 |      Fleaur |       650.507.9877 |  3100.00
</pre>


<h3>Query 2: Query All Departments And Employees by Department</h3>

We have a table for employees stored by department - EMPLOYEE_ID is the clustering column.

<pre>
DROP TABLE IF EXISTS EMPLOYEES_BY_DEPT;
CREATE TABLE employees_by_dept (
 DEPARTMENT_ID            	bigint,
 DEPARTMENT_NAME        text static,
 EMPLOYEE_ID            bigint,
 FIRST_NAME             text ,
 LAST_NAME              text,
 PRIMARY KEY (DEPARTMENT_ID, EMPLOYEE_ID));
</pre>


For each department we store the department id, the name of the department, and then successive clustered columns of employees in that department.

Similar to the example previously, we select employees by department from our SparkSQL tables:

<pre lang="scala">
scala> val emp_by_dept = sqlContext.sql("SELECT d.department_id, d.department_name, e.employee_id, e.first_name, e.last_name FROM empTable e, deptTable d where e.department_id=d.department_id")
</pre>
Response:
<pre>
emp_by_dept: org.apache.spark.sql.DataFrame = [department_id: decimal(4,0), department_name: string, employee_id: decimal(6,0), first_name: string, last_name: string]
</pre>

Here's the schema for the resulting dataframe:
<pre lang="scala">
scala> emp_by_dept.printSchema()
root
 |-- department_id: decimal(4,0) (nullable = false)
 |-- department_name: string (nullable = false)
 |-- employee_id: decimal(6,0) (nullable = false)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = false)
</pre>

The data is in the format that we want it for Cassandra:
<pre lang="scala">
scala> emp_by_dept.show()
+-------------+---------------+-----------+----------+-----------+              
|department_id|department_name|employee_id|first_name|  last_name|
+-------------+---------------+-----------+----------+-----------+
|           40|Human Resources|        203|     Susan|     Mavris|
|           50|       Shipping|        120|   Matthew|      Weiss|
|           50|       Shipping|        121|      Adam|      Fripp|
|           50|       Shipping|        122|     Payam|   Kaufling|
|           50|       Shipping|        123|    Shanta|    Vollman|
|           50|       Shipping|        124|     Kevin|    Mourgos|
|           50|       Shipping|        125|     Julia|      Nayer|
|           50|       Shipping|        126|     Irene|Mikkilineni|
|           50|       Shipping|        127|     James|     Landry|
|           50|       Shipping|        128|    Steven|     Markle|
|           50|       Shipping|        129|     Laura|     Bissot|
|           50|       Shipping|        130|     Mozhe|   Atkinson|
|           50|       Shipping|        131|     James|     Marlow|
|           50|       Shipping|        132|        TJ|      Olson|
|           50|       Shipping|        133|     Jason|     Mallin|
|           50|       Shipping|        134|   Michael|     Rogers|
|           50|       Shipping|        135|        Ki|        Gee|
|           50|       Shipping|        136|     Hazel| Philtanker|
|           50|       Shipping|        137|    Renske|     Ladwig|
|           50|       Shipping|        138|   Stephen|     Stiles|
+-------------+---------------+-----------+----------+-----------+
only showing top 20 rows
</pre>

<h3>Write the dataframe to Cassandra</h3>
<pre lang="scala">
scala> emp_by_dept.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "employees_by_dept", "keyspace" -> "hr")).save()
</pre>
And the records are there in Cassandra:
<pre>
cqlsh:hr> select * from employees_by_dept;           

 department_id | employee_id | department_name | first_name  | last_name
---------------+-------------+-----------------+-------------+-------------
            30 |         114 |      Purchasing |         Den |    Raphaely
            30 |         115 |      Purchasing |   Alexander |        Khoo
            30 |         116 |      Purchasing |      Shelli |       Baida
            30 |         117 |      Purchasing |       Sigal |      Tobias
            30 |         118 |      Purchasing |         Guy |      Himuro
            30 |         119 |      Purchasing |       Karen |  Colmenares
            20 |         201 |       Marketing |     Michael |   Hartstein
            20 |         202 |       Marketing |         Pat |         Fay
            80 |         145 |           Sales |        John |     Russell
            80 |         146 |           Sales |       Karen |    Partners
</pre>
Now we're able to retrieve data about departments:
<pre>
cqlsh:hr> select distinct department_id, department_name from employees_by_dept;
 department_id | department_name
---------------+------------------
            30 |       Purchasing
            20 |        Marketing
            80 |            Sales
            60 |               IT
           110 |       Accounting
            50 |         Shipping
            10 |   Administration
           100 |          Finance
            40 |  Human Resources
            70 | Public Relations
            90 |        Executive
</pre>
And the second part of the query requirement was to be able to return employees by department:
<pre>
cqlsh:hr> select department_name, first_name, last_name from employees_by_dept where department_id=50;

 department_name | first_name | last_name
-----------------+------------+-------------
        Shipping |    Matthew |       Weiss
        Shipping |       Adam |       Fripp
        Shipping |      Payam |    Kaufling
        Shipping |     Shanta |     Vollman
        Shipping |      Kevin |     Mourgos
        Shipping |      Julia |       Nayer
        Shipping |      Irene | Mikkilineni
        Shipping |      James |      Landry
        Shipping |     Steven |      Markle
        Shipping |      Laura |      Bissot
        Shipping |      Mozhe |    Atkinson
        Shipping |      James |      Marlow

</pre>




<h3>Query 3: Query All Jobs, And Employees By Job</h3>
The second part of this requirement was to be able to optionally return Employees by Job</h3>
With the techniques described above you should now be able to have a go at doing the same thing yourself with the Jobs and Employees tables.

<h3>Query 4: Query All Managers, And Employees By Manager</h3>
If you're a real over-achiever why not have a go at using the Manager column in the Employees table :)
The second part of this requirement was to be able to optionally return employees by manager.


