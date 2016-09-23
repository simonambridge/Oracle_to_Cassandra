# Migrating Relational Data From Oracle To DSE/Cassandra - using Spark DataFrames, SparkSQL and the spark-cassandra connector
The objective of this exercise is to demonstrate how to migrate data from Oracle to DataStax Cassandra. I'll be using the DataFrame capability introduced in Apache Spark 1.3 to load data from tables in an Oracle database (12c) via Oracle's JDBC thin driver, to generate a result set, joining tables where necessary.
The data will then be saved to DSE/Cassandra.

This demo is not intended as an exhaustive or comprehensive tutorial but it should provide enough material to gain an understanding of the processes involved in migrating data from Oracle to Cassandra
 
<h2>Pre-requisites</h2>
<h3> DataStax Enterprise</h3>
You'll need a working installation of DataStax Enterprise. I've used release 5.0.1  here, release 5.0.2 latest available at time of publication.

- Ubuntu/Debian - https://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/install/installDEBdse.html
- Red Hat/Fedora/CentOS/Oracle Linux - https://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/install/installRHELdse.html

To setup your environment, you'll also need the following resources:

- Python 2.7
- Java 8
- For Red Hat, CentOS and Fedora, install EPEL (Extra Packages for Enterprise Linux).
- An Oracle database:
 - In my example the database name is orcl
 - A running Oracle tns listener. In my example I'm using the default port of 1521.
 - You are able to make a tns connection to the Oracle database e.g. <b>"sqlplus user/password@service_name"</b>.
- The Oracle thin JDBC driver. You can download the ojdbc JAR file from:
http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html
I've used ojdbc7.jar which is certified for use with both JDK7 and JDK8
In my 12c Oracle VM Firefox this downloaded to /app/oracle/downloads/ so you'll see the path referenced in the instructions below.


As we will connect from Spark, using the Oracle jdbc driver, to the "orcl" database on TNS port 1521, and then to Cassandra, all these components must be working correctly.
<br>

Now on to installing DataStax Enterprise and playing with some data!
<p>
<H1>Set Up DataStax Components</H1>
<br>
Installation instructions for DSE are provided at the top of this doc. I'll show the instructions for Red Hat/CentOS/Fedora here. I'm using an Oracle Enterprise Linux VirtualBox instance.
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

<h3>DataStax OpsCenter - I got 6.0.2.1</h3>
<pre>
# yum install opscenter
</pre>

<h3>DataStax OpsCenter Agent - I got 6.0.2.1</h3>
<pre>
# yum install datastax-agent
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

After a few minutes use nodetool to check that all is up and running (check for "UN" next to the IP address):
<pre>
$ nodetool status
Datacenter: SearchAnalytics
===========================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load       Owns    Host ID                               Token                                    Rack
UN  127.0.0.1  346.89 KB  ?       8e6fa3db-9018-47f0-96df-8c78067fddaa  6840808785095143619                      rack1

Note: Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless
</pre>
You should also check that you can log into cqlsh:
<pre>
$ cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.0.7.1159 | DSE 5.0.1 | CQL spec 3.4.0 | Native protocol v4]
Use HELP for help.
cqlsh> 
</pre>
Type exist in cqlsh to return to the shell prompt.
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

The HR user has a simple schema consisting of 7 tables. The data is stored in a relational format, using foreign keys to look up into tables e.g. To find which department an employee belongs to, you use the value in the DEPARTMENT_ID column to look up a record in the DEPARTMENTS table to identify the department the employee belongs to. 
You can walk through the model using the diagram below:
 
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
Foreign keys on JOB_ID, DEPARTMENT_ID and MANAGER_ID.

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
Foreign keys on MANAGER_ID and LOCATION_ID.
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
Foreign key on COUNTRY_ID.
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
Foreign key on REGION_ID.
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

Let's walk through the schema to get familiar with the data that we're going to migrate.
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
So all is looking good on the Oracle side. Now time to turn to DSE/Cassandra and Spark.

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

Next, load the data from the Oracle table using JDBC.<p>
For Spark versions below 1.4:
<pre lang="scala">
scala> val employees = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "employees"))
</pre>
For Spark 1.4 onwards:
<pre lang="scala">
scala> val employees = sqlContext.read.format("jdbc").option("url", "jdbc:oracle:thin:hr/hr@localhost:1521/orcl").option("driver", "oracle.jdbc.OracleDriver").option("dbtable", "employees").load()
</pre>

The Spark REPL responds with:
<pre>
employees: org.apache.spark.sql.DataFrame = [EMPLOYEE_ID: decimal(6,0), FIRST_NAME: string, LAST_NAME: string, EMAIL: string, PHONE_NUMBER: string, HIRE_DATE: timestamp, JOB_ID: string, SALARY: decimal(8,2), COMMISSION_PCT: decimal(2,2), MANAGER_ID: decimal(6,0), DEPARTMENT_ID: decimal(4,0)]
</pre>
All good so far.

<h2>Examining The DataFrame</h2>
Now that we've created a dataframe using the jdbc method shown above, we can use the dataframe method printSchema() to look at the dataframe schema. 
You'll notice that it looks a lot like a table. That's great because it means that we can use it to manipulate large volumes of tabular data:

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

We can use the dataframe .show() method to display the first x:int rows in the table:

<pre lang="scala">
scala> employees.show(5)
+-----------+-----------+----------+--------+------------+--------------------+----------+--------+--------------+----------+-------------+
|EMPLOYEE_ID| FIRST_NAME| LAST_NAME|   EMAIL|PHONE_NUMBER|           HIRE_DATE|    JOB_ID|  SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+-----------+----------+--------+------------+--------------------+----------+--------+--------------+----------+-------------+
|        100|     Steven|      King|   SKING|515.123.4567|2003-06-16 23:00:...|   AD_PRES|24000.00|          null|      null|           90|
|        101|      Neena|   Kochhar|NKOCHHAR|515.123.4568|2005-09-20 23:00:...|     AD_VP|17000.00|          null|       100|           90|
|        102|        Lex|   De Haan| LDEHAAN|515.123.4569|2001-01-13 00:00:...|     AD_VP|17000.00|          null|       100|           90|
|        103|  Alexander|    Hunold| AHUNOLD|590.423.4567|2006-01-03 00:00:...|   IT_PROG| 9000.00|          null|       102|           60|
|        104|      Bruce|     Ernst|  BERNST|590.423.4568|2007-05-20 23:00:...|   IT_PROG| 6000.00|          null|       103|           60|
+-----------+-----------+----------+--------+------------+--------------------+----------+--------+--------------+----------+-------------+
only showing top 5 rows
</pre>

Let's load some data from the Oracle table Departments:
For Spark versions below 1.4:
<pre lang="scala">
scala> val departments = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "departments"))
</pre>
</pre>
For Spark 1.4 onwards:
<pre lang="scala">
scala> val departments = sqlContext.read.format("jdbc").option("url", "jdbc:oracle:thin:hr/hr@localhost:1521/orcl").option("driver", "oracle.jdbc.OracleDriver").option("dbtable", "departments").load()
</pre>

REPL responds:
<pre lang="scala">
departments: org.apache.spark.sql.DataFrame = [DEPARTMENT_ID: decimal(4,0), DEPARTMENT_NAME: string, MANAGER_ID: decimal(6,0), LOCATION_ID: decimal(4,0)]
</pre>

There are some options available that allow you to tune how Spark uses the JDBC driver. The JDBC datasource supports partitionning so that you can specify how Spark will parallelize the load operation from the JDBC source. By default a JDBC load will be sequential which is much less efficient where multiple workers are available.

The options describe how to partition the table when reading in parallel from multiple workers:
<ul>
<li>partitionColumn</li>
<li>lowerBound</li>
<li>upperBound</li>
<li>numPartitions.	</li>
</ul>

These options must all be specified if any of them is specified. 
<ul>
<li>partitionColumn must be a numeric column from the table in question used to partition the table.</li>

<li>Notice that lowerBound and upperBound are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned.</li>
<li>fetchSize	is the JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows).</li>
</ul>
<br>
So this might be an alternative load command for the departments table using some of those options:

<pre lang="scala">
scala> val departments = sqlContext.read.format("jdbc")
                  .option("url", "jdbc:oracle:thin:hr/hr@localhost:1521/orcl")
                  .option("driver", "oracle.jdbc.OracleDriver")
                  .option("dbtable", "departments")
                  .option("partitionColumn", "DEPARTMENT_ID")
                  .option("lowerBound", "1")
                  .option("upperBound", "100000000")
                  .option("numPartitions", "4")
                  .option("fetchsize","1000")
                  .load()
</pre>

> Don’t create too many partitions in parallel on a large cluster, otherwise Spark might crash the external database.

You should experiment with these options when you load your data on your infrastructure.
You can read more about this here: http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases

View the Departments schema:
<pre lang="scala">
scala> departments.printSchema()
root
 |-- DEPARTMENT_ID: decimal(4,0) (nullable = false)
 |-- DEPARTMENT_NAME: string (nullable = false)
 |-- MANAGER_ID: decimal(6,0) (nullable = true)
 |-- LOCATION_ID: decimal(4,0) (nullable = true)
</pre>

At this point the JDBC statement has been validated but Spark hasn't yet checked the physical data (e.g. if you provide an invalid partitioning column you won't get an error message until you try to read the data).

Now read records from the Departments DataFrame:

<pre lang="scala">
scala> departments.show(5)
+-------------+--------------------+----------+-----------+
|DEPARTMENT_ID|     DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-------------+--------------------+----------+-----------+
|           10|      Administration|       200|       1700|
|           20|           Marketing|       201|       1800|
|           30|          Purchasing|       114|       1700|
|           40|     Human Resources|       203|       2400|
|           50|            Shipping|       121|       1500|
+-------------+--------------------+----------+-----------+
only showing top 5 rows
</pre>
What's the total number of records in the departments dataframe:
<pre lang="scala">
scala> departments.count()
res1: Long = 27           
</pre>
<br>

<h2>Cassandra Data Modelling</h2>
Some basics:
<ul>
  <li>
    DSE/Cassandra is a NoSQL distributed database so we cannot use the table joins that are in the relational Oracle HR database schema.
  </li>
  <li>
    We need to de-normalise the HR schema, removing foreign keys and look-up tables - these are relational concepts that don't exist in the distributed world.
  </li>
</ul>

Cassandra is a NoSQL distributed database so we cannot use the table joins that are in the relational Oracle HR database schema. 
<ul>
  <li>
    We need to de-normalise the HR schema, removing foreign keys and look-up tables - these are relational concepts that don't exist in the distributed world.
  </li>
</ul>

For this demonstration I will focus on the EMPLOYEES and DEPARTMENTS tables.

<ul>
  <li>
    Cassandra data modelling is a query-driven process - you first decide the queries that you wish to run, then build your data model around those queries. 
  </li>
  <li>
    This is how Cassandra can ensure that your queries scale with the cluster (this is usually the opposite of the relational world where you start with your data, decide what queries you want to run against it, then index it to the eyeballs to support those queries).
  </li>
</ul>

Remember that in a relational database maintaining those indexes is very expensive operation that becomes more expensive as the volume of data grows.

<ul>
  <li>
    In the Cassandra world we start with the queries and then design the data model to support those queries. 
  </li>
  <li>
    We want to pull the source data from Oracle and move it to the tables in Cassandra. 
  </li>
  <li>
    We will perform data transformations on the in-flight data in Spark and SparkSQL to achieve this.
  </li>
</ul>

Our queries are:
<ul>
  <li>
    Query1: query all employees on EMPLOYEE_ID
  </li>
  <li>
    Query2: query all departments on DEPARTMENT_ID, and query Employees by Department
  </li>
</ul>

In Cassandra we will create the following tables:
<ul>
  <li>
    A table for employees, similar to HR.EMPLOYEES but without the foreign keys to JOB_ID, MANAGER_ID and DEPARTMENT_ID. 
  </li>
  <li>
    We will replace the HR.DEPARTMENTS lookup table - we will use EMPLOYEES_BY_DEPARTMENT with a PK on DEPARTMENT_ID, clustered on EMPLOYEE_ID
  </li>
</ul>

<h2>Create HR KeySpace In Cassandra</h2>
The first thing that we need to do in Cassandra is create a keyspace to contain the tables that we will create. I'm using a replication factor of 1 because I have one node in my development cluster. For most production deployments we recommend a multi-datacenter Active-Active HA setup across geographical regions using NetworkTopologyStrategy with RF=3:

Log into cqlsh 
> If you didn't change the IP defaults in cassandra.yaml then just type 'cqlsh' - if you changed the IP to be the host IP then you may need to supply the hostname e.g. 'cqlsh <hostname>'.

From the cqlsh prompt, create the keyspace to hold our Cassandra HR tables:
<pre>
CREATE KEYSPACE IF NOT EXISTS HR WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
USE HR;
</pre>

<h2>Create Employees Table</h2>

De-normalising and potentially duplicating data in Cassandra is not a bad thing. It's quicker than updating indexes, and disk is cheap, and Cassandra writes are fast!

So for Query 1 we'll also move the Department, Manager and Job data out of the EMPLOYEES table and use the EMPLOYEES table purely for employee personal data. We get the information that we've moved by creating tables specifically to support those queries. See Query 2 as an example of getting data about departments and employees.
For Query 2 we'll use EMPLOYEES_BY_DEPARTMENT to query Departments and Employees by Department.

<h2>Query 1: Query All Employees on EMPLOYEE_ID</h2>
Create the table we use to hold employees in Cassandra:
<pre lang="sql">
DROP TABLE IF EXISTS employees;

CREATE TABLE employees (
 employee_id            	bigint,
 first_name             	text ,
 last_name              	text,
 email                  	text,
 phone_number           	text,
 hire_date              	text,
 salary                 	decimal,
 commission_pct         	decimal,
 PRIMARY KEY (employee_id));
</pre>
This table satisfies query 1 we can now do an extremely fast retrieval using a partition key to access our data:
<pre lang="sql">
cqlsh:hr> select * from employees where employee_id=188;

 employee_id | commission_pct | email  | first_name | hire_date                | last_name | phone_number | salary
-------------+----------------+--------+------------+--------------------------+-----------+--------------+---------
         188 |           null | KCHUNG |      Kelly | 2005-06-14 00:00:00-0400 |     Chung | 650.505.1876 | 3800.00
</pre>

<h3>Change Dataframe Colun Names To Lower Case</h3>
At this point go back to the Spark shell/REPL for the following steps.

If you try to save your employee dataframe to Cassandra right now (using the Spark-Cassandra connector) it will fail with an error message saying that columns don't exist e.g. "EMPLOYEE_ID", "FIRST_NAME" - even though they're there. This is because the connector expects the column case in the dataframe to match the column case in the Cassandra table. In Cassandra column names are always in lower case, so the dataframe names must match.<p>
So we need to modify our dataframe schema....
Here's a reminder of our raw employees dataframe schema again:

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

We want to change those column names to lower case - so we create a list of column names in lower case matching the dataframe order, creating a new dataframe "emps_lc" in the process.
We rename the columns in the dataframe like this:

<pre lang="scala">
scala> val newNames = Seq("employee_id", "first_name", "last_name", "email","phone_number","hire_date","job_Id","salary","commission_pct","manager_id","department_id")

newNames: Seq[String] = List(employee_id, first_name, last_name, email, phone_number, hire_date, job_Id, salary, commission_pct, manager_id, department_id)

scala> val emps_lc = employees.toDF(newNames: _*)

emps_lc: org.apache.spark.sql.DataFrame = [employee_id: decimal(6,0), first_name: string, last_name: string, email: string, phone_number: string, hire_date: timestamp, job_Id: string, salary: decimal(8,2), commission_pct: decimal(2,2), manager_id: decimal(6,0), department_id: decimal(4,0)]
</pre>

The schema in our new dataframe is in lower case - Yay!
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


There are some columns in the dataframe that we don't need for this step. We simply create a new dataframe containing just the columns that we do want to use.

<h3>Create SparkSQL Tables From The DataFrames</h3>
We can manipulate the data in Spark in two ways. We can use the dataframe methods which allow for querying and filtering of data. Or we can use SparkSQL. To access data using SparkSQL we need to register a dataframe as a temporary table against which we can run relational SQL queries. 
As an example, let's join the employees and departments tables:
<pre lang="scala">
scala> employees.registerTempTable("empTable")

scala> departments.registerTempTable("deptTable")
</pre>

<br>
We'll select the columns that we want for the Cassandra table into a new dataframe called "emps_lc_subset":

<pre lang="scala">
scala> val emps_lc_subset = sqlContext.sql("SELECT employee_id, first_name, last_name, email, phone_number, hire_date, salary, commission_pct FROM empTable")

emps_lc_subset: org.apache.spark.sql.DataFrame = [employee_id: decimal(6,0), first_name: string, last_name: string, email: string, phone_number: string, hire_date: timestamp, salary: decimal(8,2), commission_pct: decimal(2,2)]

</pre>

And we now have the schema we're looking for to match the Cassandra target table.
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

<h3>Write The Employees Dataframe To Cassandra</h3>
Now we can save the data to a Cassandra table using the Spark-Cassandra connector (truncate the table if it isn't empty):
<pre lang="scala">
scala> emps_lc_subset.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "employees", "keyspace" -> "hr")).save()
</pre>

And if we hop over to cqlsh we can see the records are there:
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


<h2>Query 2: Query All Departments And Employees by Department</h2>

<H3>Multi-Table Joins In SparkSQL</h3>
when we migrate data from a relational database to a NoSQL database like Apache Cassandra there is invariably a need to perform some element of data transformation. Transformation typically involves duplication and de-normalisation of data and to do this we frequently want to join data between tables. Let's see how we can do that in Spark.

<H3>Create Cassandra Table EXISTS EMPLOYEES_BY_DEPT</h3>
To satisfy Query 2 we have a table for Employees stored by Department.
DEPARTMENT_ID is the partitioning key, EMPLOYEE_ID is the clustering column. Wel will need to join the Employees and Departments tables to satisfy this query.

<pre lang="sql">
DROP TABLE IF EXISTS EMPLOYEES_BY_DEPT;
CREATE TABLE employees_by_dept (
 DEPARTMENT_ID            	bigint,
 DEPARTMENT_NAME        text static,
 EMPLOYEE_ID            bigint,
 FIRST_NAME             text ,
 LAST_NAME              text,
 PRIMARY KEY (DEPARTMENT_ID, EMPLOYEE_ID));
</pre>


> For each department we store the department id and the name of the department (as a partioning key and a static column for that partitioning key), and then for each department partition key there will be successive clustered columns of employees in that department.

We select employees-by-department by joining our SparkSQL tables on DEPARTMENT_ID.

In the Spark REPL:
As an example, let's join the employees and departments tables.
If you haven't already registered the Spark SQL tables do it like this:
<pre lang="scala">
scala> employees.registerTempTable("empTable")

scala> departments.registerTempTable("deptTable")
</pre>
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

<h3>Write the Employees_By_Department dataframe to Cassandra</h3>
Now save the employees_by_dept data to Cassandra using the Spark-Cassandra connector:
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
Query 3 asks for a list of all job titles and descriptions. The second part of this query requirement was to be able to return Employees by Job</h3>
With the techniques described above you should now be able to have a go at doing the same thing yourself with the Jobs and Employees tables.
(hint: replace the HR.JOBS lookup table - use EMPLOYEES_BY_JOB with a PK on JOB_ID, clustered on EMPLOYEE_ID)

<h3>Query 4: Query All Managers, And Employees By Manager</h3>
If you're a real over-achiever why not have a go at using the Manager column in the Employees table :)
We need to be able to query all Managers. The second part of this query requirement was to be able to return Employees by Manager.
(hint: replace the old FK on MANAGER_ID in the EMPLOYEES table - use an EMPLOYEES_BY_MANAGER table with a PK on MANAGER, clustered on EMPLOYEE_ID)

