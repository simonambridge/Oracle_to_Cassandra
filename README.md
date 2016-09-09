# Oracle_to_Cassandra

<H1>WORK IN PROGRESS - come back later</H1>

add gateway in /etc/sysconfig/network
eth8 in /etc/sysconfig/network-scripts


DEVICE=eth8
ONBOOT=yes
TYPE=Ethernet
BOOTPROTO=dhcp
NETMASK=255.255.255.0
GATEWAY=171.28.136.254
PERRDNS=yes
PEERROUTES=yes

service network restart

Add /etc/yum.repos.d/datastax.repo
[datastax] 
name = DataStax Repo for DataStax Enterprise
baseurl=https://datastaxrepo_gmail.com:utJVKEg4lKeaWTX@rpm.datastax.com/enterprise
enabled=1
gpgcheck=0

rpm --import http://rpm.datastax.com/rpm/repo_key 


sudo yum install dse-full-5.0.1-1
sudo yum install opscenter --> 6.0.2.1
sudo yum install datastax-agent --> 6.0.2.1


install guest additions
restart
killall VBoxClient
VBoxClient-all

cqlsh
"No appropriate python interpreter found"

http://tecadmin.net/install-python-2-7-on-centos-rhel/#
yum install gcc
cd /usr/src
wget https://www.python.org/ftp/python/2.7.12/Python-2.7.12.tgz
tar xzf Python-2.7.12.tgz 
cd Python-2.7.12
./configure
make altinstall

# python2.7 --version
Python 2.7.12
("python --version" still brings up 2.6.6)

cqlsh now works




# cqlsh
Warning: Timezone defined and 'pytz' module for timezone conversion not installed. Timestamps will be displayed in UTC timezone.

Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.0.7.1159 | DSE 5.0.1 | CQL spec 3.4.0 | Native protocol v4]
Use HELP for help.
cqlsh> 

http://sharadchhetri.com/2014/05/30/install-pip-centos-rhel-ubuntu-debian/
yum install wget


# cat /etc/redhat-release 
Red Hat Enterprise Linux Server release 6.8 (Santiago)

# rpm -ivh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
Retrieving http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
warning: /var/tmp/rpm-tmp.X7MF42: Header V3 RSA/SHA256 Signature, key ID 0608b895: NOKEY
Preparing...                ########################################### [100%]
   1:epel-release           ########################################### [100%]

# rpm -qa | grep epel
epel-release-6-8.noarch

To remove v7 incorrectly applied:
rpm -e epel_release
rm -rf /var/cache/yum/x86_64/6Server/epel


Creates
1. /etc/yum.repos.d/epel.repo
2. /etc/yum.repos.d/epel-testing.repo

yum install -y python-pip

wget --no-check-certificate https://bootstrap.pypa.io/ez_setup.py
sudo /usr/local/bin/python2.7 ez_setup.py
sudo /usr/local/bin/easy_install-2.7 pip
sudo /usr/local/bin/easy_install-2.7 pitz (by accident!)

# which pip
/usr/local/bin/pip
# which easy_install
/usr/local/bin/easy_install

# sudo /usr/local/bin/easy_install-2.7 pytz

Now no error message for pytz:

# cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.0.7.1159 | DSE 5.0.1 | CQL spec 3.4.0 | Native protocol v4]
Use HELP for help.
cqlsh> 

Shutdown DSE & enable Analytics

#  service dse stop
Stopping DSE daemon : dse                                  [  OK  ]
# vi /etc/default/dse
# rm -rf /var/lib/cassandra/data/*
# rm /var/log/cassandra/system.log 
rm: remove regular file `/var/log/cassandra/system.log'? y

Cassandra account created:
# cat /etc/passwd | grep cassandra
cassandra:x:476:472::/var/lib/cassandra:/bin/bash

Ulimits updated for the Cassandra user:
# cat /etc/security/limits.d/cassandra.conf

cassandra - memlock unlimited
cassandra - nofile 100000
cassandra - nproc 32768
cassandra - as unlimited

sudo su - cassandra
sudo service dse start
cqlsh - OK

$ dse client-tool spark master-address
spark://127.0.0.1:7077
 

??pip install cassandra-driver??




Oracle Database
---------------

$ sqlplus / as sysdba

SQL*Plus: Release 12.1.0.2.0 Production on Mon Sep 5 05:25:42 2016

Copyright (c) 1982, 2014, Oracle.  All rights reserved.

Connected to:
Oracle Database 12c Enterprise Edition Release 12.1.0.2.0 - 64bit Production
With the Partitioning, OLAP, Advanced Analytics and Real Application Testing options


SQL> select database_name from v$database;

DATABASE_NAME
----------------------
ORCL.US.ORACLE.COM

1 row selected.

Allow local User ID's to be created:
SQL> alter session set "_ORACLE_SCRIPT"=true; 

@?/demo/schema/human_resources/hr_main.sql
hr
users
temp
Admin123
$ORACLE_HOME/demo/schema/log/

PL/SQL procedure successfully completed.

Schema
------

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
 
SQL> desc jobs
 Name                    Null?    Type
 ----------------------- -------- ----------------
 JOB_ID                  NOT NULL VARCHAR2(10)
 JOB_TITLE               NOT NULL VARCHAR2(35)
 MIN_SALARY                       NUMBER(6)
 MAX_SALARY                       NUMBER(6)

 

SQL> desc departments
 Name                    Null?    Type
 ----------------------- -------- ----------------
 DEPARTMENT_ID           NOT NULL NUMBER(4)
 DEPARTMENT_NAME         NOT NULL VARCHAR2(30)
 MANAGER_ID                       NUMBER(6)
 LOCATION_ID                       NUMBER(4)

SQL> desc locations
 Name                    Null?    Type
 ----------------------- -------- ----------------
 LOCATION_ID             NOT NULL NUMBER(4)
 STREET_ADDRESS                   VARCHAR2(40)
 POSTAL_CODE                      VARCHAR2(12)
 CITY                    NOT NULL VARCHAR2(30)
 STATE_PROVINCE                   VARCHAR2(25)
 COUNTRY_ID                       CHAR(2)

SQL> desc countries
 Name                    Null?    Type
 ----------------------- -------- ----------------
 COUNTRY_ID              NOT NULL CHAR(2)
 COUNTRY_NAME                     VARCHAR2(40)
 REGION_ID                        NUMBER


We can select employees from the employees table, for example:
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



We want to focus on employees reporting to a manager with ID=121:

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


Who is that manager?

SQL> select * from employees where employee_id=121;

EMPLOYEE_ID FIRST_NAME           LAST_NAME            EMAIL                PHONE_NUMBER         HIRE_DATE JOB_ID         SALARY COMMISSION_PCT MANAGER_ID DEPARTMENT_ID
----------- -------------------- -------------------- -------------------- -------------------- --------- ---------- ---------- -------------- ---------- -------------
        121 Adam                 Fripp                AFRIPP               650.123.2234         10-APR-05 ST_MAN           8200                       100            50

What is HIS job?
SQL> select * from jobs where job_id='ST_MAN';

JOB_ID     JOB_TITLE                           MIN_SALARY MAX_SALARY
---------- ----------------------------------- ---------- ----------
ST_MAN     Stock Manager                             5500       8500


Who is HIS boss?

EMPLOYEE_ID FIRST_NAME           LAST_NAME            EMAIL                PHONE_NUMBER         HIRE_DATE JOB_ID         SALARY COMMISSION_PCT MANAGER_ID DEPARTMENT_ID
----------- -------------------- -------------------- -------------------- -------------------- --------- ---------- ---------- -------------- ---------- -------------
        100 Steven               King                 SKING                515.123.4567         17-JUN-03 AD_PRES         24000                                      90

They work in Department=50 - what is that?

SQL> select * from departments where department_id=50;

DEPARTMENT_ID DEPARTMENT_NAME                MANAGER_ID LOCATION_ID
------------- ------------------------------ ---------- -----------
           50 Shipping                              121        1500

Where is location=1500?

SQL> select * from locations where location_id=1500;

LOCATION_ID STREET_ADDRESS                           POSTAL_CODE  CITY                           STATE_PROVINCE            CO
----------- ---------------------------------------- ------------ ------------------------------ ------------------------- --
       1500 2011 Interiors Blvd                      99236        South San Francisco            California                US

What country is that in?
SQL> select * from countries where country_id='US';

CO COUNTRY_NAME                              REGION_ID
-- ---------------------------------------- ----------
US United States of America                          2

1 row selected.

SQL> select * from regions where region_id=2;

 REGION_ID REGION_NAME
---------- -------------------------
         2 Americas





The objective is to use the DatFrame capability introduced in Apache Spark 1.3 to load data from tables in Oracle database (12c) using Oracle's JDBC thin driver.
 and generate a result set by joining 2 tables.

We have to add the Oracle JDBC jar file to our Spark classpath so that Spark knows how to talk to the Oracle database.

You can download the ojdbc JAR file from:

http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html
I've used ojdbc7.jar which is certified for use with both JDK7 and JDK8

To include this extension lib you can add the line in the “spark-env.sh” file. For this test we only need the driver file on our single (SparkMaster).
In a bigger cluster we would need to distribute it to the slave nodes too.

1. Add jars from submit command line - no good, I want it in the REPL
./bin/spark-submit --class "SparkTest" --master local[*] --jars /fullpath/first.jar,/fullpath/second.jar /fullpath/your-program.jar

2. Add --jars to command line - FAILED, did not work

$ dse spark --jars /app/oracle/downloads/ojdbc7.jar

3. Add to /etc/dse/spark/spark-env.sh - SUCCESS, but deprecation warning messages

 SPARK_CLASSPATH="/app/oracle/downloads/ojdbc7.jar"

[oracle@demo ~]$ dse spark
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
WARN  2016-09-07 15:49:08,722 org.apache.spark.SparkConf: 
SPARK_CLASSPATH was detected (set to '/app/oracle/downloads/ojdbc7.jar').
This is deprecated in Spark 1.0+.

Please instead use:
 - ./spark-submit with --driver-class-path to augment the driver classpath
 - spark.executor.extraClassPath to augment the executor classpath
        
WARN  2016-09-07 15:49:08,728 org.apache.spark.SparkConf: Setting 'spark.executor.extraClassPath' to '/app/oracle/downloads/ojdbc7.jar' as a work-around.
WARN  2016-09-07 15:49:08,728 org.apache.spark.SparkConf: Setting 'spark.driver.extraClassPath' to '/app/oracle/downloads/ojdbc7.jar' as a work-around.
Created spark context..
Spark context available as sc.
Hive context available as sqlContext. Will be initialized on first use.

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._


scala> val employees = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "hr.employees"))
warning: there were 1 deprecation warning(s); re-run with -deprecation for details
employees: org.apache.spark.sql.DataFrame = [EMPLOYEE_ID: decimal(6,0), FIRST_NAME: string, LAST_NAME: string, EMAIL: string, PHONE_NUMBER: string, HIRE_DATE: timestamp, JOB_ID: string, SALARY: decimal(8,2), COMMISSION_PCT: decimal(2,2), MANAGER_ID: decimal(6,0), DEPARTMENT_ID: decimal(4,0)]

SUCCESS - but not ideal as its a deprecated 

3. Use driver_class_path - SUCCESS

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

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

scala> val employees = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:hr/hr@localhost:1521/orcl", "dbtable" -> "hr.employees")) -deprecation
warning: there were 1 deprecation warning(s); re-run with -deprecation for details
employees: org.apache.spark.sql.DataFrame = [EMPLOYEE_ID: decimal(6,0), FIRST_NAME: string, LAST_NAME: string, EMAIL: string, PHONE_NUMBER: string, HIRE_DATE: timestamp, JOB_ID: string, SALARY: decimal(8,2), COMMISSION_PCT: decimal(2,2), MANAGER_ID: decimal(6,0), DEPARTMENT_ID: decimal(4,0)]

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


Cassandra Data Model
--------------------
Cassandra is a NoSQL distributed database so we cannot use the table joins that are in the relational Oracle HR database schema. 
We need to de-normalise the HR schema, removing foreign keys and look up tables - these are relational concepts.

For this exercise I will focus on the EMPLOYEES, JOBS and DEPARTMENTS tables.

Cassandra data modeling is a query-driven process - you first decide the queries that you wish to run, then build your data model around those queries. 
This is how Cassandra can ensure that your queries with scale with the cluster (this is usually the opposite of the relational world where you start with 
your data, decide what queries you want to run against it, then index it to the eyeballs to support those queries).

Remember that in a relational database maintaining those indexes is very expensive operation that becomes more expensive as the volume of data grows.
In contrast

1. Query all employees on EMPLOYEE_ID
2. Query all departments, optionally returning employees by department
3. Query all jobs, optionally returning  employees by job
4. Query all managers, optionally returning  employees by manager

In Cassandra we will create the following tables:
1. 1 table for employees, similar to HR.EMPLOYEES but without the foreign keys to JOB_ID, MANAGER_ID and DEPARTMENT_ID. 
   The original DEPARTMENTS table has a FK MANAGER_ID to the EMPLOYEES table, so we will also add a clustering column MANAGES_DEPT_ID so that we can identify all departments for which an employee is a manager
   We will not make this clustering column part of the partitioning primary key because we may want to search on 
2. We will replace the HR.DEPARTMENTS lookup table - we will use EMPLOYEES_BY_DEPARTMENT with a PK on DEPARTMENT_ID, clustered on EMPLOYEE_ID
3. We will replace the HR.JOBS lookup table - we will use EMPLOYEES_BY_JOB with a PK on JOB_ID, clustered on EMPLOYEE_ID
4. We will replace the FK on MANAGER_ID in the EMPLOYEES table - instead we will use an EMPLOYEES_BY_MANAGER table with a PK on MANAGER, clustered on EMPLOYEE_ID

CREATE KEYSPACE IF NOT EXISTS HR WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
USE HR;

This table will store information on employees, and for each employee there is a clustering column for the department(s) managed by that employee.

If we do it like this then all the columns except the partition key (EMPLOYEE_ID) will be part of the clustered record under MANAGES_DEPT_ID:

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
 MANAGES_DEPT_ID     		bigint,
 MANAGES_DEPT_NAME	        text,
 PRIMARY KEY ((EMPLOYEE_ID), MANAGES_DEPT_ID));

However, for each employee their first name, last name, email address etc is static data - so we can define them as static columns that belong to the partition key

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
 MANAGES_DEPT_ID     		bigint,
 MANAGES_DEPT_NAME	        text,
 PRIMARY KEY ((EMPLOYEE_ID), MANAGES_DEPT_ID));


Alternatively we can move the manager data out of the EMPLOYEES table and use the EMPLOYEES table purely for employee personal data and put the department manager details in another table altogether.

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






test data
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




DROP TABLE IF EXISTS EMPLOYEES_BY_DEPARTMENT;
CREATE TABLE EMPLOYEES_BY_DEPARTMENT (
DEPARTMENT_ID      bigint, 
DEPARTMENT_NAME    text, 
EMPLOYEE_ID        bigint, 
EMPLOYEE_NAME      text, 
PRIMARY KEY ((DEPARTMENT_ID), EMPLOYEE_ID));











