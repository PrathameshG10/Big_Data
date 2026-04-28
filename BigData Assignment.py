	## Assignments

# 1. Find max sal, min sal, avg sal, total sal per dept per job in emp.csv file.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
         .appName("demo01")\
         .getOrCreate()
         
filepath = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/emp.csv"

emp_schema = "empno INT, ename STRING, job STRING, mgr INT, hire DATE, sal DOUBLE, comm DOUBLE, deptno INT"


emp = spark.read\
      .schema(emp_schema)\
      .csv(filepath)
      
emp.printSchema()
emp.show()

result1 = emp.groupBy("deptno","job")\
            .agg(
               max("sal").alias("maximun"),
               min("sal").alias("minimum"),
               avg("sal").alias("Average"),
               sum("sal").alias("Total")
            )
result1.show()

# 2. Find deptwise total sal from emp.csv and dept.csv. Print dname and total sal. Hint: use join()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("demo02") \
        .getOrCreate()

filepath = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/emp.csv"
filepath1 = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/dept.csv"

emp_schema = "empno INT, ename STRING, job STRING, mgr INT, hire DATE, sal DOUBLE, comm DOUBLE, deptno INT"
dept_schema = "deptID INT, dname STRING, location STRING"

emp = spark.read \
        .schema(emp_schema) \
        .csv(filepath)

dept = spark.read \
         .schema(dept_schema) \
         .csv(filepath1)

emp.show()
dept.show()

from pyspark.sql.functions import sum, col

result2 = emp.join(dept, emp.deptno == dept.deptID, "inner") \
            .groupBy("dname") \
            .agg(sum("sal").alias("Total_Salary"))

result2.show()

# 3. Count number of movie ratings per year. Hint: convert time column to TIMESTAMP using from_unixtime().

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("demo03") \
        .getOrCreate()
        
filepath = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/movies/ratings.csv"

movie_schema = "userID INT , movieID INT , rating FLOAT , timestamp BIGINT"

movie= spark.read\
        .option("header" , "true")\
        .schema(movie_schema)\
        .csv(filepath)
        
movie.printSchema()
movie.show()

result3 = movie.withColumn(
        "timestamp",
        from_unixtime(col("timestamp"))\
               
).groupBy(year("timestamp").alias("Year")) \
         .agg(count("*").alias("Total_Ratings")) \
         .orderBy("Year")

result3.show()


# 4. Load Fire Service Calls with pre-defined schema. Repeat all 10 Hive assignments on that dataset. 
# Do the assignments using Dataframe syntax. Use Linux command below to create sample dataset.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("question4") \
    .getOrCreate()

filepath = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/Fire_Department_Calls_for_Service.csv"

fire_schema = StructType([
    StructField("CallNumber", IntegerType(), True),
    StructField("UnitID", StringType(), True),
    StructField("IncidentNumber", IntegerType(), True),
    StructField("CallType", StringType(), True),
    StructField("CallDate", StringType(), True),
    StructField("WatchDate", StringType(), True),
    StructField("CallFinalDisposition", StringType(), True),
    StructField("AvailableDtTm", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Zipcode", IntegerType(), True),
    StructField("Battalion", StringType(), True),
    StructField("StationArea", StringType(), True),
    StructField("Box", StringType(), True),
    StructField("OriginalPriority", StringType(), True),
    StructField("Priority", StringType(), True),
    StructField("FinalPriority", IntegerType(), True),
    StructField("ALSUnit", BooleanType(), True),
    StructField("CallTypeGroup", StringType(), True),
    StructField("NumAlarms", IntegerType(), True),
    StructField("UnitType", StringType(), True),
    StructField("UnitSequenceInCallDispatch", IntegerType(), True),
    StructField("FirePreventionDistrict", StringType(), True),
    StructField("SupervisorDistrict", StringType(), True),
    StructField("Neighborhood", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("RowID", StringType(), True),
    StructField("Delay", FloatType(), True)
])

fire = spark.read \
    .option("header", "true") \
    .schema(fire_schema) \
    .csv(filepath)

fire.printSchema()


# 1. How many distinct types of calls were made to the ﬁre department?
fire.select(count_distinct("CallType")).show()


# 2. What are distinct types of calls made to the ﬁre department?
fire.select("CallType").distinct().show(truncate=False)


# 3. Find out all responses for delayed times greater than 5 mins?
fire.select("CallNumber", "CallType", "Delay") \
       .filter(col("Delay") > 5) \
       .show()


# 4. What were the most common call types?
fire.groupBy("CallType") \
       .count() \
       .orderBy(col("count").desc()) \
       .show()

# 5. What zip codes accounted for the most common calls?
fire.groupBy("Zipcode") \
       .count() \
       .orderBy(col("count").desc()) \
       .show()


# 6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?
fire.select("Neighborhood", "Zipcode") \
       .filter(col("Zipcode").isin(94102, 94103)) \
       .distinct() \
       .show(truncate=False)


# 7. What was the sum of all calls, average, min, and max of the call response times?
fire.select(
    sum("Delay").alias("Total_Response_Time"),
    avg("Delay").alias("Avg_Response_Time"),
    min("Delay").alias("Min_Response_Time"),
    max("Delay").alias("Max_Response_Time")
).show()


# 8. How many distinct years of data are in the CSV ﬁle?
fire.withColumn("CallDateTS", to_timestamp("CallDate", "MM/dd/yyyy")) \
       .select(count_distinct(year("CallDateTS")).alias("Distinct_Years")) \
       .show()


# 9. What week of the year in 2018 had the most ﬁre calls?
fire.withColumn("CallDateTS", to_timestamp("CallDate", "MM/dd/yyyy")) \
       .filter(year("CallDateTS") == 2018) \
       .groupBy(weekofyear("CallDateTS").alias("Week")) \
       .count() \
       .orderBy(col("count").desc()) \
       .show(1)


# 10. What neighborhoods in San Francisco had the worst response time in 2018?
fire.withColumn("CallDateTS", to_timestamp("CallDate", "MM/dd/yyyy")) \
       .filter(year("CallDateTS") == 2018) \
       .groupBy("Neighborhood") \
       .agg(avg("Delay").alias("Avg_Delay")) \
       .orderBy(col("Avg_Delay").desc()) \
       .show()


spark.stop()

# 5. For each department, rank employees by their salary in descending order. Include employee name, 
# department name, salary, and their rank within the department. Handle ties so employees with the same 
# salary get the same rank, with the next rank(s) skipped.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("demo05") \
        .getOrCreate()

filepath = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/emp.csv"
filepath1 = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/dept.csv"

emp_schema = "empno INT, ename STRING, job STRING, mgr INT, hire DATE, sal DOUBLE, comm DOUBLE, deptno INT"
dept_schema = "deptID INT, dname STRING, location STRING"

emp = spark.read \
        .schema(emp_schema) \
        .csv(filepath)

dept = spark.read \
         .schema(dept_schema) \
         .csv(filepath1)

emp.show()
dept.show()
data = emp.join(dept, emp.deptno == dept.deptID, "inner")

wnd = Window.partitionBy("deptID").orderBy(col("sal").desc())

result3 = data.select(
        col("ename"),
        col("dname"),
        col("sal"),
        rank().over(wnd).alias("rnk")
)

result3.show()

# 6. Salary Difference from Department Average
#    For each employee, show how much their salary differs from their department's average salary. 
# Also show the department's maximum salary. - Output: ename, dname, sal, dept_avg_sal, sal_diff_from_avg, 
# dept_max_sal
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("demo06") \
        .getOrCreate()

filepath = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/emp.csv"
filepath1 = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/dept.csv"

emp_schema = "empno INT, ename STRING, job STRING, mgr INT, hire DATE, sal DOUBLE, comm DOUBLE, deptno INT"
dept_schema = "deptID INT, dname STRING, location STRING"

emp = spark.read \
        .schema(emp_schema) \
        .csv(filepath)

dept = spark.read \
         .schema(dept_schema) \
         .csv(filepath1)

emp.show()
dept.show()

data1 = emp.join(dept , emp.deptno == dept.deptID , "inner")

wnd = Window.partitionBy("dname")

result4 = data1.select(
        col("ename"),
        col("dname"),
        col("sal"),
        avg("sal").over(wnd).alias("dept_avg_sal"),
        (col("sal") - avg("sal").over(wnd)).alias("sal_diff_from_avg"),
        max("sal").over(wnd).alias("dept_max_sal")
)

result4.show()

# 7. For each department, identify:
#    - The first employee hired (earliest hire date)
#    - The last employee hired (most recent hire date)
#    - The employee hired immediately after each employee (by hire date)
#    - Output: deptno, dname, ename, hire, first_hire_flag, last_hire_flag, next_hire_employee
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
        .appName("demo07") \
        .getOrCreate()

filepath = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/emp.csv"
filepath1 = "/media/sunbeam/New Volume/CDAC DBDA/Big Data/BigData/data/dept.csv"

emp_schema = "empno INT, ename STRING, job STRING, mgr INT, hire DATE, sal DOUBLE, comm DOUBLE, deptno INT"
dept_schema = "deptID INT, dname STRING, location STRING"

emp = spark.read \
        .schema(emp_schema) \
        .csv(filepath)

dept = spark.read \
         .schema(dept_schema) \
         .csv(filepath1)

emp.show()
dept.show()

data = emp.join(dept, emp.deptno == dept.deptID, "inner")

wnd = Window.partitionBy("deptno") \
                .orderBy(col("hire").asc())
                

result = data.select(
    col("deptno"),
    col("dname"),
    col("ename"),
    col("hire"),
    first("ename").over(wnd).alias("first_hire_flag"),
    last("ename").over(wnd.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)).alias("last_hire_flag"),
    lead("ename").over(wnd).alias("next_hire_employee")
)

result.show()
