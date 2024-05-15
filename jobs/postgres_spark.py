from pyspark.sql import SparkSession # type: ignore

spark = SparkSession.builder \
    .appName("PostgreSQL_spark") \
    .config('spark.jars', 'jobs/lib/postgresql-42.7.3.jar') \
    .getOrCreate()

# df = spark.read.csv("${SPARK_HOME}/opt/airflow/jobs/data/loan.csv", header=True, inferSchema=True)

# df.write \
#   .format("jdbc") \
#   .option("url", "jdbc:postgresql://172.26.0.2:5432/airflow") \
#   .option("driver", "org.postgresql.Driver") \
#   .option("dbtable", "public.loan") \
#   .option("user", "airflow") \
#   .option("password", "airflow")\
#   .save()    

postgredf = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://172.27.0.2:5432/airflow") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.ab_user") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .load()

postgredf.printSchema()

postgredf.show(10)

fetchdata = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://172.27.0.2:5432/airflow") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.loan") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .load()
fetchdata.show(10)

fetchdata.createOrReplaceTempView("loan_data")
spark.sql("SELECT * FROM loan_data").show(5)
spark.stop()
