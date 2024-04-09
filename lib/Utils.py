from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_conf


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_spark_conf(env)) \
            .config('spark.sql.autoBroadcastJoinThreshold',-1) \
            .config('spark.sql.adaptive.enabled','false') \
            .config('HADOOP_HOME',r'C:\spark\hadoop-3.3.5') \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties') \
            .master("local[2]") \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf=get_spark_conf(env)) \
            .enableHiveSupport() \
            .getOrCreate()
