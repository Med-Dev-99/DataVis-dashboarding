from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

class Spark:
    def __init__(self):
        pass

    def getContext(self,master="local"):
        sc = SparkContext.getOrCreate(SparkConf().setMaster(master)
            .set("spark.hadoop.yarn.resourcemanager.hostname","172.22.0.2")
            .set("spark.hadoop.yarn.resourcemanager.address", "172.22.0.2:8032"))
        sc.setLogLevel("INFO")
        return sc
        
    def getSession(self,name,master="local"):
        spark = SparkSession.builder\
        .appName(name)\
        .master(master).getOrCreate()
        return spark


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")