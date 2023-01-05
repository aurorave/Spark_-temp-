# 아파치 스파크 학습을 위한 도커 환경 셋업하기 (feat.Zeppelin)

%pyspark

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-filter.py

arrayStructureData = [
    (("James","","Smith"),
    ["Java","Scala","C++"], "OH", "M"),
    (("Anna","Rose",""),
    ["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),
    ["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),
    ["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),
    ["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),
    ["Python","VB"],"OH","M")
    ]
    
arrayStructureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('middlename', StringType(), True)
        ])),
        StructField('languages',
        ArrayType(StringType()), True),
        StructField('state', StringType(), True),
        StructField('gender', StringType(), True)
        ])

df = spark.createDataFrame(data = arrayStructureData, schema = arrayStructureSchema)
df.printSchema()
df.show()
