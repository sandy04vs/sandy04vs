- 👋 Hi, I’m @sandy04vs
- 👀 I’m interested in ...
- 🌱 I’m currently learning ...
- 💞️ I’m looking to collaborate on ...
- 📫 How to reach me ...

<!---
sandy04vs/sandy04vs is a ✨ special ✨ repository because its `README.md` (this file) appears on your GitHub profile.
You can click the Preview link to take a look at your changes.
--->
#Pyspark window functions(row_number(),rank(),dense_rank()) usage
import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import *
simpledata = [("Sandeep",100,94,23),
             ("Sarath",100,89,22),
             ("Venky",100,89,22),
             ("Rohith",100,87,21),
             ("Charan",100,87,16),
             ("Dhoni",100,99,42),
             ("Kohli",100,99,36),
             ("Rayudu",100,93,36),
             ("Ruturaj",100,91,23)]
schema = ["Player_name","Total_Score","Runs_scored","Age"]
df = spark.createDataFrame(data = simpledata , schema = schema)
df.printSchema()
df.show(truncate = False)
windowspec = Window.orderBy("Runs_scored")
df.withColumn("Row_Number",row_number().over(windowspec)).show(truncate = False) # row_number() function
df.withColumn("Rank",rank().over(windowspec)).show(truncate = False)             # rank() function
df2 = df.withColumn("Dense_Rank",dense_rank().over(windowspec))                  # dense_rank() function
df2.show(truncate = False)
df.withColumn("Lag_Function",lag(col("Runs_Scored"),2).over(windowspec)).show(truncate = False)         # lag() function
df.withColumn("Lead_Function",lead(col("Runs_Scored"),2).over(windowspec)).show(truncate = False)       # lead() function
