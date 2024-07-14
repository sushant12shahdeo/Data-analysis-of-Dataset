
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate()
df= spark.read.csv("/FileStore/ADSI_Table_1_5__1_-1.csv",header=True, inferSchema=True)
df=df.withColumnRenamed ("Total - Total", "Total_Affected")
df=df.withColumnRenamed ("Total - Percentage Share", "Percentage_Share")
df.createOrReplaceTempView("disaster_schema")
#Total number of people affected by each cause
total_affected=spark.sql("""SELECT cause, SUM(Total_Affected) AS Total_Affected FROM disaster_schema GROUP BY cause""")
total_affected.show()
#Percentage share by each cause
percentage_share = spark.sql("""SELECT cause,AVG (Percentage_Share) AS Percentage_Share FROM disaster_schema GROUP BY cause""")
percentage_share.show()
total_affected_pd =total_affected.toPandas()
#Plotting total number of people affected by each cause
plt.bar(total_affected_pd['cause'], total_affected_pd['Total_Affected'])
plt.xlabel('cause')
plt.ylabel('Total Affected')
plt.title("Total Number of People Affected by Each Cause")
plt.xticks (rotation =90)
plt.show()
#df.printSchema ()
display(df)
  
