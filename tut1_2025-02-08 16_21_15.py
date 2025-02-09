# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading json
# MAGIC

# COMMAND ----------

df_json = spark.read.format('json').option('infershema',True)\
    .option('header',True)\
    .option('multiline',False)\
    .load('/FileStore/tables/drivers.json')


# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL type schema

# COMMAND ----------

myddl_schema = ''' 
                    Item_Identifier string,
                    Item_Weight double,
                    Item_Fat_Content double,
                    Item_Visibility double,
                    Item_Type string,
                    Item_MRP double,
                    Outlet_Identifier string,
                    Outlet_Establishment_Year integer,
                    Outlet_Size string,
                    Outlet_Location_Type string,
                    Item_Outlet_Sales double
                '''

# COMMAND ----------

df = spark.read.format('csv')\
            .schema(myddl_schema)\
            .option('header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Struct type schema

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

my_struct_schema = StructType([
                                StructField('Item_Identifier',StringType(),True)
])

# COMMAND ----------

df = spark.read.format('csv')\
                                .schema(my_struct_schema)\
                                .option('header',True)\
                                .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select
# MAGIC

# COMMAND ----------

df.display()


# COMMAND ----------

df_sel = df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alias

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_Id')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter/Where

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiple condition pull
# MAGIC

# COMMAND ----------

df.filter((col('Item_type')=='Soft Drinks') & (col('item_weight')< 10)).display()

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 3','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_wt')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

df = df.withColumn('flag',lit('New'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('multipy',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','reg'))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','lf')).display()

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort/orderBy

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,0]).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit

# COMMAND ----------

df.limit(10).display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop

# COMMAND ----------

df.drop('Item_Identifier').display()

# COMMAND ----------

df.drop().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop_Duplicates
# MAGIC

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.drop_duplicates(['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Union , UnionByName

# COMMAND ----------

data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)


# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STRING functions
# MAGIC
# MAGIC

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type')).display()

# COMMAND ----------

df.select(initcap('Item_Type').alias('I_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Functions

# COMMAND ----------

df = df.withColumn('curr_date', current_date())

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('Fut_date', date_add('curr_date',7))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('week_before',date_sub('curr_date',7)).display()
