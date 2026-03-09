from pyspark import pipelines as dp
from pyspark.sql import functions as F
dbutils.widgets.text("catalog","retail")
dbutils.widgets.text("schema","schema_retail")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
layer = "silver"

@dp.table(name=f"{catalog}.{schema}.{layer}_products")
def load_silver_products():
    return (spark.read.table(f"{catalog}.{schema}.bronze_products"))
            
@dp.table(name=f"{catalog}.{schema}.{layer}_customer")
def load_silver_customer():
    return(
        spark.read.table(f"{catalog}.{schema}.bronze_customer"))
    

@dp.table(name=f"{catalog}.{schema}.{layer}_purchase")
def load_silver_purchase():
        return(spark.read.table(f"{catalog}.{schema}.bronze_purchase"))

@dp.table(name=f"{catalog}.{schema}.dim_product_category")
def load_silver_dim_product():
    return (spark.sql(f"""select row_number() over(order by category) as cat_id,category from (select distinct category from {catalog}.{schema}.bronze_products order by category asc) as a"""))

@dp.table(name=f"{catalog}.{schema}.dim_product")
def load_silver_dim_product():
    return (spark.sql(f"""select row_number() over(order by product_name asc) as product_id,product_name from (select distinct product_name from {catalog}.{schema}.bronze_products order by product_name asc) as a"""))
@


# @dp.materialized_view(name=f"{catalog}.{schema}.{layer}_mat_view")
# def load_mat_view():
#     df = spark.read.table(f"{catalog}.{schema}.bronze_purchase")
#     df1 = df.join(spark.read.table(f"{catalog}.{schema}.bronze_products"),how="inner",on="product_id",)
#     return (df1.withColumn("rpt_dt",F.current_timestamp)
                      
               