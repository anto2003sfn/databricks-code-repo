from pyspark import pipelines as dp

@dp.table(name="cat_sales.default.silver_account")
def load_silver_account():
    return(spark.readStream.table("cat_sales.default.account"))
    
