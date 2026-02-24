from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

@dp.table(name="cat_sales.default.gold_account1")
def load_gold_account():
    windowSpec = Window.orderBy(F.col("id").desc())
    df = (spark.read.table("cat_sales.default.silver_account")
          .withColumn("surrogate_key", F.row_number().over(windowSpec))
          .withColumn("source", F.lit("sales force")))
    return df
