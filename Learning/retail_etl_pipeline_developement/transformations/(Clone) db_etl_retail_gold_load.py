from pyspark import pipelines as dp
@dp.materialized_view(name=f"{catelog}.{schema}.gold_customer_summary")
def create_view():
    return spark.sql(f"""select c.*,p.*,pr.* from {catelog}.{schema}.silver_purchase p  left join {catelog}.{schema}.silver_customer c on p.customer_id=c.customer_id join {catelog}.{schema}.silver_products pr on pr.product_id=p.product_id""")