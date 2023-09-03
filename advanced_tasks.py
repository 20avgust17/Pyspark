from pyspark.sql import SparkSession, Window
from pyspark.sql.connect.functions import countDistinct
from pyspark.sql.functions import col, sum, dense_rank
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


"""task-1"""
spark = SparkSession.builder.appName("complex_task_1").getOrCreate()

schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("sales_amount", IntegerType(), True),
])

data = [
    ("Store_A", "Product_X", 100),
    ("Store_A", "Product_Y", 150),
    ("Store_B", "Product_X", 120),
    ("Store_B", "Product_Y", 130),
    ("Store_A", "Product_X", 110),
    ("Store_B", "Product_Y", 140),
    ("Store_A", "Product_Y", 160),
    ("Store_B", "Product_X", 130),
    ("Store_A", "Product_Y", 170),
    ("Store_B", "Product_X", 110),
    ("Store_A", "Product_X", 120),
    ("Store_B", "Product_Y", 180),
]

df = spark.createDataFrame(data, schema=schema)

# Grouping by stores and summing sales
total_sales_df = df.groupBy("store_id").agg(sum("sales_amount").alias("total_sales"))

# Find the store with the highest sales
max_sales_store = total_sales_df.orderBy(col("total_sales").desc()).first()["store_id"]

# Display a list of products sold in this store
products_sold_in_max_sales_store = df.filter(df.store_id == max_sales_store).select("product_id").distinct()
products_sold_in_max_sales_store.show()

spark.stop()

"""task-2"""

spark = SparkSession.builder.appName("complex_task_2").getOrCreate()

schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
])

data = [
    (1, "John Doe", "HR", 60000),
    (2, "Jane Smith", "HR", 55000),
    (3, "Alice Johnson", "IT", 75000),
    (4, "Bob Brown", "IT", 80000),
    (5, "Michael Lee", "Finance", 70000),
    (6, "David Wilson", "Finance", 72000),
    (7, "Eva Williams", "IT", 77000),
    (8, "Sarah Davis", "Finance", 71000),
    (9, "James Jones", "HR", 59000),
    (10, "Linda Miller", "IT", 76000),
]

df = spark.createDataFrame(data, schema=schema)

# Rank employees by salary in each department
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

ranked_df = df.withColumn("rank", dense_rank().over(window_spec))

# Select the top 3 employees in each department
top_3_employees = ranked_df.filter(col("rank") <= 3).select("employee_name", "department", "salary")
top_3_employees.show()

spark.stop()

"""task-3"""

spark = SparkSession.builder.appName("complex_task_3").getOrCreate()

sales_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
])

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
])

sales_data = [
    ("Store_A", "Product_X", 1),
    ("Store_A", "Product_Y", 2),
    ("Store_B", "Product_X", 3),
    ("Store_B", "Product_Y", 4),
    ("Store_A", "Product_X", 1),
    ("Store_B", "Product_Y", 2),
    ("Store_A", "Product_Y", 3),
    ("Store_B", "Product_X", 4),
    ("Store_A", "Product_Y", 1),
    ("Store_B", "Product_X", 2),
    ("Store_A", "Product_X", 3),
    ("Store_B", "Product_Y", 4),
]

# Create a DataFrame with customer data
customers_data = [
    (1, "John Doe", "john.doe@example.com"),
    (2, "Jane Smith", "jane.smith@example.com"),
    (3, "Alice Johnson", "alice.johnson@example.com"),
    (4, "Bob Brown", "bob.brown@example.com"),
]

df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_customers = spark.createDataFrame(customers_data, schema=customers_schema)

# Join both DataFrames
combined_df = df_sales.join(df_customers, on="customer_id", how="inner")

# Grouping products and counting the number of unique customers
product_customer_count = combined_df.groupBy("product_id").agg(countDistinct("email").alias("customer_count"))

# Find the top 3 products by the number of unique customers
top_3_products = product_customer_count.orderBy(col("customer_count").desc()).limit(3)
top_3_products.show()

spark.stop()
