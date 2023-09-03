from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

spark = SparkSession.builder.appName("person_info").getOrCreate()

schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("date birthday", DateType(), True),
    StructField("Address", StringType(), True)
])
data_sets = [

    ("John", "Doe", 25, "male", date(1998, 5, 15), "New-York"),
    ("Jane", "Smith", None, "female", date(2000, 2, 20), None),
    (None, None, 30, "Male", date(1993, 11, 10), "Los Angeles"),
    ("Alice", "Johnson", 28, None, date(1995, 8, 2), "London"),
    ("Kelvin", "Harris", 29, "male", date(1994, 8, 2), "Paris"),
    ("Kelvin", "Harris", 29, None, date(1994, 8, 2), "Rome"),
    ("Eva", "Williams", 30, "female", date(1993, 8, 2), "Milan"),
    ("Michael", "Brown", 26, None, date(1998, 8, 2), "London"),
]
df = spark.createDataFrame([], schema)

data2 = [
    ("John", "Doe", "Doctor"),
    ("Jane", "Smith", "Engineer"),
    ("Alice", "Johnson", "Teacher"),
    ("Michael", "Brown", "Student"),
    ("Eva", "Williams", "Artist"),
]

schema2 = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("occupation", StringType(), True),
])

df2 = spark.createDataFrame(data2, schema=schema2)

"""/// base method ///"""

# count df row
df_count = df.count()

# filter all column isNull
df_na = df.na.drop(how='all')

# drop duplicates
df_without_duplicates = df.dropDuplicates()

# df show first 5 row
df.show(5)

"""/// filter method ///"""

# Task 1: Count the number of records (rows) in the DataFrame
count = df.count()

# Task 2: Count the number of unique values in the "gender" column
unique_gender_count = df.select("gender").distinct()
unique_gender_count.show()

# Task 3: Filter records where age is greater than 30
filtered_df = df.filter(df.age > 30)
filtered_df.show()

# Task 4: Filter records where age is greater than 30
filtered_df_1 = df.filter(df.age > 30)
filtered_df_1.show()

# Task 5: Filter out men
filtered_df_2 = df.filter(df.gender == "Male")
filtered_df_2.show()

# Task 6: Filter entries where address is not specified
filtered_df_3 = df.filter(df.address.isNull())
filtered_df_3.show()

# Task 7: Filter out entries where the name starts with "J"
filtered_df_4 = df.filter(df.first_name.startswith("J"))
filtered_df_4.show()

# Task 8: Filter out records where the date of birth is after 1990
filtered_df_5 = df.filter(df.date_birthday > date(1990, 1, 1))
filtered_df_5.show()

"""/// group_by method ///"""

# Task 1: Calculate the average age for each gender
df.groupBy("gender").agg({"age": "avg"}).show()

# Task 2: Count the number of people in each city
df.groupBy("address").count().show()

# Task 3: Find the youngest and oldest person in the DataFrame
df.groupBy().agg({"age": "min", "age": "max"}).show()

# Task 4: Count the number of men and women
df.groupBy("gender").count().show()

# Task 5: Count the number of people in each city, but only for men
filtered_df_2 = df.filter(df.gender == "Male")
filtered_df_2.groupBy("address").count().show()

"""/// join method ///"""

# Task 1: Execute an internal join on first and last name and print the result
joined_df_1 = df.join(df2, on=["first_name", "last_name"], how="inner")
joined_df_1.show()

# Task 2: Run a join with a filter where age is over 25 and profession is "Teacher"
filtered_joined_df = df.join(df2, on=["first_name", "last_name"], how="inner").filter(
    (df.age > 25) & (df.occupation == "Teacher"))
filtered_joined_df.show()