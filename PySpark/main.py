import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------
# Spark session
# ---------------------------
spark = (
    SparkSession.builder
    .appName("StarWarsCsvSpark")
    .master("spark://spark-master:7077")
    .config("spark.jars", "/extra-jars/mysql-connector-j-9.4.0.jar")
    .config("spark.driver.extraClassPath", "/extra-jars/mysql-connector-j-9.4.0.jar")
    .config("spark.executor.extraClassPath", "/extra-jars/mysql-connector-j-9.4.0.jar")
    .getOrCreate()
)

# ---------------------------
# Read CSV
# ---------------------------
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", True) \
    .load("/data/StarWars.csv")

# ---------------------------
# Clean column names
# ---------------------------
def clean_colname(name):
    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return name.lower()[:50] or "col"

df = df.toDF(*[clean_colname(c) for c in df.columns])

# ---------------------------
# Drop fully empty columns only
# ---------------------------
def drop_empty_columns(df):
    non_empty_cols = [c for c in df.columns if df.select(c).na.drop().count() > 0]
    return df.select(non_empty_cols)

# ---------------------------
# Fill missing key fields
# ---------------------------
def fill_empty(df):
    df = df.replace("", None)
    return df.na.fill({
        'are_you_a_fan': 'N/A',
        'star_wars_films_seen': 'None',
        'star_wars_film_ranking': 'No Rankings',
        'character_opinion': 'N/A',
        'first_shooter': 'N/A',
        'familiar_with_expanded_universe': 'N/A',
        'fan_of_expanded_universe' : 'N/A',
        'st_fan': 'N/A',
        'gender': 'N/A',
        'age': 'N/A',
        'household_income' : 'N/A',
        'education': 'N/A',
        'location_census_region': 'N/A'
    })

# ---------------------------
# Combine related columns
# ---------------------------
def combined_cols(df):
    # Combine related columns
    df = df.withColumn(
        "star_wars_films_seen",
        F.concat_ws(" , ", df.star_wars_films_seen, df.c4, df.c5, df.c6, df.c7, df.c8)
    )

    df = df.withColumn(
        "star_wars_film_ranking",
        F.concat_ws(" , ", df.star_wars_film_ranking, df.c10, df.c11, df.c12, df.c13, df.c14)
    )

    df = df.withColumn(
        "character_opinion",
        F.concat_ws(" , ", df.character_opinion, df.c16, df.c17, df.c18, df.c19, df.c20, df.c21, df.c22,
                    df.c23, df.c24, df.c25, df.c26,df.c27, df.c28)
    )

    # Drop intermediate columns
    df = df.drop(
        "c4", "c5", "c6", "c7", "c8",
        "c10", "c11", "c12", "c13", "c14", "c16",
        "c17", "c18", "c19", "c20", "c21", "c22",
        "c23", "c24", "c25", "c26" ,"c27", "c28"
    )

    return df

# ---------------------------
# Apply transformations
# ---------------------------
df = drop_empty_columns(df)

# Rename key columns
rename_map = [
    ("have_you_seen_any_of_the_6_films", "has_seen_star_wars"),
    ("do_you_consider_yourself_to_be_a_fan_of_the_star_w", "are_you_a_fan"),
    ("which_of_the_following_star_wars_films_have_you_se", "star_wars_films_seen"),
    ("please_rank_the_star_wars_films_in_order_of_prefer", "star_wars_film_ranking"),
    ("please_state_whether_you_view_the_following_charac", "character_opinion"),
    ("which_character_shot_first", "first_shooter"),
    ("are_you_familiar_with_the_expanded_universe", "familiar_with_expanded_universe"),
    ("do_you_consider_yourself_to_be_a_fan_of_the_expand", "fan_of_expanded_universe"),
    ("do_you_consider_yourself_to_be_a_fan_of_the_star_t", "st_fan")
]

for old_start, new_name in rename_map:
    col_to_rename = [c for c in df.columns if c.startswith(old_start)]
    if col_to_rename:
        df = df.withColumnRenamed(col_to_rename[0], new_name)

# Remove repeated header row (if any)
if "has_seen_star_wars" in df.columns:
    df = df.filter(~F.col("has_seen_star_wars").contains("Response"))

# Combine columns (important: assign result)
df = combined_cols(df)

# Fill in missing values
df = fill_empty(df)

# ---------------------------
# Inspect
# ---------------------------
print("Columns after cleaning:", df.columns)
print("Number of rows:", df.count())
df.show(5, truncate=False)

# ---------------------------
# Write cleaned CSV
# ---------------------------
df.coalesce(1) \
  .write.mode("overwrite") \
  .option("header", True) \
  .csv("/data/output.csv")

# ---------------------------
# Write to MySQL
# ---------------------------
jdbc_url = "jdbc:mysql://mysql-db:3306/starwars_db"
table_name = "starwars_cleaned"
properties = {
    "user": "sparkuser",
    "password": "sparkpass",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df.write \
    .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)

print(f" Data written successfully to MySQL table: {table_name}")

