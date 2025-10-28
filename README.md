 Star Wars Survey Data Cleaning with PySpark
 Overview:

This project processes and cleans raw Star Wars survey data using Apache Spark (PySpark) and stores the cleaned results into a MySQL database.

It handles column renaming, data cleaning, missing value imputation, and column combination — producing a structured dataset ready for analytics or visualization.

 Features:

 Runs on a Spark cluster (spark://spark-master:7077)

 Automatically cleans and standardizes column names

 Drops completely empty columns

 Combines fragmented question columns into single logical fields

 Handles missing values with default placeholders

 Removes duplicate header rows

 Writes cleaned data to:

Local CSV (/data/output.csv)

MySQL database table (starwars_cleaned)

Project Structure
├── StarWars.csv                  # Raw input data
├── main.py                       # ETL PySpark script (this file)
├── /data/                        # Mounted data folder (input + output)
│   ├── StarWars.csv
│   └── output.csv/
└── /extra-jars/
    └── mysql-connector-j-9.4.0.jar


Transformation Steps:
1. Load CSV

Reads the Star Wars survey CSV file into a Spark DataFrame with headers and inferred schema.

2. Clean Column Names

Removes special characters and spaces

Replaces them with underscores (_)

Converts all names to lowercase

3. Drop Empty Columns

Only keeps columns that contain at least one non-null value.

4. Rename Key Columns

Maps long or truncated survey question names to readable ones like:
| Original Prefix                                      | New Column Name          |
| ---------------------------------------------------- | ------------------------ |
| `have_you_seen_any_of_the_6_films`                   | `has_seen_star_wars`     |
| `please_rank_the_star_wars_films_in_order_of_prefer` | `star_wars_film_ranking` |
| `please_state_whether_you_view_the_following_charac` | `character_opinion`      |
| `which_character_shot_first`                         | `first_shooter`          |
| ...                                                  | ...                      |


5. Combine Related Columns

Merges multiple fragmented survey question columns into one using concat_ws().

For example:
star_wars_films_seen = concat_ws(" , ", c3, c4, c5, c6, c7, c8)

6. Fill Missing Values

Replaces missing or empty values with placeholders (e.g. "N/A" or "None").

7. Remove Redundant Header Rows

Eliminates any repeated header text rows (e.g. “Response” entries).

8. Save Cleaned Data

Writes the final DataFrame to /data/output.csv

Uploads to MySQL (starwars_cleaned table) using the JDBC connector

🗄️ Database Configuration

MySQL Connection Details
| Property | Value                      |
| -------- | -------------------------- |
| Host     | `mysql-db`                 |
| Port     | `3306`                     |
| Database | `starwars_db`              |
| Table    | `starwars_cleaned`         |
| User     | `sparkuser`                |
| Password | ``                |
| Driver   | `com.mysql.cj.jdbc.Driver` |

🧰 Requirements
Prerequisites

Apache Spark 3.x

MySQL server

Docker (optional, for containerized Spark setup)

Python Libraries

pyspark

re

Extra JARs

Ensure the MySQL connector JAR is mounted:
/extra-jars/mysql-connector-j-9.4.0.jar


