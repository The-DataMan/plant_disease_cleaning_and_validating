import duckdb
import os

conn=duckdb.connect()

input_folder="initial"
output_folder="sanitized"

os.makedirs(output_folder,exist_ok=True)
#turning csv into sql table to manipulate and query
#Modifying some column names to remove spaces
#Trimming as there were 3 distinct Disease states instead of 2 because of different formatting
conn.execute("""
CREATE TABLE disease_data AS
SELECT 
    Temperature,
    Humidity,
    "Wind Speed" AS Wind_Speed, 
    Visibility,
    Pressure,
    TRIM(Disease) AS Disease,
    "Disease In number" as Disease_in_number
FROM read_csv_auto(?)
""", [input_folder + '/disease_data.csv'])

conn.execute("""
COPY disease_data TO ? (
    HEADER,
    DELIMITER ',',
    OVERWRITE_OR_IGNORE
)
""", [output_folder + '/sanitized_disease_data.csv'])

conn.close()