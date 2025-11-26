import duckdb


conn=duckdb.connect()

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
FROM read_csv_auto('initial/disease_data.csv')
""")

conn.execute("""
COPY disease_data TO 'sanitized/sanitized_disease_data.csv' (
    HEADER,
    DELIMITER ',',
    OVERWRITE_OR_IGNORE
)
""")

conn.close()