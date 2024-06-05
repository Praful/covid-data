import pandas as pd
import sqlite3
# data sources:
#  red/blue/swing states: https://en.wikipedia.org/wiki/Red_states_and_blue_states
#  population: https://www.worldometers.info/coronavirus/country/us/
#  covid data: https://data.cdc.gov/NCHS/Provisional-COVID-19-Death-Counts-by-Week-Ending-D/r8kw-7aab/about_data
#
# Step 1: Read the CSV file into a pandas DataFrame
df = pd.read_csv(
    '../data/Provisional_COVID-19_Death_Counts_by_Week_Ending_Date_and_State_20240605 (1).csv')

# Step 2: Create a connection to a new SQLite database (or connect to an existing one)
conn = sqlite3.connect('../data/covid_database.db')

# Step 3: Insert the DataFrame into the database
df.to_sql('usa_covid', conn, if_exists='replace', index=False)


print(pd.read_sql_query(
    'select cdc.state, sum("COVID-19 Deaths") as deaths from usa_covid cdc group by state order by deaths', conn))


print(pd.read_sql_query('select cdc.state, sum("COVID-19 Deaths") as deaths, wm.population as "Population", round(sum("covid-19 deaths")/wm.population*1000000) as "Deaths per 1m" from usa_covid cdc join worldmeter_covid wm on cdc.state=wm."usa state" join  where cdc.`group`="By Month" group by cdc.state, wm.population order by "Deaths per 1m";', conn))

# Step 4: Perform SQL queries
query = "SELECT * FROM my_table WHERE column_name = 'some_value'"
result = pd.read_sql_query(query, conn)

# Display the query result
print(result)
