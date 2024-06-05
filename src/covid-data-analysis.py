import pandas as pd
import sqlite3
# data sources:
#  red/blue/swing states: https://en.wikipedia.org/wiki/Red_states_and_blue_states
#  population: https://www.worldometers.info/coronavirus/country/us/
#  covid data: https://data.cdc.gov/NCHS/Provisional-COVID-19-Death-Counts-by-Week-Ending-D/r8kw-7aab/about_data
#
conn = sqlite3.connect('../data/covid_database.db')


def load(conn, csv, table, date_column=None):
    df = pd.read_csv(csv, parse_dates=date_column if date_column else None)
    df.to_sql(table, conn, if_exists='replace', index=False)

# do once
#  load(conn,'../data/Provisional_COVID-19_Death_Counts_by_Week_Ending_Date_and_State_20240605.csv', 'cdc_covid', ['Data as of', 'Start Date', 'End Date', 'Week Ending Date'])
#  load(conn,'../data/worldmeter-covid-usa.csv', 'worldmeter_covid')
#  load(conn,'../data/red_blue_states.csv', 'worldmeter_covid')

#  print(pd.read_sql_query( 'select cdc.state, sum("COVID-19 Deaths") as deaths from usa_covid cdc group by state order by deaths', conn))


query = '''
    select
        cdc.state,
        sum("COVID-19 Deaths") as deaths,
        wm.population as "Population",
        round(sum("covid-19 deaths")/wm.population*1000000) as "Deaths per 1m",
        rb.party
    from
        cdc_covid cdc
    left join
        worldmeter_covid wm on cdc.state=wm."usa state"
    left join
        red_blue rb on rb.state=cdc.state
    where
        cdc.`group`="By Month" and
        cdc."End Date" >= "2021-02-01"
    group by
        cdc.state, wm.population
    order by
        "Deaths per 1m" desc;
'''
print(pd.read_sql_query(query, conn))

#  result = pd.read_sql_query(query, conn)

# Display the query result
#  print(result)
