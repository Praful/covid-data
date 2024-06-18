import pandas as pd
import sqlite3
# data sources:
#  red/blue/swing states: https://en.wikipedia.org/wiki/Red_states_and_blue_states
#  population: https://www.worldometers.info/coronavirus/country/us/
#  covid data: https://data.cdc.gov/NCHS/Provisional-COVID-19-Death-Counts-by-Week-Ending-D/r8kw-7aab/about_data
#
conn = sqlite3.connect('../data/covid_database.db')


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
