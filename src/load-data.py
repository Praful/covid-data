
import pandas as pd
import sqlite3
# data sources:
#  red/blue/swing states: https://en.wikipedia.org/wiki/Red_states_and_blue_states
#  population: https://www.worldometers.info/coronavirus/country/us/
#  covid data by state: https://data.cdc.gov/NCHS/Provisional-COVID-19-Death-Counts-by-Week-Ending-D/r8kw-7aab/about_data
#  covid data by county: https://wonder.cdc.gov/mcd-icd10-provisional.html
#  county presidential results: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VOQCHQ
# vulnerability data: https://www.atsdr.cdc.gov/placeandhealth/svi/interactive_map.html#more-pcm
conn = sqlite3.connect('../data/covid_database.db')


def load(conn, csv, table, sep='\t', date_column=None):
    print(40*'-')
    print('Loading', csv, 'to', table)
    try:
        df = pd.read_csv(csv, sep=sep, parse_dates=date_column if date_column else None)
        df.to_sql(table, conn, if_exists='replace', index=False)
        print('Done')
    except Exception as e:
        print('Failed', e)


load(conn, '../data/Provisional_COVID-19_Death_Counts_by_Week_Ending_Date_and_State_20240605.csv',
     'cdc_covid_state', sep=',', date_column=['Data as of', 'Start Date', 'End Date', 'Week Ending Date'])

load(conn, '../data/Provisional Mortality Statistics, 2018 through Last Week(tab).csv',
     'cdc_covid_county')

load(conn, '../data/worldmeter-covid-usa.csv', 'worldmeter_covid')

load(conn, '../data/red-blue-states.csv', 'red_blue_states')

# presidential election results by country
load(conn, '../data/countypres_2000-2020.csv', 'harvard_county_pres_elections')

# social vulnerability index
load(conn, '../data/svi_interactive_map.csv', 'harvard_vulnerability', sep=',')

#  print(pd.read_sql_query(
    #  'select cdc.state, sum("COVID-19 Deaths") as deaths from usa_covid cdc group by state order by deaths', conn))
