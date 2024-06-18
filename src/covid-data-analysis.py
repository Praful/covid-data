import pandas as pd
import sqlite3
# data sources:
#  red/blue/swing states: https://en.wikipedia.org/wiki/Red_states_and_blue_states
#  population: https://www.worldometers.info/coronavirus/country/us/
#  covid data: https://data.cdc.gov/NCHS/Provisional-COVID-19-Death-Counts-by-Week-Ending-D/r8kw-7aab/about_data
#
conn = sqlite3.connect('../data/covid_database.db')


def run(query):
    result = pd.read_sql_query(query, conn)
    # Display the query result
    print(result)


query = '''
    select
        cdc.state,
        sum("COVID-19 Deaths") as deaths,
        wm.population as "Population",
        round(sum("covid-19 deaths")/wm.population*1000000) as "Deaths per 1m",
        rb.party
    from
        cdc_covid_state cdc
    left join
        worldmeter_covid wm on cdc.state=wm."usa state"
    left join
        red_blue_states rb on rb.state=cdc.state
    where
        cdc.`group`="By Month" and
        cdc."End Date" >= "2021-02-01"
    group by
        cdc.state, wm.population
    order by
        "Deaths per 1m" desc;
'''
#  print(pd.read_sql_query(query, conn))
run(query)


county_election_winners = '''
    WITH RankedVotes AS (
        SELECT 
            county_name,
            county_fips,	
            year,
            party, 
            candidatevotes,
            ROW_NUMBER() OVER (PARTITION BY county_fips, year ORDER BY candidatevotes DESC) as rank
        FROM 
            harvard_county_pres_elections
    )
    SELECT 
        county_name,
        county_fips,
        year,
        party, 
        candidatevotes
    FROM 
        RankedVotes
    WHERE 
        rank = 1
    order by
        county_fips
'''


county_election_winners_swing = '''
    WITH RankedVotes AS (
        SELECT 
            county_name, 
            county_fips, 
            year,
            party, 
            candidatevotes,
            ROW_NUMBER() OVER (PARTITION BY county_fips, year ORDER BY candidatevotes DESC) as rank
        FROM 
            harvard_county_pres_elections
    ),
    TopVotes AS (
        SELECT 
            county_name, 
            county_fips, 
            year,
            party, 
            candidatevotes,
            ROW_NUMBER() OVER (PARTITION BY county_fips ORDER BY year) as year_rank,
            LAG(party) OVER (PARTITION BY county_fips ORDER BY year) as previous_party
        FROM 
            RankedVotes
        WHERE 
            rank = 1
    )
    SELECT 
        county_name, 
        county_fips, 
        year,
        party, 
        candidatevotes,
        CASE 
            WHEN party != previous_party THEN 'SWING'
            ELSE party
        END as change
    FROM 
        TopVotes
    ORDER BY 
        county_fips, year;

'''

run(county_election_winners)
run(county_election_winners_swing)
