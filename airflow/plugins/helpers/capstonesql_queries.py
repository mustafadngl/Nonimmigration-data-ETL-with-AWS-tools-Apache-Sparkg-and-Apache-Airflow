class SqlQueries:
    facti94 = (""" 
        INSERT INTO facti94(record_id,arrival_date,city,temp_id,port_id,demog_id)
        SELECT 
            i.record_id,
            i.arrival_date,
            i.port_city,
            t.temp_id,
            p.port_id, 
            d.demog_id            
        FROM staging_i94 i
        LEFT JOIN citytemperatures t ON i.port_city = t.city, i.arrival_month = t.month
        LEFT JOIN port p ON i.port_code = p.iata_code
        LEFT JOIN demographics d ON i.port_city = d.city
    """)
    
    personali94_insert = (""""
        INSERT INTO pesonali94
        SELECT DISTINCT
            i.record_id,
            i.age,
            i.birth_year,
            i.gender,
            i.country_born,
            i.country_residence,
            i.occup,
            i.airline,
            i.fltno,
            i.state,
            i.visa_purpose,
            i.port_code
         FROM staging_i94 i
    """)
    
    citytemperatures_insert = ("""
        INSERT INTO citytemperatures (
            city,
            month,
            avg_temp,
            avg_tempuncertainity,
            latitude,
            longitude)
        SELECT DISTINCT
            UPPER(t.City),
            t.month,
            t.avg_temp,
            t.avg_tempuncertainity,
            t.latitude,
            t.longitude
        FROM staging_temp t
    """)

    demographics_insert = ("""
        INSERT INTO demographics(
            city,
            state,
            state_code,
            median_age,
            male_pop,
            female_pop,
            total_pop,
            veteran_pop,
            foreignborn_pop,
            avg_household,
            dominant_race,
            dominant_racepop)
        SELECT
            UPPER(d.City),
            d.State,
            d."State Code",
            d."Median Age",
            d."Male Population",
            d."Female Population",
            d."Total Population",
            d."Number of Veterans",
            d."Foreign Born",
            d."Average Household Size",
            d.Race,
            d.Count
        FROM staging_demog d
    """)
    
    ports_insert = ("""
        INSERT INTO ports(
            iata_code,
            name,
            type,
            city,
            country-state,
            elevation_ft,
            gps_code,
            latitude,
            longitude)
        SELECT
            p.iata_code,
            p.name,
            p.type,
            UPPER(p.municipality),
            p.iso-region,
            p.elevation_ft,
            p.gps_code,
            SPLIT_PART(p.coordinates,",",1),
            SPLIT_PART(p.coordinates,",",2)
        FROM staging_ports p
    """)    
 
    date_insert = ("""
        INSERT INTO date
        SELECT
            i.arrival_date,
            EXTRACT(year FROM i.arrival_date),
            EXTRACT(month FROM i.arrival_date),
            EXTRACT(day FROM i.arrival_date),
            EXTRACT(week FROM i.arrival_date),
            EXTRACT(dayofweek FROM i.arrival_date),
            EXTRACT(quarter FROM i.arrival_date)
        FROM staging_i94 i        
    """)