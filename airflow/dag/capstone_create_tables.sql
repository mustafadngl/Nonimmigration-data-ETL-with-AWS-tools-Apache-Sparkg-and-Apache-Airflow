CREATE TABLE IF NOT EXISTS public.staging_i94(
	record_year BIGINT,
    record_month INT,
    arrival_date DATE,
    arrival_year INT,
    arrival_month INT,
    arrival_day INT,
    departure_date DATE,
    age INT,
    gender VARCHAR(1),
    country_born VARCHAR(256),
    country_residence VARCHAR(256),
    state_abbr VARCHAR(2),
    state VARCHAR(40),
    visa_purpose VARCHAR(15),
    visatype VARCHAR(2),
    birth_year INT,
    port_code VARCHAR(3),
    port_city VARCHAR(15),
    occup VARCHAR(30),
    airline VARCHAR(10),
    fltno INT,
    record_id BIGINT   
);


CREATE TABLE IF NOT EXISTS public.staging_temp(
	City VARCHAR(15),
    Country VARCHAR(15),
    Lattitude VARCHAR(256),
    Longitude VARCHAR(256),
    month INT,
    avg_temperature NUMERIC(5,3),
    avg_tempuncertainity NUMERIC(4,3)
    
);

CREATE TABLE IF NOT EXISTS public.staging_demog(
	City VARCHAR(20),
    State VARCHAR (20),
    "Median Age" NUMERIC(3,1),
    "Male Population" BIGINT,
    "Female Population" BIGINT,
    "Total Population" BIGINT,
    "Number of Veterans" BIGINT,
    "Foreign Born" BIGINT,
    "Average Household Size" NUMERIC(3,2),
    "State Code" VARCHAR(2),
    Race VARCHAR(150),
    Count BIGINT
);

CREATE TABLE IF NOT EXISTS public.staging_port(
	ident VARCHAR(10),
    type VARCHAR(20),
    name VARCHAR(100),
    elevation_ft INT,
    continent VARCHAR(2),
    iso_country VARCHAR(2),
    iso_region VARCHAR(5),
    municipality VARCHAR(30),
    gps_code VARCHAR(10),
    iata_code VARCHAR(3),
    local_code VARCHAR(10),
    coordinates VARCHAR(256)   
);

CREATE TABLE IF NOT EXISTS public.personali94(
	record_id BIGINT NOT NULL,
    age INT,
    birth_year INT,
    gender VARCHAR(1),
    country_born VARCHAR(256),
    country_residence VARCHAR(256),
    occupation VARCHAR(50),
    airline VARCHAR(10),
    flight_no INT,
    state VARCHAR(40),
    visa_purpose VARCHAR(15),
    port_code VARCHAR(3),
    CONSTRAINT personalI94_pkey PRIMARY KEY (record_id)
);

CREATE TABLE IF NOT EXISTS public.ports(
	port_id INT GENERATED ALWAYS AS IDENTITY,
    iata_code VARCHAR(3) NOT NULL,
    name VARCHAR(100),
    type VARCHAR(20),
    city VARCHAR(30),
    country_state VARCHAR(15),
    elevation_ft INT,
    gps_code VARCHAR(10),
    latitude TEXT,
    longitude TEXT,
	CONSTRAINT ports_pkey PRIMARY KEY (port_id)
);

    
CREATE TABLE IF NOT EXISTS public.citytemperatures(
	temp_id INT GENERATED ALWAYS AS IDENTITY,
    city VARCHAR(15) NOT NULL,
    month INT,
    avg_temp NUMERIC(5,3),
    avg_tempuncertainity NUMERIC(4,3),
    latitude VARCHAR(256),
    longitude VARCHAR(256),
    CONSTRAINT citytemperature_pkey PRIMARY KEY (temp_id)
);

CREATE TABLE IF NOT EXISTS public.demographics(
	demog_id INT GENERATED ALWAYS AS IDENTITY,
    city VARCHAR(20) NOT NULL,
    state VARCHAR(20),
    state_code VARCHAR(2),
    median_age NUMERIC(3,1),
    male_pop BIGINT,
    female_pop BIGINT,
    total_pop BIGINT,
    veteran_pop BIGINT,
    foreignborn_pop BIGINT,
    avg_household NUMERIC(3,2),
    dominant_race VARCHAR(150),
    dominant_racepop BIGINT,
    CONSTRAINT demographics_pkey PRIMARY KEY (demog_id)
);

CREATE TABLE IF NOT EXISTS public.date(
	arrival_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    dayofweek INT NOT NULL,
    quarter INT NOT NULL,
    CONSTRAINT date_pkey PRIMARY KEY (arrival_date)
);

CREATE TABLE IF NOT EXISTS public.facti94(
	facti94_id INT GENERATED ALWAYS AS IDENTITY,
	record_id INT NOT NULL,
    arrival_date DATE NOT NULL,
    city VARCHAR(20) NOT NULL,
    temp_id INT,
    port_id INT,
    demog_id INT
);