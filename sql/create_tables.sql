CREATE TABLE Dim_County
(
 county_id               varchar(5) NOT NULL,
 county_name             varchar(50) NOT NULL,
 state_id                varchar(3) NOT NULL,
 population              integer NOT NULL,
 area                    decimal NOT NULL,
 population_density      decimal NOT NULL,
 poor_health             decimal NOT NULL,
 smokers                 decimal NOT NULL,
 obesity                 decimal NOT NULL,
 physical_inactivity     decimal NOT NULL,
 excessive_drinking      decimal NOT NULL,
 uninsured               decimal,
 physicians              decimal,
 unemployment            decimal,
 air_pollution           decimal,
 housing_problems        decimal NOT NULL,
 household_overcrowding  decimal,
 food_insecurity         decimal NOT NULL,
 residential_segregation decimal,
 over_sixtyfives         decimal NOT NULL,
 rural                   decimal,
 CONSTRAINT county_pkey PRIMARY KEY ( county_id )
);

CREATE TABLE Dim_Date
(
 "date"      timestamp NOT NULL,
 day         integer NOT NULL,
 week        integer NOT NULL,
 month       integer NOT NULL,
 "year"      integer NOT NULL,
 day_of_week integer NOT NULL,
 CONSTRAINT date_pkey PRIMARY KEY ( "date" )
)
DISTSTYLE KEY DISTKEY ( "date" )
INTERLEAVED SORTKEY ( "year", month, day );

CREATE TABLE Dim_State
(
 state_id                varchar(3) NOT NULL,
 state_name              varchar(50) NOT NULL,
 population              integer NOT NULL,
 area                    decimal NOT NULL,
 population_density      decimal NOT NULL,
 poor_health             decimal NOT NULL,
 smokers                 decimal NOT NULL,
 obesity                 decimal NOT NULL,
 physical_inactivity     decimal NOT NULL,
 excessive_drinking      decimal NOT NULL,
 uninsured               decimal NOT NULL,
 physicians              decimal NOT NULL,
 unemployment            decimal NOT NULL,
 air_pollution           decimal,
 housing_problems        decimal NOT NULL,
 household_overcrowding  decimal,
 food_insecurity         decimal NOT NULL,
 residential_segregation decimal NOT NULL,
 over_sixtyfives         decimal NOT NULL,
 rural                   decimal NOT NULL,
 CONSTRAINT state_pkey PRIMARY KEY ( state_id )
);

CREATE TABLE Fact_CountyTimeSeries
(
 county_id    varchar(5) NOT NULL,
 "date"       timestamp NOT NULL,
 covid_cases  integer NOT NULL,
 covid_deaths integer NOT NULL,
 min_temp     decimal NOT NULL,
 max_temp     decimal NOT NULL,
 cloud_cover  decimal NOT NULL,
 wind         decimal NOT NULL,
 CONSTRAINT fact_countyTimeSeries_pkey PRIMARY KEY ( county_id, "date" ),
 CONSTRAINT fact_countyTimeSeries_county_fkey FOREIGN KEY ( county_id ) REFERENCES Dim_County ( county_id ),
 CONSTRAINT fact_countyTimeSeries_date_pkey FOREIGN KEY ( "date" ) REFERENCES Dim_Date ( "date" )
);

CREATE TABLE Fact_StateTimeSeries
(
 "date"       timestamp NOT NULL,
 state_id     varchar(3) NOT NULL,
 covid_cases  integer NOT NULL,
 covid_deaths integer NOT NULL,
 CONSTRAINT fact_stateTimeSeries_pkey PRIMARY KEY ( "date", state_id ),
 CONSTRAINT fact_stateTimeSeries_state_fkey FOREIGN KEY ( state_id ) REFERENCES Dim_State ( state_id ),
 CONSTRAINT fact_stateTimeSeries_date_fkey FOREIGN KEY ( "date" ) REFERENCES Dim_Date ( "date" )
);