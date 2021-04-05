CREATE TABLE Dim_County
(
 fips                    integer NOT NULL,
 county_name             varchar(50) NOT NULL,
 state                   varchar(100) NOT NULL,
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
 CONSTRAINT county_pkey PRIMARY KEY ( fips )
);

CREATE TABLE Dim_Time
(
 "timestamp" integer NOT NULL,
 day         integer NOT NULL,
 week        integer NOT NULL,
 month       integer NOT NULL,
 "year"      integer NOT NULL,
 day_of_week integer NOT NULL,
 CONSTRAINT time_pkey PRIMARY KEY ( "timestamp" )
)
DISTSTYLE KEY DISTKEY ( "timestamp" )
INTERLEAVED SORTKEY ( "year", month, day );

CREATE TABLE Dim_State
(
 state                   varchar(50) NOT NULL,
 abbreviation            varchar(3) NOT NULL,
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
 CONSTRAINT state_pkey PRIMARY KEY ( state )
);

CREATE TABLE Fact_County
(
 "timestamp"       integer NOT NULL,
 county_id         varchar(5) NOT NULL,
 covid_case_total  integer NOT NULL,
 covid_case_delta  integer NOT NULL,
 covid_death_total integer NOT NULL,
 covid_death_delta integer NOT NULL,
 min_temp          decimal NOT NULL,
 max_temp          decimal NOT NULL,
 cloud_cover       decimal NOT NULL,
 wind              decimal NOT NULL,
 CONSTRAINT PK_Fact_County PRIMARY KEY ( "timestamp", fips ),
 CONSTRAINT FK_Fact_County_TimeId FOREIGN KEY ( "timestamp" ) REFERENCES Dim_Time ( "timestamp" ),
 CONSTRAINT FK_Fact_County_CountyId FOREIGN KEY ( fips ) REFERENCES Dim_County ( fips )
);

CREATE TABLE Fact_State
(
 "timestamp"       integer NOT NULL,
 state             varchar(100) NOT NULL,
 covid_case_total  integer NOT NULL,
 covid_case_delta  integer NOT NULL,
 covid_death_total integer NOT NULL,
 covid_death_delta integer NOT NULL,
 CONSTRAINT PK_Fact_State PRIMARY KEY ( "timestamp", state ),
 CONSTRAINT FK_Fact_State_TimeId FOREIGN KEY ( "timestamp" ) REFERENCES Dim_Time ( "timestamp" ),
 CONSTRAINT FK_Fact_State_StateId FOREIGN KEY ( state ) REFERENCES Dim_State ( state )
);