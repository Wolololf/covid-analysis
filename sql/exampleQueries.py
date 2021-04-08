class exampleQueries:
    highest_case_rates_by_county = ("""
    SELECT fc.fips, dc.county_name, max(fc.covid_case_total) as covid_case_total
        FROM fact_county fc
        LEFT JOIN dim_county dc
        ON fc.fips == dc.fips
        GROUP BY fc.fips, dc.county_name
        ORDER BY covid_case_total DESC
        LIMIT 10
    """)
    
    highest_death_rates_by_state = ("""
    SELECT fs.state, max(fs.covid_case_total) as covid_case_total
        FROM fact_state fs
        GROUP BY fs.state
        ORDER BY covid_case_total DESC
        LIMIT 10
    """)
    
    highest_normalised_case_rates_by_state = ("""
        WITH county_norm AS (
        SELECT fc.fips, fc.covid_case_total / dc.population as norm_case_total
        FROM fact_county fc
        LEFT JOIN dim_county dc
        ON fc.fips == dc.fips
        GROUP BY fc.fips, fc.covid_case_total, dc.population
    )
    SELECT fc.fips, dc.county_name, dc.state, max(cn.norm_case_total) as norm_case_total
        FROM fact_county fc
        LEFT JOIN dim_county dc
        ON fc.fips == dc.fips
        LEFT JOIN county_norm cn
        ON fc.fips == cn.fips
        GROUP BY fc.fips, dc.county_name, dc.state
        ORDER BY norm_case_total DESC
        LIMIT 10
    """)
    
    highest_normalised_death_rates_by_state = ("""
        WITH state_norm AS (
        SELECT fs.state, fs.covid_death_total / ds.population as norm_death_total
        FROM fact_state fs
        LEFT JOIN dim_state ds
        ON fs.state == ds.state
        GROUP BY fs.state, fs.covid_death_total, ds.population
    )
    SELECT fs.state, max(sn.norm_death_total) as norm_death_total
        FROM fact_state fs
        LEFT JOIN state_norm sn
        ON fs.state == sn.state
        GROUP BY fs.state
        ORDER BY norm_death_total DESC
        LIMIT 10
    """)
    
    largest_temperature_difference_by_county = ("""
    SELECT fc.fips, dc.county_name, dc.state, fc.max_temp - fc.min_temp as temp_change
        FROM fact_county fc
        LEFT JOIN dim_county dc
        ON fc.fips == dc.fips
        GROUP BY fc.fips, dc.county_name, dc.state, fc.max_temp, fc.min_temp
        ORDER BY temp_change DESC
        LIMIT 10
    """)
    
    largest_temperature_difference_by_county = ("""
    SELECT fc.fips, dc.county_name, dc.state, fc.max_temp - fc.min_temp as temp_change
        FROM fact_county fc
        LEFT JOIN dim_county dc
        ON fc.fips == dc.fips
        GROUP BY fc.fips, dc.county_name, dc.state, fc.max_temp, fc.min_temp
        ORDER BY temp_change DESC
        LIMIT 10
    """)
    
    county_normalised_cases_by_population_density_percentile = ("""
    WITH percentiles AS (
        SELECT dc.fips, PERCENT_RANK() OVER(
            ORDER BY dc.population_density ASC
        ) AS percent_rank
        FROM dim_county dc
    ),
    percent_buckets AS (
        SELECT p.fips, ROUND(p.percent_rank, 1) AS bucket
        FROM percentiles p
    ),
    max_cases AS (
        SELECT fc.fips, max(fc.covid_case_total / dc.population_density) as normalised_covid_cases
        FROM fact_county fc
        LEFT JOIN dim_county dc
        ON fc.fips == dc.fips
        GROUP BY fc.fips
    )
    SELECT pb.bucket, avg(mc.normalised_covid_cases) as average_normalised_cases
        FROM fact_county fc
        LEFT JOIN percent_buckets pb
        ON fc.fips == pb.fips
        LEFT JOIN max_cases mc
        ON fc.fips == mc.fips
        GROUP BY pb.bucket
        ORDER BY pb.bucket DESC
        LIMIT 10
    """)