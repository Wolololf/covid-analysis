class exampleQueries:
    highest_case_rates_by_county = ("""
        SELECT fc.fips, fc.timestamp, dc.county_name, max(fc.covid_case_total) as covid_case_total
        FROM fact_county fc
        LEFT JOIN dim_county dc
        ON fc.fips == dc.fips
        GROUP BY fc.fips, fc.timestamp, dc.county_name
        ORDER BY covid_case_total DESC
        LIMIT 10
    """)