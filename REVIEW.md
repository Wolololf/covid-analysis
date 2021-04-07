## To any reviewers

### Prerequisites
To run this project, you'll need to install PySpark: https://spark.apache.org/docs/latest/api/python/getting_started/install.html  
There are plenty of other guides out there as well.

To get this to work well with Jupyter Notebooks, you'll need to add

export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="lab"

to your .bashprofile (or .zshrc) so that the "pyspark" terminal command boots up a Jupyter Notebook.

### What to run
The key code cells are in the "4. Pipeline" notebook, where the cleaning and ETL pipeline scripts are run. "8. Example queries" has some test queries running on the gathered data.

### Specifications
I'm using a total of 4 data sources at the moment, most of which started out as .csv with one of them (area data) as .json:
* Covid-19 case data
* County health data
* County weather data
* County area data

Overall, the county facts table comes to ~1.3m rows of data, the state facts table only ~20k, and the dimension tables are tiny.

I've taken the liberty of modifying the suggested steps a little since my notebooks became too long and hard to read. I've split the ETL pipeline for each table into its own notebook to give each table enough room to be evaluated and conclusions drawn. As a consequence, I bumped the data quality checks to an extra step so I could validate the tables in a separate notebook. Similarly, I've broken up the project writeup into one notebook for documentation, and one for the data dictionary.  
Lastly, I've added a short notebook with example queries.