import duckdb

# Run a simple query and display the result
con = duckdb.connect("data/hr_analytics.duckdb")
duckdb.sql("SELECT * FROM pg_indexes").show()