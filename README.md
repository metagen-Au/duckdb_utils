# duckdb_utils

Utilities for working with DuckDB and MotherDuck in R

## Overview

`duckdb_utils` is an R package that provides a collection of utility functions for working with DuckDB and MotherDuck databases. It offers simplified connection handling, connection pooling, and data upload functions with support for spatial data.

## Features

* **Connection Management**: Easy connection to DuckDB and MotherDuck
* **Connection Pooling**: Efficient management of database connections
* **Data Upload**: Fast and reliable data upload with support for:
  * Standard data frames
  * Spatial data (via sf package)
  * Large CSV/Parquet files
  * Upsert operations

## Installation

```r
# Install from GitHub
# install.packages("devtools")
# devtools::install_github("yourusername/duckdb_utils")
```

## Usage

### Creating a Connection

```r
library(duckdb_utils)

# Connect to a local DuckDB database
con <- create_duckdb_connection("my_database.duckdb")

# Connect to MotherDuck
con <- create_duckdb_connection("md:my_warehouse", token = "your_token")
# Or using environment variable DUCKDB_MOTHERDUCK_TOKEN
```

### Using Connection Pooling

```r
# Initialize a connection pool
init_db_pool("config.csv", pool_size = 5)

# Get a connection from the pool
con <- get_pooled_connection()

# Return the connection to the pool when done
return_connection(con)

# Close the pool when your application shuts down
close_db_pool()
```

### Uploading Data

```r
# Upload a data frame
data <- data.frame(id = 1:10, value = letters[1:10])
upload_data(data, "my_table", con, create_if_missing = TRUE)

# Upload with upsert (update existing records based on key)
upload_data(data, "my_table", con, upsert_cols = "id")

# Upload spatial data
library(sf)
spatial_data <- st_read("my_shapefile.shp")
upload_data(spatial_data, "spatial_table", con, create_if_missing = TRUE)

# Upload large CSV files directly
big_upload("large_file.csv", "big_table", con)
```

## License

MIT
