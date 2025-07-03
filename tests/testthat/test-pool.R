test_that("connection pool initialization works", {
  # Create a temporary config file
  config_file <- tempfile(fileext = ".csv")
  on.exit(unlink(config_file))
  
  # Write test config
  write.csv(
    data.frame(dbpath = ":memory:"),
    config_file,
    row.names = FALSE
  )
  
  # Initialize pool
  pool <- init_db_pool(config_file, pool_size = 2)
  
  # Test pool was created
  expect_true(exists("db_pool", envir = .GlobalEnv))
  expect_s3_class(pool, "Pool")
  
  # Clean up
  close_db_pool()
})

test_that("get_pooled_connection returns a valid connection", {
  skip_if_not_installed("pool")
  
  # Create a temporary config file
  config_file <- tempfile(fileext = ".csv")
  on.exit(unlink(config_file))
  
  # Write test config
  write.csv(
    data.frame(dbpath = ":memory:"),
    config_file,
    row.names = FALSE
  )
  
  # Initialize pool
  init_db_pool(config_file, pool_size = 2)
  on.exit(close_db_pool(), add = TRUE)
  
  # Get connection from pool
  con <- get_pooled_connection()
  
  # Test connection
  expect_s4_class(con, "DBIConnection")
  expect_true(inherits(con, "duckdb_connection"))
  
  # Test we can run SQL on it
  expect_true(DBI::dbIsValid(con))
  DBI::dbExecute(con, "CREATE TABLE test (id INTEGER)")
  expect_true(DBI::dbExistsTable(con, "test"))
  
  # Return connection to pool
  return_connection(con)
})

test_that("close_db_pool closes and removes pool", {
  # Create a temporary config file
  config_file <- tempfile(fileext = ".csv")
  on.exit(unlink(config_file))
  
  # Write test config
  write.csv(
    data.frame(dbpath = ":memory:"),
    config_file,
    row.names = FALSE
  )
  
  # Initialize pool
  init_db_pool(config_file, pool_size = 2)
  
  # Verify pool exists
  expect_true(exists("db_pool", envir = .GlobalEnv))
  
  # Close pool
  close_db_pool()
  
  # Verify pool is removed
  expect_false(exists("db_pool", envir = .GlobalEnv))
})
