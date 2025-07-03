test_that("create_duckdb_connection works", {
  # Test memory connection
  con <- create_duckdb_connection()
  expect_s4_class(con, "DBIConnection")
  expect_true(inherits(con, "duckdb_connection"))
  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("table_exists works", {
  con <- create_duckdb_connection()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # No tables should exist in a new memory connection
  expect_false(table_exists(con, "test_table"))
  
  # Create a test table
  DBI::dbWriteTable(con, "test_table", data.frame(a = 1:3, b = letters[1:3]))
  
  # Table should now exist
  expect_true(table_exists(con, "test_table"))
})

test_that("get_table_schema works", {
  con <- create_duckdb_connection()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Create a test table
  DBI::dbWriteTable(con, "test_table", data.frame(
    int_col = 1:3,
    char_col = letters[1:3],
    logical_col = c(TRUE, FALSE, TRUE)
  ))
  
  schema <- get_table_schema(con, "test_table")
  expect_s3_class(schema, "data.frame")
  expect_equal(nrow(schema), 3)
  expect_true(all(c("column_name", "data_type") %in% names(schema)))
})
