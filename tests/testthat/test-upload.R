test_that("infer_schema works correctly", {
  # Create test data
  test_data <- data.frame(
    int_col = 1:3,
    dbl_col = c(1.1, 2.2, 3.3),
    char_col = c("a", "b", "c"),
    date_col = as.Date(c("2023-01-01", "2023-01-02", "2023-01-03")),
    logical_col = c(TRUE, FALSE, TRUE)
  )
  
  schema <- infer_schema(test_data)
  
  expect_type(schema, "character")
  expect_named(schema, names(test_data))
  expect_equal(schema[["int_col"]], "INTEGER")
  expect_equal(schema[["dbl_col"]], "DOUBLE")
  expect_equal(schema[["char_col"]], "VARCHAR")
  expect_equal(schema[["date_col"]], "DATE")
  expect_equal(schema[["logical_col"]], "BOOLEAN")
})

test_that("upload_data works with basic data frame", {
  con <- create_duckdb_connection()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Create test data
  test_data <- data.frame(
    id = 1:3,
    value = c("a", "b", "c")
  )
  
  # Upload data
  upload_data(test_data, "test_table", con, create_if_missing = TRUE)
  
  # Check table exists
  expect_true(table_exists(con, "test_table"))
  
  # Check data was inserted correctly
  result <- DBI::dbGetQuery(con, "SELECT * FROM test_table ORDER BY id")
  expect_equal(nrow(result), 3)
  expect_equal(result$id, 1:3)
  expect_equal(result$value, c("a", "b", "c"))
})
