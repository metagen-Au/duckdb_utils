test_that("big_upload works with CSV files", {
  # Skip if running in non-interactive situations
  skip_on_ci()
  skip_on_cran()
  
  # Create a test connection
  con <- create_duckdb_connection()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Create a temporary CSV file
  temp_csv <- tempfile(fileext = ".csv")
  on.exit(unlink(temp_csv), add = TRUE)
  
  # Generate test data
  test_data <- data.frame(
    id = 1:100,
    name = replicate(100, paste0(sample(letters, 8, replace = TRUE), collapse = "")),
    value = rnorm(100),
    date = sample(seq(as.Date("2020-01-01"), as.Date("2022-12-31"), by = "day"), 100, replace = TRUE)
  )
  
  # Write to CSV
  write.csv(test_data, temp_csv, row.names = FALSE)
  
  # Test big_upload
  big_upload(
    file_path = temp_csv,
    table_name = "test_big_upload",
    con = con,
    create = TRUE
  )
  
  # Verify table exists
  expect_true(table_exists(con, "test_big_upload"))
  
  # Verify data was uploaded correctly
  result <- DBI::dbGetQuery(con, "SELECT * FROM test_big_upload ORDER BY id")
  expect_equal(nrow(result), 100)
  expect_equal(result$id, 1:100)
  expect_equal(result$value, test_data$value)
  
  # Test overwrite functionality
  new_data <- data.frame(
    id = 1:50,
    name = replicate(50, paste0(sample(letters, 8, replace = TRUE), collapse = "")),
    value = rnorm(50),
    date = sample(seq(as.Date("2023-01-01"), as.Date("2023-12-31"), by = "day"), 50, replace = TRUE)
  )
  
  # Write to CSV
  write.csv(new_data, temp_csv, row.names = FALSE)
  
  # Test overwrite
  big_upload(
    file_path = temp_csv,
    table_name = "test_big_upload",
    con = con,
    overwrite = TRUE
  )
  
  # Verify data was overwritten
  result <- DBI::dbGetQuery(con, "SELECT * FROM test_big_upload ORDER BY id")
  expect_equal(nrow(result), 50)
  expect_equal(result$id, 1:50)
})

test_that("big_upload handles file type errors", {
  # Create a test connection
  con <- create_duckdb_connection()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Create a temporary file with wrong extension
  temp_file <- tempfile(fileext = ".txt")
  on.exit(unlink(temp_file), add = TRUE)
  
  # Write some content
  writeLines("This is not a CSV or Parquet file", temp_file)
  
  # Test that it produces an error
  expect_error(
    big_upload(
      file_path = temp_file,
      table_name = "test_invalid_upload",
      con = con
    ),
    "Unsupported file type"
  )
})
