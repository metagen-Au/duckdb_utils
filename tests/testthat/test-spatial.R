test_that("upload_data works with sf objects", {
  # Skip if sf package is not available
  skip_if_not_installed("sf")
  
  # Create a test connection
  con <- create_duckdb_connection()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Create a simple sf object
  if (requireNamespace("sf", quietly = TRUE)) {
    # Create point data
    points_df <- data.frame(
      id = 1:5,
      name = c("Point A", "Point B", "Point C", "Point D", "Point E"),
      x = c(-122.4, -118.2, -74.0, -87.6, -80.2),
      y = c(37.8, 34.0, 40.7, 41.9, 25.8)
    )
    
    # Convert to sf object
    points_sf <- sf::st_as_sf(
      points_df, 
      coords = c("x", "y"), 
      crs = 4326
    )
    
    # Test upload
    upload_data(
      data = points_sf,
      table_name = "spatial_test",
      con = con,
      create_if_missing = TRUE
    )
    
    # Check that table exists
    expect_true(table_exists(con, "spatial_test"))
    
    # Check data was uploaded correctly
    result <- DBI::dbGetQuery(con, "SELECT * FROM spatial_test")
    expect_equal(nrow(result), 5)
    expect_equal(result$id, 1:5)
    expect_equal(result$name, points_df$name)
    
    # Check geometry column (should be WKT)
    geom_col <- "geometry"  # Default name in sf
    expect_true(geom_col %in% names(result))
    expect_type(result[[geom_col]], "character")
    expect_true(all(grepl("^POINT", result[[geom_col]])))
  }
})

test_that("infer_schema handles geometry columns correctly", {
  # Skip if sf package is not available
  skip_if_not_installed("sf")
  
  if (requireNamespace("sf", quietly = TRUE)) {
    # Create point data
    points_df <- data.frame(
      id = 1:3,
      name = c("A", "B", "C"),
      x = c(-122.4, -118.2, -74.0),
      y = c(37.8, 34.0, 40.7)
    )
    
    # Convert to sf object
    points_sf <- sf::st_as_sf(
      points_df, 
      coords = c("x", "y"), 
      crs = 4326
    )
    
    # Get geometry column name
    geom_col <- attr(sf::st_geometry(points_sf), "sf_column")
    
    # Convert geometry to WKT
    points_sf[[geom_col]] <- sf::st_as_text(sf::st_geometry(points_sf))
    df_with_wkt <- sf::st_drop_geometry(points_sf)
    
    # Test infer_schema
    schema <- infer_schema(df_with_wkt, geom_col)
    
    # Check schema
    expect_type(schema, "character")
    expect_true(geom_col %in% names(schema))
    expect_equal(schema[[geom_col]], "VARCHAR")
  }
})
