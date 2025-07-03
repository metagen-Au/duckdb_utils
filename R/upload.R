#' Upload data to DuckDB with type checking and validation
#'
#' @param data Data frame or sf object to upload
#' @param table_name Target table name
#' @param con DuckDB connection
#' @param create_if_missing Create table if it doesn't exist
#' @param schema Optional named list of column types
#' @param upsert_cols Optional character vector of key columns for upsert
#' @param geom_col Optional name of geometry column (WKT assumed)
#' @import DBI
#' @import duckdb
#' @import sf
#' @export
upload_data <- function(data, 
                        table_name, 
                        con, 
                        create_if_missing = FALSE,
                        schema = NULL,
                        upsert_cols = NULL,
                        geom_col = NULL) {
  conn <- get_pooled_connection()
  on.exit(return_connection(conn))

  if (is.null(conn)) stop("Database connection required")

  is_spatial <- inherits(data, "sf") || !is.null(geom_col)

  if (is_spatial && !requireNamespace("sf", quietly = TRUE)) {
    stop("Package 'sf' is required for spatial data")
  }

  # Convert geometry to WKT if needed
  if (is_spatial) {
    if (is.null(geom_col)) {
      geom_col <- attr(sf::st_geometry(data), "sf_column")
    }
    data[[geom_col]] <- sf::st_as_text(sf::st_geometry(data))
    data <- sf::st_drop_geometry(data)
  }

  if (!table_exists(con, table_name)) {
    if (!create_if_missing) {
      stop(sprintf("Table '%s' does not exist. Set create_if_missing = TRUE to create it.", table_name))
    }
    if (is.null(schema)) {
      schema <- infer_schema(data, geom_col)
    }
    create_table(con, table_name, schema)
  }

  if (!is.null(upsert_cols)) {
    upsert_data(con, data, table_name, upsert_cols)
  } else {
    DBI::dbWriteTable(con, table_name, data, append = TRUE)
  }
}

#' Infer DuckDB schema
#' @param data Data frame
#' @param geom_col Optional geometry column name
#' @return Named list of column types
infer_schema <- function(data, geom_col = NULL) {
  types <- sapply(data, function(x) {
    if (is.numeric(x)) {
      if (all(x == floor(x), na.rm = TRUE)) "INTEGER" else "DOUBLE"
    } else if (is.character(x)) "VARCHAR"
    else if (inherits(x, "Date")) "DATE"
    else if (inherits(x, "POSIXt")) "TIMESTAMP"
    else if (is.logical(x)) "BOOLEAN"
    else "VARCHAR"
  })

  if (!is.null(geom_col) && geom_col %in% names(data)) {
    types[geom_col] <- "VARCHAR"  # store geometry as WKT
  }

  return(types)
}

#' Create table in DuckDB
#' @param con DuckDB connection
#' @param table_name Table name
#' @param schema Named list of types
create_table <- function(con, table_name, schema) {
  fields <- paste(names(schema), schema, collapse = ",\n  ")
  sql <- sprintf("CREATE TABLE %s (\n  %s\n)", table_name, fields)
  DBI::dbExecute(con, sql)
}

#' Upsert using DuckDB MERGE
#' @param con DuckDB connection
#' @param data Data frame
#' @param table_name Table name
#' @param key_cols Vector of key columns
upsert_data <- function(con, data, table_name, key_cols) {
  temp_table <- paste0("tmp_", table_name, "_", as.integer(Sys.time()))
  DBI::dbWriteTable(con, temp_table, data, temporary = TRUE)

  keys <- paste(sprintf("target.%s = source.%s", key_cols, key_cols), collapse = " AND ")
  update_cols <- setdiff(names(data), key_cols)
  update_expr <- paste(sprintf("%s = source.%s", update_cols, update_cols), collapse = ", ")

  sql <- sprintf(
    "MERGE INTO %s AS target
     USING %s AS source
     ON %s
     WHEN MATCHED THEN UPDATE SET %s
     WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
    table_name,
    temp_table,
    keys,
    update_expr,
    paste(names(data), collapse = ", "),
    paste(sprintf("source.%s", names(data)), collapse = ", ")
  )

  DBI::dbExecute(con, sql)
  DBI::dbExecute(con, sprintf("DROP TABLE %s", temp_table))
}

#' Big Upload for DuckDB / MotherDuck
#'
#' Uploads large CSV or Parquet file directly into DuckDB table using fast SQL-based ingestion.
#'
#' @param file_path Path to CSV or Parquet file (local or remote)
#' @param table_name Name of target DuckDB table
#' @param con DuckDB connection (defaults to pooled connection)
#' @param overwrite Logical. If TRUE, drops and recreates the table
#' @param create Logical. If TRUE, creates table if not exists (default TRUE)
#' @param ... Additional options passed to `read_csv_auto()` or `read_parquet()` (e.g. delim = ";")
#' @import DBI
#' @import duckdb
#' @import sf
#' @export
big_upload <- function(file_path,
                       table_name,
                       con = NULL,
                       overwrite = FALSE,
                       create = TRUE,
                       ...) {

  if (is.null(con)) {
    con <- get_pooled_connection()
    on.exit(return_connection(con))
  }

  # File format check
  file_ext <- tools::file_ext(file_path)
  is_csv <- file_ext %in% c("csv", "txt")
  is_parquet <- file_ext %in% c("parquet", "pq")

  if (!is_csv && !is_parquet) {
    stop("Unsupported file type. Must be .csv or .parquet")
  }

  # Drop table if overwrite = TRUE
  if (overwrite) {
    DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", table_name))
  }

  # Use read_csv_auto() or read_parquet() directly in DuckDB SQL
  sql_read <- if (is_csv) {
    sprintf("SELECT * FROM read_csv_auto('%s', AUTO_DETECT=TRUE, HEADER=TRUE)", file_path)
  } else {
    sprintf("SELECT * FROM read_parquet('%s')", file_path)
  }

  # Build full query to insert/create table
  if (overwrite || !table_exists(con, table_name)) {
    if (!create) stop("Table does not exist. Set `create = TRUE` or check table name.")
    query <- sprintf("CREATE TABLE %s AS %s", table_name, sql_read)
  } else {
    query <- sprintf("INSERT INTO %s %s", table_name, sql_read)
  }

  DBI::dbExecute(con, query)
  message(sprintf("Upload to '%s' from '%s' complete.", table_name, basename(file_path)))
}
