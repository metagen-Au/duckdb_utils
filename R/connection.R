
#' Create DuckDB / MotherDuck Connection
#'
#' @param dbdir Path to local DuckDB file, or a MotherDuck warehouse name (e.g. "md:my_warehouse")
#' @param read_only Logical, open DB read-only?
#' @param token Optional token for MotherDuck (defaults to `Sys.getenv("DUCKDB_MOTHERDUCK_TOKEN")`)
#' @return DBI connection object
#' @import duckdb
#' @import DBI
#' @export
create_duckdb_connection <- function(dbdir = ":memory:", read_only = FALSE, token = Sys.getenv("DUCKDB_MOTHERDUCK_TOKEN")) {
  if (grepl("^md:", dbdir)) {
    # Connect to MotherDuck using token
    if (nzchar(token)) {
      duckdb::duckdb_register_token(token)
    } else {
      warning("No MotherDuck token found in DUCKDB_MOTHERDUCK_TOKEN. Connection may fail.")
    }
  }

  con <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = dbdir,
    read_only = read_only
  )

  return(con)
}

#' Check if Table Exists
#'
#' @param con Database connection
#' @param table_name Table to check
#' @return TRUE if table exists, else FALSE
#' @import DBI
#' @export
table_exists <- function(con, table_name) {
  DBI::dbExistsTable(con, table_name)
}

#' Get Table Schema
#'
#' @param con Database connection
#' @param table_name Table name
#' @return Data frame of column names and types
#' @import DBI
#' @export
get_table_schema <- function(con, table_name) {
  if (!table_exists(con, table_name)) {
    stop(sprintf("Table '%s' does not exist", table_name))
  }

  query <- sprintf("PRAGMA table_info(%s)", table_name)
  result <- DBI::dbGetQuery(con, query)

  # Normalize output to match expectation
  schema <- data.frame(
    column_name = result$name,
    data_type = result$type,
    stringsAsFactors = FALSE
  )

  return(schema)
}
