#' Create a DuckDB or MotherDuck connection with memoization, token checking, and cleanup
#'
#' @param dbdir Path to DuckDB file or MotherDuck URI (e.g. "md:my_warehouse")
#' @param read_only Logical; if TRUE, the connection is opened read-only
#' @param token MotherDuck token (defaults to Sys.getenv("DUCKDB_MOTHERDUCK_TOKEN")); NULL to skip
#' @return A memoized DBIConnection
#' @import DBI
#' @import duckdb
#' @export
create_duckdb_connection <- local({
  cache <- new.env(parent = emptyenv())
  function(dbdir     = ":memory:",
           read_only = FALSE,
           token     = Sys.getenv("DUCKDB_MOTHERDUCK_TOKEN")) {

    key <- paste(dbdir, read_only, token, sep = "|")
    if (exists(key, envir = cache)) {
      con <- cache[[key]]
      if (DBI::dbIsValid(con)) return(con)
    }

    # MotherDuck URIs require a non‐empty token
    if (grepl("^md:", dbdir)) {
      if (!nzchar(token)) {
        stop("A valid MotherDuck token is required when dbdir starts with 'md:'.")
      }
      tryCatch(
        duckdb::duckdb_register_token(token),
        error = function(e) stop("Invalid MotherDuck token: ", e$message)
      )
    }

    # Attempt to connect, with clear failure message
    con <- tryCatch(
      DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir, read_only = read_only),
      error = function(e) stop("Failed to connect to DuckDB at '", dbdir, "': ", e$message)
    )

    # Ensure we clean up the connection at session end or GC
    reg.finalizer(con, function(ptr) {
      if (DBI::dbIsValid(ptr)) {
        DBI::dbDisconnect(ptr)
      }
    }, onexit = TRUE)

    cache[[key]] <- con
    con
  }
})

#' Check if a table exists
#'
#' @param con A DBIConnection
#' @param table_name Unquoted table name
#' @return Logical; TRUE if the table exists
#' @import DBI
#' @export
table_exists <- function(con, table_name) {
  DBI::dbExistsTable(con, table_name)
}

#' Get detailed schema for a DuckDB table
#'
#' @param con A DBIConnection
#' @param table_name Unquoted table name
#' @return A tibble with columns: column, type, not_null, default, is_pk
#' @import DBI
#' @import tibble
#' @export
get_table_schema <- function(con, table_name) {
  qname <- DBI::dbQuoteIdentifier(con, table_name)
  info  <- DBI::dbGetQuery(con,
            sprintf("PRAGMA table_info(%s)", qname)
         )
  if (nrow(info) == 0) {
    stop("Table '", table_name, "' does not exist or has no columns")
  }

  tibble::tibble(
    column    = info$name,
    type      = info$type,
    not_null  = as.logical(info$notnull),
    default   = info$dflt_value,
    is_pk     = info$pk == 1
  )
}
