
#' Initialize Database Connection Pool
#' @param path Path to connection config CSV file (must include `dbpath`)
#' @param key_name Optional keyring name for MotherDuck token
#' @param pool_size Maximum number of connections in the pool
#' @param idle_timeout Time in seconds before idle connections are closed
#' @return DuckDB connection pool
#' @import DBI
#' @import duckdb
#' @import pool
#' @import keyring
#' @export
init_db_pool <- function(path, key_name = NULL, pool_size = 5, idle_timeout = 300) {
  if (is.null(path)) {
    stop("No path to config file provided.")
  }

  config <- tryCatch({
    read.csv(path, header = TRUE, stringsAsFactors = FALSE)
  }, error = function(e) {
    stop("Could not read config file: ", e$message)
  })

  dbdir <- config$dbpath
  if (is.null(dbdir)) stop("Missing 'dbpath' in config file")

  # Optionally register MotherDuck token
  if (!is.null(key_name)) {
    if (!any(keyring::key_list(key_name)$service == key_name)) {
      stop(sprintf("Key '%s' not found in system keyring", key_name))
    }
    token <- keyring::key_get(key_name)
    duckdb::duckdb_register_token(token)
  }

  pool <- pool::dbPool(
    drv = duckdb::duckdb(),
    dbdir = dbdir,
    read_only = FALSE,
    minSize = 1,
    maxSize = pool_size,
    idleTimeout = idle_timeout,
    validationInterval = 60
  )

  # Store pool in environment
  pool_env <- new.env()
  pool_env$pool <- pool
  assign("db_pool", pool_env, envir = .GlobalEnv)

  return(pool)
}


#' Store Database Credentials in System Keyring
#'
#' @param key_name Name to use for the key in the system keyring
#' @param password Password to store
#' @export
store_db_credentials <- function(key_name, password) {
  tryCatch({
    keyring::key_set_with_value(service = key_name, password = password)
    message("Credentials stored successfully in system keyring")
  }, error = function(e) {
    stop("Failed to store credentials: ", e$message)
  })
}

#' Get Connection from Pool
#'
#' @return A database connection from the pool
#' @export
get_pooled_connection <- function() {
  if (!exists("db_pool", envir = .GlobalEnv)) {
    stop("Database pool not initialized. Call init_db_pool first.")
  }
  
  pool <- get("db_pool", envir = .GlobalEnv)$pool
  return(pool::poolCheckout(pool))
}

#' Return Connection to Pool
#'
#' @param conn Connection to return to the pool
#' @export
return_connection <- function(conn) {
  pool::poolReturn(conn)
}

#' Close Database Connection Pool
#'
#' @export
close_db_pool <- function() {
  if (!exists("db_pool", envir = .GlobalEnv)) {
    return(invisible())
  }
  pool <- get("db_pool", envir = .GlobalEnv)$pool
  pool::poolClose(pool)
  rm("db_pool", envir = .GlobalEnv)
}
