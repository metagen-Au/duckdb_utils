# connection_pool_utils.R

#' @title DuckDB / MotherDuck Connection Pool Utilities
#' @description
#' Create and manage DuckDB (including MotherDuck) connection pools with token validation,
#' memoization, automatic cleanup, and safe read/write controls.
#' @import DBI
#' @import duckdb
#' @import pool
#' @import keyring
#' @export

# Package‐level cache for connection pools
.duckdb_pools <- new.env(parent = emptyenv())

#' Initialize or retrieve a memoized DuckDB/MotherDuck connection pool
#'
#' @param dbdir Path to local DuckDB file or MotherDuck URI (e.g. "md:warehouse")
#' @param read_only Logical; if TRUE, all pooled connections are opened read-only.
#' @param key_name Optional keyring service name for MotherDuck token
#' @param pool_size Maximum number of concurrent DB connections
#' @param idle_timeout Seconds before idle connections are closed
#' @return A `DBIConnectionPool` object
init_db_pool <- function(dbdir,
                         read_only    = FALSE,
                         key_name     = NULL,
                         pool_size    = 5,
                         idle_timeout = 300) {
  # Build cache key
  key <- paste(dbdir, read_only, key_name, pool_size, idle_timeout, sep = "|")
  if (exists(key, envir = .duckdb_pools)) {
    return(.duckdb_pools[[key]])
  }

  # Read MotherDuck token if requested
  if (!is.null(key_name)) {
    token <- tryCatch(
      keyring::key_get(service = key_name),
      error = function(e) stop("Failed to retrieve token from keyring: ", e$message)
    )
    if (nzchar(token) && grepl("^md:", dbdir)) {
      tryCatch(
        duckdb::duckdb_register_token(token),
        error = function(e) stop("Invalid MotherDuck token: ", e$message)
      )
    }
  }

  # Create the pool
  pool_obj <- pool::dbPool(
    drv               = duckdb::duckdb(),
    dbdir             = dbdir,
    read_only         = read_only,
    minSize           = 1,
    maxSize           = pool_size,
    idleTimeout       = idle_timeout,
    validationInterval = 60
  )

  # Finalizer to auto-close on session exit
  reg.finalizer(pool_obj, function(p) {
    if (inherits(p, "DBIConnectionPool")) {
      message("Closing DuckDB pool for: ", dbdir)
      pool::poolClose(p)
    }
  }, onexit = TRUE)

  # Memoize and return
  .duckdb_pools[[key]] <- pool_obj
  pool_obj
}

#' Store credentials into system keyring
#'
#' @param key_name Service name to store under
#' @param password Credential to store
#' @export
store_db_credentials <- function(key_name, password) {
  tryCatch({
    keyring::key_set_with_value(service = key_name, password = password)
    message("Stored credentials in keyring under service: ", key_name)
  }, error = function(e) {
    stop("Failed to store credentials: ", e$message)
  })
}

#' Checkout a connection from the default pool
#'
#' @return A `DBIConnection`
#' @export
get_pooled_connection <- function() {
  # Default to an in-memory pool if none initialized
  pool_obj <- if (length(ls(envir = .duckdb_pools)) == 0) {
    init_db_pool(":memory:")
  } else {
    # return the first available pool
    .duckdb_pools[[ls(envir = .duckdb_pools)[1]]]
  }
  pool::poolCheckout(pool_obj)
}

#' Return a connection to its pool
#'
#' @param conn A `DBIConnection` previously checked out
#' @export
return_connection <- function(conn) {
  if (inherits(conn, "DBIConnection")) {
    pool::poolReturn(conn)
  }
}

#' Close and clear all DuckDB connection pools
#'
#' @export
close_all_pools <- function() {
  for (key in ls(envir = .duckdb_pools)) {
    pool_obj <- .duckdb_pools[[key]]
    if (inherits(pool_obj, "DBIConnectionPool")) {
      message("Explicitly closing pool: ", key)
      pool::poolClose(pool_obj)
    }
    rm(list = key, envir = .duckdb_pools)
  }
  invisible(NULL)
}
