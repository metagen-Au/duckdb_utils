#' @title DuckDB Upload Utilities with Validation, Transactions & Bulk Import
#' @description
#' Robust routines for uploading data frames, spatial tables, CSV/Parquet files,
#' with schema inference, upserts, transaction safety, optimized COPY for large imports,
#' proper identifier quoting, read-only enforcement, and cleanup.
#' @import DBI
#' @import duckdb
#' @import dplyr
#' @import dbplyr
#' @import pool
#' @import keyring
#' @import sf
NULL

# Helper: safely quote an identifier
db_ident <- function(con, x) {
  DBI::dbQuoteIdentifier(con, x)
}

# Helper: safely quote a string literal
db_str <- function(con, x) {
  DBI::dbQuoteString(con, x)
}

# Check table existence
table_exists <- function(con, table_name) {
  DBI::dbExistsTable(con, table_name)
}

#' Upload data with transaction, validation, upsert, spatial support, and read-only enforcement
#'
#' @param data Data frame or sf object
#' @param table_name Target table name (unquoted)
#' @param conn Optional DBIConnection or DBIConnectionPool; if NULL, defaults to pooled connection
#' @param create_if_missing Logical; create table if missing
#' @param schema Named list of DB types to override inference
#' @param upsert_cols Character vector of key columns for upsert
#' @param geom_col Geometry column name for sf objects (WKT assumed)
#' @export
upload_data <- function(data,
                        table_name,
                        conn = NULL,
                        create_if_missing = FALSE,
                        schema = NULL,
                        upsert_cols = NULL,
                        geom_col = NULL) {
  # Acquire connection
  if (is.null(conn)) {
    conn <- get_pooled_connection()
    on.exit(return_connection(conn), add = TRUE)
  }
  # Enforce read-only flag
  info <- DBI::dbGetInfo(conn)
  if (!is.null(info$read_only) && info$read_only) {
    stop("Cannot write to a read-only DuckDB connection/pool.")
  }

  DBI::dbWithTransaction(conn, {
    # Prepare spatial data
    if (inherits(data, "sf") || (!is.null(geom_col) && geom_col %in% names(data))) {
      if (!requireNamespace("sf", quietly = TRUE)) {
        stop("Package 'sf' required for spatial uploads. Install with install.packages('sf').")
      }
      if (inherits(data, "sf")) {
        geom_col <- attr(sf::st_geometry(data), "sf_column")
      }
      if (!geom_col %in% names(data)) {
        stop(sprintf("Geometry column '%s' not found in data.", geom_col))
      }
      data[[geom_col]] <- sf::st_as_text(sf::st_geometry(data))
      data <- sf::st_drop_geometry(data)
    }

    # Create table if needed
    q_table <- db_ident(conn, table_name)
    if (!table_exists(conn, table_name)) {
      if (!create_if_missing) {
        stop(sprintf("Table '%s' does not exist. Set create_if_missing=TRUE.", table_name))
      }
      inferred <- infer_schema(data, geom_col)
      types <- if (is.null(schema)) inferred else modifyList(inferred, schema)
      create_table(conn, table_name, types)
    }

    # Upsert vs append
    if (!is.null(upsert_cols)) {
      upsert_data(conn, data, table_name, upsert_cols)
    } else {
      # Small in-memory: use copy_to
      if (nrow(data) < 1e5) {
        dplyr::copy_to(conn, data, name = table_name, overwrite = FALSE, append = TRUE)
      } else {
        DBI::dbAppendTable(conn, table_name, data)
      }
    }
  })
}

#' Infer DuckDB schema from an R data.frame
#'
#' @param data Data frame
#' @param geom_col Geometry column name
#' @return Named list of DB types
infer_schema <- function(data, geom_col = NULL) {
  types <- vapply(names(data), function(col_name) {
    x <- data[[col_name]]
    if (is.factor(x)) {
      "VARCHAR"
    } else if (is.integer(x)) {
      "INTEGER"
    } else if (is.numeric(x)) {
      if (all(x == floor(x), na.rm = TRUE)) "INTEGER" else "DOUBLE"
    } else if (is.character(x)) {
      "VARCHAR"
    } else if (inherits(x, "Date")) {
      "DATE"
    } else if (inherits(x, "POSIXt")) {
      "TIMESTAMP"
    } else if (is.logical(x)) {
      "BOOLEAN"
    } else {
      stop(sprintf("Unsupported column type '%s' for '%s'. Please coerce before upload.", class(x)[1], col_name))
    }
  }, character(1), USE.NAMES = TRUE)

  if (!is.null(geom_col) && geom_col %in% names(data)) {
    types[[geom_col]] <- "VARCHAR"
  }
  types
}

#' Create a DuckDB table with quoted identifiers
#' @param conn DBIConnection
#' @param table_name Unquoted table name
#' @param schema Named list of column types
create_table <- function(conn, table_name, schema) {
  q_table <- db_ident(conn, table_name)
  fields <- paste(sprintf("%s %s", names(schema), schema), collapse = ", ")
  sql <- sprintf("CREATE TABLE %s (%s)", q_table, fields)
  message("Executing: ", sql)
  DBI::dbExecute(conn, sql)
}

#' Upsert using DuckDB MERGE with cleanup and quoting
#' @param conn DBIConnection
#' @param data Data frame
#' @param table_name Unquoted target table name
#' @param key_cols Character vector of key columns
#' @export
upsert_data <- function(conn, data, table_name, key_cols) {
  if (length(key_cols) < 1 || !all(key_cols %in% names(data))) {
    stop("Provide at least one valid key column present in 'data'.")
  }
  temp <- paste0("tmp_", table_name, "_", as.integer(Sys.time()))
  q_temp <- db_ident(conn, temp)
  on.exit({ DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", q_temp)) }, add = TRUE)

  message(sprintf("Writing to temp table %s...", temp))
  DBI::dbWriteTable(conn, temp, data, temporary = TRUE, overwrite = TRUE)

  # Build MERGE
  q_table <- db_ident(conn, table_name)
  condition <- paste(sprintf("target.%s = source.%s", key_cols, key_cols), collapse = " AND ")
  updates <- setdiff(names(data), key_cols)
  upd_expr <- if (length(updates) == 0) "NOTHING" else paste(sprintf("%s = source.%s", updates, updates), collapse = ", ")
  cols <- DBI::dbQuoteIdentifier(conn, names(data))
  vals <- paste(sprintf("source.%s", names(data)), collapse = ", ")

  sql <- sprintf(
    "MERGE INTO %s AS target USING %s AS source ON %s
     WHEN MATCHED THEN UPDATE SET %s
     WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
    q_table, q_temp, condition, upd_expr,
    paste(cols, collapse = ", "), vals
  )
  message("Executing MERGE...")
  DBI::dbExecute(conn, sql)
}

#' Bulk upload CSV/Parquet via COPY or SQL, with read-only enforcement, quoting, and transaction
#' @param file_path Path to CSV or Parquet file
#' @param table_name Unquoted target table name
#' @param conn DBIConnection or pool; if NULL uses pooled
#' @param overwrite Logical; drop existing table if TRUE
#' @param create Logical; create table if missing
#' @export
big_upload <- function(file_path,
                       table_name,
                       conn = NULL,
                       overwrite = FALSE,
                       create = TRUE) {
  if (is.null(conn)) {
    conn <- get_pooled_connection(); on.exit(return_connection(conn), add = TRUE)
  }
  # enforce read-only
  info <- DBI::dbGetInfo(conn)
  if (!is.null(info$read_only) && info$read_only) stop("Cannot write via big_upload on a read-only connection.")
  if (!file.exists(file_path)) stop(sprintf("File not found: %s", file_path))
  ext <- tolower(tools::file_ext(file_path))
  q_table <- db_ident(conn, table_name)
  q_path  <- db_str(conn, file_path)

  DBI::dbWithTransaction(conn, {
    exists <- table_exists(conn, table_name)
    if (overwrite && exists) {
      message("Dropping existing table...")
      DBI::dbExecute(conn, sprintf("DROP TABLE %s", q_table))
      exists <- FALSE
    }
    if (!exists && !create) stop("Table missing and create=FALSE.")

    if (ext %in% c("csv", "txt")) {
      # Use COPY for CSV
      sql <- sprintf("COPY %s FROM %s (AUTO_DETECT TRUE, HEADER TRUE)", q_table, q_path)
      action <- if (!exists && create) "Creating and loading via COPY..." else "Appending via COPY..."
      message(action)
    } else if (ext %in% c("parquet", "pq")) {
      if (!exists && create) {
        sql <- sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet(%s)", q_table, q_path)
        message("Creating from Parquet...")
      } else {
        sql <- sprintf("INSERT INTO %s SELECT * FROM read_parquet(%s)", q_table, q_path)
        message("Appending from Parquet...")
      }
    } else {
      stop("Unsupported file type: ", ext)
    }
    message("Executing bulk upload SQL...")
    DBI::dbExecute(conn, sql)
  })
  message(sprintf("Completed bulk upload to %s", table_name))
}
