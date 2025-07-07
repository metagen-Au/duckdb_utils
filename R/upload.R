#' @title DuckDB Upload Utilities with Validation, Transactions & Bulk Import
#' @description
#' Robust routines for uploading data frames, spatial tables, CSV/Parquet files,
#' with schema inference, upserts, transaction safety, and optimized COPY for large imports.
#' @import DBI
#' @import duckdb
#' @import dplyr
#' @import dbplyr
#' @import pool
#' @import keyring
#' @import sf
NULL

# Helper: check table existence on a pooled connection
table_exists <- function(conn, table_name) {
  DBI::dbExistsTable(conn, table_name)
}

#' Upload data to DuckDB with type checking, upsert, spatial support, and transaction safety
#'
#' @param data Data frame or sf object
#' @param table_name Target table name
#' @param conn Optional DBI connection or pool. If NULL, uses pooled connection from init_db_pool()
#' @param create_if_missing Create table if it doesn't exist (default FALSE)
#' @param schema Optional named list of DB types to override inference
#' @param upsert_cols Optional character vector of key columns for upsert
#' @param geom_col Optional geometry column name (WKT assumed)
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
    on.exit(return_connection(conn))
  }

  # Transaction wrapper
  DBI::dbWithTransaction(conn, {
    # Prepare data
    is_spatial <- inherits(data, "sf") || !is.null(geom_col)
    if (is_spatial) {
      if (!requireNamespace("sf", quietly = TRUE)) stop("sf required for spatial data")
      if (is.null(geom_col)) geom_col <- attr(sf::st_geometry(data), "sf_column")
      data[[geom_col]] <- sf::st_as_text(sf::st_geometry(data))
      data <- sf::st_drop_geometry(data)
    }

    # Create table if needed
    if (!table_exists(conn, table_name)) {
      if (!create_if_missing) stop(sprintf("Table '%s' not found. Set create_if_missing=TRUE.", table_name))
      inferred <- infer_schema(data, geom_col)
      final_schema <- if (is.null(schema)) inferred else modifyList(inferred, schema)
      create_table(conn, table_name, final_schema)
    }

    # Upsert or append
    if (!is.null(upsert_cols)) {
      upsert_data(conn, data, table_name, upsert_cols)
    } else {
      # Use copy_to for small-ish tables
      if (nrow(data) < 1e5) {
        dplyr::copy_to(conn, data, name = table_name, overwrite = FALSE, append = TRUE)
      } else {
        DBI::dbWriteTable(conn, table_name, data, append = TRUE)
      }
    }
  })
}

#' Infer DuckDB schema from an R data.frame
#'
#' @param data Data frame
#' @param geom_col Optional geometry column name
#' @return Named list of column types
infer_schema <- function(data, geom_col = NULL) {
  types <- vapply(data, function(x) {
    if (is.factor(x)) {
      "VARCHAR"
    } else if (is.integer(x)) {
      "INTEGER"
    } else if (is.numeric(x)) {
      if (all(x == floor(x), na.rm = TRUE)) "INTEGER" else "DOUBLE"
    } else if (is.character(x)) {
      # assume reasonable length
      "VARCHAR"
    } else if (inherits(x, "Date")) {
      "DATE"
    } else if (inherits(x, "POSIXt")) {
      "TIMESTAMP"
    } else if (is.logical(x)) {
      "BOOLEAN"
    } else {
      stop(sprintf("Column '%s' has unsupported type %s", deparse(substitute(x)), class(x)[1]))
    }
  }, character(1), USE.NAMES = TRUE)

  if (!is.null(geom_col) && geom_col %in% names(data)) {
    types[geom_col] <- "VARCHAR"
  }
  types
}

#' Create a table in DuckDB
#' @param con DBI connection
#' @param table_name Table name
#' @param schema Named list of column types
create_table <- function(con, table_name, schema) {
  fields <- paste(sprintf("%s %s", names(schema), schema), collapse = ", ")
  sql <- sprintf("CREATE TABLE %s (%s)", table_name, fields)
  DBI::dbExecute(con, sql)
}

#' Upsert using DuckDB MERGE
#' @param con DBI connection
#' @param data Data frame
#' @param table_name Table name
#' @param key_cols Vector of key columns
upsert_data <- function(con, data, table_name, key_cols) {
  temp <- paste0("tmp_", table_name, "_", as.integer(Sys.time()))
  DBI::dbWriteTable(con, temp, data, temporary = TRUE)

  keys <- paste(sprintf("target.%s = source.%s", key_cols, key_cols), collapse = " AND ")
  updates <- setdiff(names(data), key_cols)
  upd_expr <- paste(sprintf("%s = source.%s", updates, updates), collapse = ", ")

  sql <- sprintf(
    "MERGE %s AS target USING %s AS source ON %s
     WHEN MATCHED THEN UPDATE SET %s
     WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
    table_name, temp, keys,
    upd_expr,
    paste(names(data), collapse = ", "),
    paste(sprintf("source.%s", names(data)), collapse = ", ")
  )
  DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, sql)
    DBI::dbExecute(con, sprintf("DROP TABLE %s", temp))
  })
}

#' Big Upload for DuckDB using optimized COPY for large files
#'
#' @param file_path Path to CSV or Parquet file
#' @param table_name DuckDB table name
#' @param conn Optional DBI connection
#' @param overwrite Drop and recreate table if TRUE
#' @param create Create table if missing
#' @import DBI
#' @export
big_upload <- function(file_path,
                       table_name,
                       conn = NULL,
                       overwrite = FALSE,
                       create = TRUE) {
  if (is.null(conn)) {
    conn <- get_pooled_connection(); on.exit(return_connection(conn))
  }
  ext <- tolower(tools::file_ext(file_path))
  DBI::dbWithTransaction(conn, {
    if (overwrite) {
      DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", table_name))
    }
    if (!overwrite && !table_exists(conn, table_name) && !create) {
      stop("Table does not exist; set create=TRUE.")
    }
    # choose COPY for CSV, or Parquet SQL for parquet
    if (ext %in% c("csv","txt")) {
      # use parallel COPY
      sql <- sprintf("COPY %s FROM '%s' (AUTO_DETECT TRUE)", table_name, file_path)
    } else if (ext %in% c("parquet","pq")) {
      sql <- sprintf(
        if (overwrite || !table_exists(conn, table_name))
          "CREATE TABLE %s AS SELECT * FROM read_parquet('%s')"
        else
          "INSERT INTO %s SELECT * FROM read_parquet('%s')",
        table_name, file_path)
    } else {
      stop("Unsupported extension: ", ext)
    }

    if (ext %in% c("csv","txt")) {
      if (overwrite || !table_exists(conn, table_name)) {
        DBI::dbExecute(conn, sprintf("CREATE TABLE %s AS %s", table_name, sub("COPY", "SELECT * FROM read_csv_auto", sql)))
      } else {
        DBI::dbExecute(conn, sub("COPY", "INSERT INTO", sql))
      }
    } else {
      DBI::dbExecute(conn, sql)
    }
  })
  message(sprintf("Upload complete: %s", table_name))
}
