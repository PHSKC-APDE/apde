#' @title Copy Data from the Data Lake to the Data Warehouse
#'
#' @description
#' This function copies data from the data lake to the data warehouse.
#'
#' @author Alastair Matheson, 2019-04-04
#'
#' @details
#' Plans for future improvements:
#' - Add warning when table is about to be overwritten.
#' - Add other options for things we're not using (e.g., file_format).
#'
#' @param conn SQL server connection created using \code{odbc} package
#' @param server server name, i.e., 'hhsaw' or 'phclaims'
#' @param config A object in memory with the YAML config file contents (should be blank if using config_url or config_file)
#' @param config_url The URL location of the YAML config file (should be blank if using config or config_file).
#' @param config_file The path and file name of the YAML config file (should be blank if using config or config_url).
#' @param to_schema schema name
#' @param to_table table name
#' @param db_name database name, e.g., "hhs_analytics_workspace", "inthealth_edw", etc.
#' @param dl_path The path to the data lake where the source files are located.
#' @param file_type file type, i.e., "csv", "parquet", or "orc".
#' @param identity The identity (username or account name) used for authentication when accessing the data lake.
#' @param secret The secret key or password associated with the identity for authentication.
#' @param max_errors The total number of records that can be rejected before entire file will be rejected by the system.
#' @param compression compression used, i.e., "none", "gzip", "defaultcodec", or "snappy".
#' @param field_quote The character used to quote fields in the input file (e.g., double quotes). Default is \code{field_quote = ""}
#' @param field_term The character or string used to separate fields in the input file (e.g., comma for CSV).
#' @param row_term The character or string used to separate rows in the input file (e.g., newline character).
#' @param first_row The row number where data begins in the input file (excluding headers if present).
#' @param overwrite Logical; if TRUE, truncate the table first before creating it, if it exists (default is \code{TRUE}).
#' @param rodbc Logical; if TRUE, use the RODBC package to run the query (avoids encoding error if using a secret key).
#' @param rodbc_dsn The DSN name of the RODBC connection to use with RODBC (only need to set if not using prod server).
#'
#' @return None
#' 
#' @note
#' Plans future improvement:
#' \itemize{
#'  \item Add warning when table is about to be overwritten
#'  \item Add in other options for things we're not using (e.g., file_format)
#' }
#' 
#' @importFrom DBI dbExecute dbExistsTable Id SQL
#' @importFrom RODBC odbcConnect sqlQuery odbcClose
#' @importFrom yaml yaml.load read_yaml
#' 
#' @export
#'
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
#'


#### FUNCTION ####
copy_into_f <- function(conn,
                        server = NULL,
                        config = NULL,
                        config_url = NULL,
                        config_file = NULL,
                        to_schema = NULL,
                        to_table = NULL,
                        db_name = NULL,
                        dl_path = NULL,
                        file_type = c("csv", "parquet", "orc"),
                        identity = NULL,
                        secret = NULL,
                        max_errors = 100,
                        compression = c("none", "gzip", "defaultcodec", "snappy"),
                        field_quote = "",
                        field_term = NULL,
                        row_term = NULL,
                        first_row = 2,
                        overwrite = T,
                        rodbc = F,
                        rodbc_dsn = "int_edw_16") {
  
  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  
  #### TEMPORARY FIX FOR ODBC ISSUES ####
  # The odbc package isn't encoding the secret key properly right now so produces
  # a Base-64 error. The RODBC doesn't seem to have that issue so for now we are
  # forcing the COPY INTO statement to use an RODBC connection
  if (rodbc == T) {
    conn_rodbc <- RODBC::odbcConnect(dsn = rodbc_dsn, 
                                     uid = keyring::key_list("hhsaw_dev")[["username"]])
  }
  
  
  #### INITIAL ERROR CHECK ####
  # Check if the config provided is a local object, file, or on a web page
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a local config object, config_url, or config_file but only one")
  }
  
  if (!is.null(config_url)) {
    message("Warning: YAML configs pulled from a URL are subject to fewer error checks")
  }
  
  if (!is.null(config_file)) {
    # Check that the yaml config file exists in the right format
    if (file.exists(config_file) == F) {
      stop("Config file does not exist, check file name")
    }
    
    if (configr::is.yaml.file(config_file) == F) {
      stop(glue::glue("Config file is not a YAML config file. ", 
                      "Check there are no duplicate variables listed"))
    }
  }
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  #### ERROR CHECKS AND OVERALL MESSAGES ####
  # Make sure a valid URL was found
  if ('404' %in% names(table_config)) {
    stop("Invalid URL for YAML file")
  }
  
  # Check that the yaml config file has necessary components
  if (!"vars" %in% names(table_config)) {
    stop("YAML file is missing a list of variables")
  } else {
    if (is.null(table_config$vars)) {
      stop("No variables specified in config file")
    }
  }
  
  # Check for issues with numeric values
  if (!is.numeric(max_errors)) {
    stop("max_errors must be numeric")
  }
  
  if (!is.numeric(first_row)) {
    stop("first_row must be numeric")
  }
  
  
  
  #### VARIABLES ####
  file_type <- match.arg(file_type)
  max_errors <- round(max_errors, 0)
  compression <- match.arg(compression)
  first_row <- round(first_row, 0)
  
  ## to_schema ----
  if (is.null(to_schema)) {
    if (!is.null(table_config[[server]][["to_schema"]])) {
      to_schema <- table_config[[server]][["to_schema"]]
    } else if (!is.null(table_config$to_schema)) {
      to_schema <- table_config$to_schema
    }
  }
  
  ## to_table ----
  if (is.null(to_table)) {
    if (!is.null(table_config[[server]][["to_table"]])) {
      to_table <- table_config[[server]][["to_table"]]
    } else if (!is.null(table_config$to_table)) {
      to_table <- table_config$to_table
    }
  }
  
  # dl_path ----
  if (is.null(dl_path)) {
    if (!is.null(table_config[[server]][["dl_path"]])) {
      dl_path <- table_config[[server]][["dl_path"]]
    } else if (!is.null(table_config$dl_path)) {
    dl_path <- table_config$dl_path
    }
  }
    
  ## db_name ----
  if (is.null(db_name)) {
    if (!is.null(table_config[[server]][["db_name"]])) {
      db_name <- table_config[[server]][["db_name"]]
    } else if (!is.null(table_config$db_name)) {
      db_name <- table_config$db_name
    }
  } 
  
  if (compression == "none") {
    compression <- DBI::SQL("")
  }
  
  if (rodbc == T & (!is.null(identity) | !is.null(secret))) {
    auth_sql <- glue::glue_sql("CREDENTIAL = (IDENTITY = {identity},
                               SECRET = {secret}),", .con = conn)
  } else {
    auth_sql <- DBI::SQL("")
  }
  
  #### RUN CODE ####
  if (overwrite == T) {
    message("Removing existing table and creating new one")
    # Need to drop and recreate so that the field types are what is desired
    if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
      DBI::dbExecute(conn,
                     glue::glue_sql("DROP TABLE {`to_schema`}.{`to_table`}",
                                    .con = conn))
    }
  }
  
  ### Create table if it doesn't exist
  if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table)) == F) {
    DBI::dbExecute(conn, glue::glue_sql(
      "CREATE TABLE {`to_schema`}.{`to_table`} (
          {DBI::SQL(glue_collapse(glue_sql('{`names(table_config$vars)`} {DBI::SQL(table_config$vars)}',
                                           .con = conn), sep = ', \n'))}
        )", .con = conn))
  }
  
  message(glue::glue("Creating [{to_schema}].[{to_table}] table"))
  
  # Set up SQL
  load_sql <- glue::glue_sql(
    "COPY INTO {`to_schema`}.{`to_table`} 
    ({`names(table_config$vars)`*})
    FROM {dl_path}
    WITH (
      FILE_TYPE = {file_type},
      {auth_sql}
      MAXERRORS = {max_errors},
      COMPRESSION = {compression},
      FIELDQUOTE = {field_quote},
      FIELDTERMINATOR = {field_term},
      ROWTERMINATOR = {row_term},
      FIRSTROW = {first_row}
    );",
    .con = conn)
  
  if (rodbc == T) {
    RODBC::sqlQuery(channel = conn_rodbc, query = load_sql)
    RODBC::odbcClose(conn_rodbc)
  } else {
    DBI::dbExecute(conn, load_sql)
  }
  
}
