# table_duplicate_f() ----
#' @title copy a (smaller) SQL table from server to another 
#' @description Made for smaller table, e.g., reference tables
#' @author Jeremy Whitehurst, 2024-05-01
#' 
#' @param conn_from Name of the connection to the FROM SQL database
#' @param conn_to Name of the connection to the TO SQL database
#' @param server_to Name of the server/odbc for the TO SQL database. 
#' ***IMPORTANT*** For any Azure based duplication, there must be a keyring with 
#' a service name that matches the server_to parameter
#' @param db_to Name of the TO SQL database
#' @param table_df data.frame that can hold a list of FROM schema, FROM tables 
#' (required), TO schema, and TO tables
#' @param from_schema Variable for FROM schema for either a single table or can 
#' populate any missing data in the table_df from_schema column
#' @param from_table Variable for FROM table for duplicating a single table
#' @param to_schema Variable for TO schema for either a single table or can populate 
#' any missing data in the table_df to_schema column, can be blank and use the 
#' from_schema on TO database
#' @param to_table Variable for TO table for either a single table or can populate 
#' any missing data in the table_df to_table column, can be blank and use the 
#' from_table on TO database
#' @param to_table_prefix Variable of a prefix to be added before the TO table name
#' @param confirm_tables If \code{TRUE}, will require user confirm the list of 
#' tables being duplicated
#' @param delete_table If \code{TRUE}, will delete old TO tables, if \code{FALSE}, 
#' will rename old TO tables
#' @param delete_table_suffix Variable of a suffix to be added after the name of 
#' old TO tables
#' @param table_structure_only If \code{TRUE}, will only copy the table structure
#' of old TO new tables
#' 
#' @details
#' Plans for future improvement ... 
#' 
#' @importFrom utils askYesNo
#' 
#' @return None
#' 
#' @export
#' 
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
#'  

table_duplicate_f <- function(conn_from,
                              conn_to,
                              server_to,
                              db_to,
                              table_df = data.frame(),
                              from_schema = NULL,
                              from_table = NULL,
                              to_schema = NULL,
                              to_table = NULL,
                              to_table_prefix = NULL,
                              confirm_tables = T,
                              delete_table = F,
                              delete_table_suffix = "_dupe_table_to_delete",
                              table_structure_only = F
                              ) {
  if(nrow(table_df) == 0) {
    # Check if table_df is empty. If it is, all from/to variables must be set.
    if(is.null(from_schema) || is.null(from_table)) {
      stop("If the data.frame, table_df, is empty, the variables from_schema and from_table must have values!")
    }
    if(is.null(to_schema)) { 
      to_schema = from_schema
    }
    if(is.null(to_table)) { 
      to_table = from_table
    }
    # Populate row 1 of table_df with from/to variables.
    table_df[1, "from_schema"] <- from_schema
    table_df[1, "from_table"] <- from_table
    table_df[1, "to_schema"] <- to_schema
    table_df[1, "to_table"] <- to_table
  } else {
    # Check that either table_df has the from/to columns OR the corresponding from/to variable is set.
    if(!"from_schema" %in% colnames(table_df) && is.null(from_schema)) {
      stop("If the data.frame, table_df, is missing the from_schema column, the from_schema variable must have a value!")
    }
    if(!"from_table" %in% colnames(table_df) && is.null(from_table)) {
      stop("If the data.frame, table_df, is missing the from_table column, the from_table variable must have a value!")
    }
#    if(!"to_schema" %in% colnames(table_df) && is.null(to_schema)) {
#      stop("If the data.frame, table_df, is missing the to_schema column, the to_schema variable must have a value!")
#    }
#    if(!"to_table" %in% colnames(table_df) && is.null(to_table)) {
#      stop("If the data.frame, table_df, is missing the to_table column, the to_table variable must have a value!")
#    }
  }
  
  # Populating any missing values in table_df and adding the to_table_prefix
  for(i in 1:nrow(table_df)) {
    if(!"from_schema" %in% colnames(table_df) || is.null(table_df[i, "from_schema"])) {
      table_df[i, "from_schema"] <- from_schema
    }
    if(!"from_table" %in% colnames(table_df) || is.null(table_df[i, "from_table"])) {
      table_df[i, "from_table"] <- from_table
    }
    if(!"to_schema" %in% colnames(table_df) || is.na(table_df[i, "to_schema"])) {
      if(!is.null(to_schema)) {
        table_df[i, "to_schema"] <- to_schema
      } else {
        table_df[i, "to_schema"] <- table_df[i, "from_schema"]
      }
    }
    if(!"to_table" %in% colnames(table_df) || is.na(table_df[i, "to_table"])) {
      if(!is.null(to_table)) {
        table_df[i, "to_table"] <- to_table
      } else {
        table_df[i, "to_table"] <- table_df[i, "from_table"]
      }
    }
    if(!is.null(to_table_prefix)) {
      table_df[i, "to_table"] <- paste0(to_table_prefix, table_df[i, "to_table"])
    }
  }
    
  # List tables to duplicate.
  message(glue::glue("Ready to duplicate the following {nrow(table_df)} table(s):"))
  for(i in 1:nrow(table_df)) {
    message(glue::glue("{i}: [{table_df[i, 'from_schema']}].[{table_df[i, 'from_table']}] -> [{table_df[i, 'to_schema']}].[{table_df[i, 'to_table']}]"))
  }
  
  # If confirm_tables == T, user must confirm the tables to duplicate.
  if(confirm_tables == T) {
    confirm <- askYesNo("Duplicate tables?")
    if(confirm == F || is.na(confirm)) {
      stop("Table duplication cancelled.")
    }
  }
  
  message("Begin duplicating tables...")
  if(table_structure_only == F) {
    for(i in 1:nrow(table_df)) {
      message(glue::glue("Table {i}: [{table_df[i, 'from_schema']}].[{table_df[i, 'from_table']}] -> [{table_df[i, 'to_schema']}].[{table_df[i, 'to_table']}]"))
      message("Pulling data from source table...")
      data_from <- DBI::dbGetQuery(conn_from, 
                                  glue::glue_sql("SELECT * FROM {`table_df[i, 'from_schema']`}.{`table_df[i, 'from_table']`}",
                                                  .con = conn_from))
      message("Checking if destination table exists...")
      if(DBI::dbExistsTable(conn_to, name = DBI::Id(schema = table_df[i, "to_schema"], table = table_df[i, "to_table"])) == T) {
        message("Destination table exists. Pulling data from destination table to compare with source table...")
        data_to <- DBI::dbGetQuery(conn_to, 
                                   glue::glue_sql("SELECT * FROM {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}",
                                                  .con = conn_to))
        if(nrow(data_to) == 0) {
          table_match <- F
        } else {
          suppressWarnings(table_match <- dplyr::all_equal(data_from, data_to, ignore_col_order = F))
        }
        if(table_match == T) {
          message("Destination table matches source table...")
          next
        } else {
          message("Destination table does not match source table...")
          if(delete_table == T) {
            message("Deleting old destination table...")
            DBI::dbExecute(conn_to,
                          glue::glue_sql("DROP TABLE {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}",
                                          .con = conn_to))
          } else {
            dts <- delete_table_suffix
            dts_num <- 0
            # Checks if there is already a "to_delete" table with the same name. If so, keep adding a number to the end until you are renaming to a new table.
            while(DBI::dbExistsTable(conn_to, name = DBI::Id(schema = table_df[i, "to_schema"], table = paste0(table_df[i, "to_table"], dts))) == T) {
              dts_num <- dts_num + 1
              dts <- paste0(delete_table_suffix, "_", dts_num)
            }
            message(glue::glue("Renaming old destination table to [{table_df[i, 'to_schema']}].[{paste0(table_df[i, 'to_table'], dts)}]..."))
            # Attempts to rename table with sytax for standard databases. If that fails, rename table with syntax that is used in an Azure Synapse environment.
            tryCatch(
              {
                DBI::dbExecute(conn_to,
                              glue::glue_sql("EXEC sp_rename {paste0(table_df[i, 'to_schema'], '.', table_df[i, 'to_table'])}, {paste0(table_df[i, 'to_table'], dts)}",
                                              .con = conn_to))
              },
              error = function(cond) {
                DBI::dbExecute(conn_to,
                              glue::glue_sql("RENAME OBJECT {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`} TO {`{paste0(table_df[i, 'to_table'], dts)}`}",
                                              .con = conn_to))    
              }
            )
          }
        }
      } else {
        message("Destination table does not exist...")
      }
      cols_from <- DBI::dbGetQuery(conn_from,
                                    glue::glue_sql("
                                                  SELECT 
                                                  [COLUMN_NAME],
                                                  [DATA_TYPE],
                                                  [CHARACTER_MAXIMUM_LENGTH],
                                                  [NUMERIC_PRECISION],
                                                  [NUMERIC_SCALE],
                                                  [CHARACTER_SET_NAME],
                                                  [COLLATION_NAME],
                                                  CONCAT(
                                                    '[', [COLUMN_NAME], '] ', 
                                                    UPPER([DATA_TYPE]), 
	                                                  CASE
                                                  		WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',CASE
                                                  		                                                                    WHEN [CHARACTER_MAXIMUM_LENGTH] = -1 THEN 'MAX'
                                                  		                                                                    ELSE CAST([CHARACTER_MAXIMUM_LENGTH] AS VARCHAR(4))
                                                  		                                                                  END
                                                  		                                                                  , ') COLLATE ', [COLLATION_NAME])
                                                  		WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
                                                  		ELSE ''
                                                  	END,
	                                                  ' NULL') AS 'COLUMN_DEFINITION'
                                                FROM [INFORMATION_SCHEMA].[COLUMNS]
                                                WHERE 
                                                  [TABLE_NAME] = {table_df[i, 'from_table']}
                                                  AND [TABLE_SCHEMA] = {table_df[i, 'from_schema']}
                                                ORDER BY [ORDINAL_POSITION]",
                                                   .con = conn_from))
      message("Creating destination table...")
      create_code <- glue::glue_sql(
        "CREATE TABLE {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}
        ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{DBI::SQL(cols_from$COLUMN_DEFINITION)}',.con = conn_to), sep = ', \n  '))})", 
        .con = conn_to)
    
      DBI::dbExecute(conn_to, create_code)
      message("Copying source data to destination table...")
      data_from <- dplyr::mutate_all(data_from, as.character)
      if(length(keyring::key_list(server_to)[["username"]]) == 0) {
        u <- NULL
        p <- NULL
      } else {
        u <- keyring::key_list(server_to)[["username"]]
        p <- keyring::key_get(server_to, keyring::key_list(server_to)[["username"]])
      }
      load_df_bcp_f(dataset = data_from,
                server = server_to,
                db_name = db_to,
                schema_name = table_df[i, "to_schema"],
                table_name = table_df[i, "to_table"],
                user = u,
                pass = p)
      message("Table duplication complete...")
    }
  } else {
    message(glue::glue("Table {i}: [{table_df[i, 'from_schema']}].[{table_df[i, 'from_table']}] -> [{table_df[i, 'to_schema']}].[{table_df[i, 'to_table']}]"))
    message("Checking if destination table exists...")
    if(DBI::dbExistsTable(conn_to, name = DBI::Id(schema = table_df[i, "to_schema"], table = table_df[i, "to_table"])) == T) {
      if(delete_table == T) {
        message("Deleting old destination table...")
        DBI::dbExecute(conn_to,
                       glue::glue_sql("DROP TABLE {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}",
                                      .con = conn_to))
      } else {
        dts <- delete_table_suffix
        dts_num <- 0
        # Checks if there is already a "to_delete" table with the same name. If so, keep adding a number to the end until you are renaming to a new table.
        while(DBI::dbExistsTable(conn_to, name = DBI::Id(schema = table_df[i, "to_schema"], table = paste0(table_df[i, "to_table"], dts))) == T) {
          dts_num <- dts_num + 1
          dts <- paste0(delete_table_suffix, "_", dts_num)
        }
        message(glue::glue("Renaming old destination table to [{table_df[i, 'to_schema']}].[{paste0(table_df[i, 'to_table'], dts)}]..."))
        # Attempts to rename table with sytax for standard databases. If that fails, rename table with syntax that is used in an Azure Synapse environment.
        tryCatch(
          {
            DBI::dbExecute(conn_to,
                           glue::glue_sql("EXEC sp_rename {paste0(table_df[i, 'to_schema'], '.', table_df[i, 'to_table'])}, {paste0(table_df[i, 'to_table'], dts)}",
                                          .con = conn_to))
          },
          error = function(cond) {
            DBI::dbExecute(conn_to,
                           glue::glue_sql("RENAME OBJECT {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`} TO {`{paste0(table_df[i, 'to_table'], dts)}`}",
                                          .con = conn_to))    
          }
        )
      }
    } else {
      message("Destination table does not exist...")
    }
    cols_from <- DBI::dbGetQuery(conn_from,
                               glue::glue_sql("
                                                  SELECT 
                                                  [COLUMN_NAME],
                                                  [DATA_TYPE],
                                                  [CHARACTER_MAXIMUM_LENGTH],
                                                  [NUMERIC_PRECISION],
                                                  [NUMERIC_SCALE],
                                                  [CHARACTER_SET_NAME],
                                                  [COLLATION_NAME],
                                                  CONCAT(
                                                    '[', [COLUMN_NAME], '] ', 
                                                    UPPER([DATA_TYPE]), 
	                                                  CASE
                                                  		WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',CASE
                                                  		                                                                    WHEN [CHARACTER_MAXIMUM_LENGTH] = -1 THEN 'MAX'
                                                  		                                                                    ELSE CAST([CHARACTER_MAXIMUM_LENGTH] AS VARCHAR(4))
                                                  		                                                                  END
                                                  		                                                                  , ') COLLATE ', [COLLATION_NAME])
                                                  		WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
                                                  		ELSE ''
                                                  	END,
	                                                  ' NULL') AS 'COLUMN_DEFINITION'
                                                FROM [INFORMATION_SCHEMA].[COLUMNS]
                                                WHERE 
                                                  [TABLE_NAME] = {table_df[i, 'from_table']}
                                                  AND [TABLE_SCHEMA] = {table_df[i, 'from_schema']}
                                                ORDER BY [ORDINAL_POSITION]",
                                              .con = conn_from))
    message("Creating destination table...")
    create_code <- glue::glue_sql(
      "CREATE TABLE {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}
        ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{DBI::SQL(cols_from$COLUMN_DEFINITION)}',.con = conn_to), sep = ', \n  '))})", 
      .con = conn_to)
  
    DBI::dbExecute(conn_to, create_code)
  }
  message("All tables duplicated successfully.")
}



#### PARAMETERS ####
# conn = name of the connection to the SQL database
# delete_table_suffix = variable of a suffix in tables that will be deleted
# table_duplicate_delete_f() ----
#' @title Delete tables based on suffix 
#' @description Delete tables based on suffix
#' @author Jeremy Whitehurst, 2024-05-01
#' 
#' @param conn Name of the connection to the FROM SQL database
#' @param delete_table_suffix String of a suffix in tables that will be deleted
#'  
#' @details
#' Plans for future improvement ... 
#' 
#' @importFrom utils askYesNo
#' 
#' @return None
#' 
#' @export
#' 
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
#'  

table_duplicate_delete_f <- function(conn,
                                     delete_table_suffix = "_dupe_table_to_delete"
                                     ) {
  tables <- DBI::dbGetQuery(conn,
                            glue::glue_sql("SELECT * FROM [INFORMATION_SCHEMA].[TABLES]
                                           WHERE [TABLE_NAME] LIKE {paste0('%', delete_table_suffix, '%')}
                                           ORDER BY TABLE_SCHEMA, TABLE_NAME",
                                           .con = conn))
  message(glue::glue("There are {nrow(tables)} table(s) to delete:"))
  for(i in 1:nrow(tables)) {
    message(glue::glue("{i}: [{tables[i, 'TABLE_SCHEMA']}].[{tables[i, 'TABLE_NAME']}]"))
  }
  confirm <- askYesNo("Delete tables?")
  if(confirm == F || is.na(confirm)) {
    stop("Table deletion cancelled.")
  } else {
    for(i in 1:nrow(tables)) {
      DBI::dbExecute(conn,
                     glue::glue_sql("DROP TABLE {`tables[i, 'TABLE_SCHEMA']`}.{`tables[i, 'TABLE_NAME']`}",
                                    .con = conn))
    }
    message("Table(s) deleted.")
  }
}
