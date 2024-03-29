#### FUNCTION TO COMPARE EXTERNAL TABLE TO SOURCE TABLE TO FIND CHANGES

#### PARAMETERS ####
# conn = name of the connection to the SQL database with source table
# schema = name of schema for source table
# table = name of source table
# conn_ext = name of the connection to the SQL database with source external
# schema_ext = name of schema for external table
# table_ext = name of external table
# sql_display = show the SQL script in Console
# sql_file_path = write the SQL script to file
# overwrite = overwrite sql script, appends if FALSE

#### FUNCTION ####
external_table_check_f <- function(conn,
                                   schema,
                                   table,
                                   conn_ext,
                                   schema_ext,
                                   table_ext,
                                   sql_display = T,
                                   sql_file_path = NULL,
                                   overwrite = T) {
  
  # Get source column information
  source_cols <- DBI::dbGetQuery(conn,
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
                                                  		WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',[CHARACTER_MAXIMUM_LENGTH], ')')
                                                  		WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
                                                  		ELSE ''
                                                  	END,
	                                                  ' NULL') AS 'COLUMN_DEFINITION'
                                                FROM [INFORMATION_SCHEMA].[COLUMNS]
                                                WHERE 
                                                  [TABLE_NAME] = {table}
                                                  AND [TABLE_SCHEMA] = {schema}
                                                ORDER BY [ORDINAL_POSITION]",
                                                .con = conn))
  
  # Get current external column information (blank if none exisits)
  external_cols <- DBI::dbGetQuery(conn_ext,
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
                                                  		WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',[CHARACTER_MAXIMUM_LENGTH], ')')
                                                  		WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
                                                  		ELSE ''
                                                  	END,
	                                                  ' NULL') AS 'COLUMN_DEFINITION'
                                                FROM [INFORMATION_SCHEMA].[COLUMNS]
                                                WHERE 
                                                  [TABLE_NAME] = {table_ext}
                                                  AND [TABLE_SCHEMA] = {schema_ext}
                                                ORDER BY [ORDINAL_POSITION]",
                                                .con = conn_ext))
  # Compare all columns, column types, lengths, precision, etc.
  result <- dplyr::all_equal(source_cols, external_cols, ignore_col_order = F, ignore_row_order = F)
  
  # If everything matches, end function
  if(result == TRUE) {
    message(glue("Source Table [{schema}].[{table}] Matches External Table [{schema_ext}].[{table_ext}]"))
    return(T)
  }
  
  # Create SQL script
  sql <- glue::glue_sql("
CREATE EXTERNAL TABLE {`schema_ext`}.{`table_ext`}
  ({DBI::SQL(glue_collapse(glue_sql('{DBI::SQL(source_cols$COLUMN_DEFINITION)}',.con = conn_ext), sep = ', \n  '))})
WITH (DATA_SOURCE = [data_WS_EDW], SCHEMA_NAME = N{schema}, OBJECT_NAME = N{table});", .con = conn_ext)
  
  # Display SQL script in console
  if(sql_display == T) {
    message(glue("External Table Creation SQL Script for [{schema_ext}].[{table_ext}]:"))
    message(sql)
  }
  
  # Write SQL Script to file
  if(is.null(sql_file_path) == F) {
    # If file exists and overwrite is F, append the script to the file.
    # Else, create a new file/overwrite the existing file
    if(file.exists((sql_file_path)) == T && overwrite == F) {
      message(glue("Appending SQL Script for [{schema_ext}].[{table_ext}] to: {sql_file_path}"))
      write(paste0("\n", sql), file = sql_file_path, append = T)
    } else {
      message(glue("Writing SQL Script for [{schema_ext}].[{table_ext}] to: {sql_file_path}"))
      write(sql, file = sql_file_path, append = F)
    }
  }
  
  return(F)
}
