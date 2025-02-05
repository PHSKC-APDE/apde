#' @title Load an R Data.Frame to SQL Server Using BCP (Bulk Copy Program)
#' @description This function loads data from an R data.frame into a SQL Server 
#' database table by saving the data.frame to an temporary file and then using 
#' the BCP (Bulk Copy Program) utility.
#' @author Jeremy Whitehurst, 2025-02-05
#' 
#' @param dataset A data.frame or data.table to be loaded, or a character string 
#' specifying the filepath of a text file to be loaded.
#' @param server The name of the SQL Server instance.
#' @param db_name The name of the target database.
#' @param schema_name The name of the schema containing the target table.
#' @param table_name The name of the target table.
#' @param user Optional. The username for SQL Server authentication.
#' @param pass Optional. The password for SQL Server authentication.
#'  
#' @details
#' This function uses the BCP utility to efficiently load data into a SQL Server 
#' table. It supports both Windows authentication and SQL Server authentication. 
#' If a data.frame or data.table is provided, it is first written to a temporary 
#' file before being loaded.
#' 
#' Plans for future improvement ...
#' 
#' @importFrom data.table fwrite
#' @importFrom glue glue
#' 
#' @return A character vector containing the output of the BCP command.
#' 
#' @export
#' 
#' @examples
#'  \dontrun{
#'   # Load data from a data.frame
#'   df <- data.frame(id = 1:5, name = c("Alice", "Bob", "Charlie", "David", "Eve"))
#'   load_df_bcp_f(df, "SQLSERVER01", "MyDatabase", "MySchema", "MyTable")
#'   
#'   # Load data using SQL Server authentication
#'   load_df_bcp_f(df, "SQLSERVER01", "MyDatabase", "dbo", "MyTable", 
#'             user = "myuser", pass = "mypassword")
#'  }
#'
load_df_bcp_f <- function(dataset, 
                       server,
                       db_name,
                       schema_name,
                       table_name, 
                       user = NULL, 
                       pass = NULL
) {
  
  if(is.character(dataset)){
    filepath = dataset
  }else{
    filepath = tempfile(fileext = '.txt')
    on.exit({
      if(file.exists(filepath)) file.remove(filepath)
    })
    data.table::fwrite(dataset, filepath, sep = '\t')
  }
  if(!is.null(user) && is.null(pass)) {
    stop("Must have the pass variable if the user variable is set!")
  }
  
  if(!is.null(user)) {
    user <- glue::glue("-U {user}")
    G <- "-G"
    DT <- "-D"
  } else {
    user <- ""
    G <- ""
    DT <- "-T"
  }
  if(!is.null(pass)) {
    pass <- glue::glue("-P {pass}")
  } else {
    pass <- ""
  }
  if(server == "PHClaims") {
    server <- "KCITSQLPRPENT40"
  }
  
  # Set up BCP arguments and run BCP
  bcp_args <- c(glue::glue('{schema_name}.{table_name} IN ',
                           '"{filepath}" ',
                           '-r \\n ',
                           '-t \\t ',
                           '-C 65001 ',
                           '-F 2 ',
                           '-S "{server}" ',
                           '-d {db_name} ', 
                           '-b 100000 ',
                           '-c ',
                           '{G} ',
                           '{user} ',
                           '{pass} ',
                           '-q ',
                           '{DT}')) 
  
  
  # Load
  a = system2(command = "bcp", args = c(bcp_args), stdout = TRUE, stderr = TRUE)
  
  status = attr(a, 'status')
  if(length(status)>0 && status == 1){
    stop(a)
  }
  
  a
}