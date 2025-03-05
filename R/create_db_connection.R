#' @title Create Database Connection Function
#' 
#' @author Jeremy Whitehurst
#' @author Kai Fukutaki
#' 
#' @description
#' Create a connection to the prod or dev servers that APDE uses
#' 
#' @param  server Whether we are connecting to HHSAW, inthealth, or phextractstore
#' @param prod Whether the production or dev/WIP servers are to be used. Defaults to \code{prod = TRUE}.
#' @param interactive Whether to input password manually for HHSAW/inthealth or to 
#' use keyring. True should be used when connecting on VM, typically, or if
#' keyring/odbc are not set up yet for a given server. Defaults to \code{interactive = FALSE}.
#' 
#' @importFrom odbc odbc odbcListDataSources dbConnect
#' @importFrom keyring key_list key_get
#' 
#' @export
#' 


#### FUNCTION ####
create_db_connection <- function(server = c("phextractstore", "hhsaw", "inthealth"), 
                                 prod = TRUE, 
                                 interactive = FALSE) {
  # declare visible bindings for global variables ----
    odbc_sources <- NULL
  
  # validate arguments ----
    server <- match.arg(server)
    
    if (!is.logical(prod)) {
      stop("'prod' must be a logical value (TRUE/FALSE)")
    }
    
    if (!is.logical(interactive)) {
      stop("'interactive' must be a logical value (TRUE/FALSE)")
    }
    
  # identify databases and servers ----
    if (server == "hhsaw") {
      db_name <- "hhs_analytics_workspace"
    } else if (server == "inthealth") {
      db_name <- "inthealth_edw"
    }
    
    if (prod == T & server %in% c("hhsaw", "inthealth")) {
      server_name <- "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433"
    } else {
      server_name <- "tcp:kcitazrhpasqldev20.database.windows.net,1433"
    }
  
  # connect to databases ----
    if (server == "phextractstore") {
      if (prod == T) {
        conn <- odbc::dbConnect(odbc::odbc(),
                                Driver = "SQL Server",
                                Server = "KCITSQLPRPHIP40",
                                Database = "PHExtractStore")
      } else {
        conn <- odbc::dbConnect(odbc::odbc(),
                                Driver = "SQL Server",
                                Server = "KCITSQLUATHIP40",
                                Database = "PHExtractStore")
      }
    } else if (interactive == F) {
      conn <- DBI::dbConnect(odbc::odbc(),
                             driver = "ODBC Driver 17 for SQL Server",
                             server = server_name,
                             database = db_name,
                             uid = keyring::key_list("hhsaw")[["username"]],
                             pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                             Encrypt = "yes",
                             TrustServerCertificate = "yes",
                             Authentication = "ActiveDirectoryPassword")
    } else if (interactive == T) {
      conn <- DBI::dbConnect(odbc::odbc(),
                             driver = "ODBC Driver 17 for SQL Server",
                             server = server_name,
                             database = db_name,
                             uid = keyring::key_list("hhsaw")[["username"]],
                             Encrypt = "yes",
                             TrustServerCertificate = "yes",
                             Authentication = "ActiveDirectoryInteractive")
    }
    
  # return connection ----
    return(conn)
}