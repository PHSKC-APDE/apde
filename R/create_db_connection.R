#### CODE TO SET CURRENT DB CONNECTION
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12


### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R

### Function elements
# server = whether we are working in HHSAW or PHClaims or other


#### FUNCTION ####
create_db_connection <- function(server = c("phclaims", "hhsaw", "inthealth"), 
                                 prod = T, 
                                 interactive = F) {
  
  server <- match.arg(server)
  
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
  
  if (server == "phclaims") {
    odbc_sources <- odbcListDataSources()$name
    if ("PHClaims" %in% odbc_sources) {
      conn <- DBI::dbConnect(odbc::odbc(), "PHClaims")
    }
    else if ("PHClaims40" %in% odbc_sources) {
      conn <- DBI::dbConnect(odbc::odbc(), "PHClaims40")
    }
    else {
      stop("PHClaims db is not in available ODBC sources.")
    }
  } else if (server == "phextractstore") {
    if (prod == T & "PHExtractStoreProd" %in% odbc_sources) {
      conn <- DBI::dbConnect(odbc::odbc(), "PHExtractStoreProd")
    }
    else if (prod == F & "PHExtractStoreDev" %in% odbc_sources) {
      conn <- DBI::dbConnect(odbc::odbc(), "PHExtractStoreDev")
    }
    else {
      stop("PHExtractStoreProd or Dev is not in available ODBC sources.")
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
  
  return(conn)
}
