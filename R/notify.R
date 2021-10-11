suppressWarnings(library(odbc)) # Read to and write from SQL
suppressWarnings(library(keyring)) # Access stored credentials
suppressWarnings(library(glue)) # Safely combine code and variables
suppressWarnings(library(blastula)) # Email functionality

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")

apde_notify_address_add_f <- function(address) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  id <- DBI::dbGetQuery(conn, 
                        glue::glue_sql("SELECT id 
                                       FROM [apde].[notify_addresses]
                                       WHERE [address] = {address}",
                                       .con = conn))
  if(nrow(id) == 0) {
    DBI::dbExecute(conn,
                   glue::glue_sql("INSERT INTO [apde].[notify_addresses]
                                  ([address])
                                  VALUES
                                  ({address})",
                                  .con = conn))
    id <- DBI::dbGetQuery(conn, 
                          glue::glue_sql("SELECT id 
                                       FROM [apde].[notify_addresses]
                                       WHERE [address] = {address}",
                                         .con = conn))
  }
  return(as.numeric(id))
}

apde_notify_address_remove_f <- function(id = NULL,
                                         address = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  if (is.null(id) & is.null(address)) {
    stop("Either id or address must be entered.")
  }
  if (!is.null(id)) {
    exists <- DBI::dbGetQuery(conn, glue::glue_sql("SELECT * 
                                                   FROM [apde].[notify_addresses]   
                                                   WHERE [id] = {id}",          
                                                   .con = conn))
  } else {
    exists <- DBI::dbGetQuery(conn, glue::glue_sql("SELECT * 
                                                   FROM [apde].[notify_addresses]   
                                                   WHERE [address] = {address}",          
                                                   .con = conn))
  }
  if(nrow(exists) == 0) {
    stop("There is no entry with this id or address.")
  }
  DBI::dbExecute(conn, 
                 glue::glue_sql("DELETE FROM [apde].[notify_addresses]                                   
                                WHERE id = {as.numeric(exists$id)}",                                   
                                .con = conn))
}

apde_notify_f <- function(msg_name,
                          vars) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msg <- DBI::dbGetQuery(conn,
                         glue::glue_sql("SELECT *
                                        FROM [apde].[notify_msgs]
                                        WHERE [msg_name] = {msg_name}",
                                        .con = conn))
  email_list <- DBI::dbGetQuery(conn,
                                glue::glue_sql("SELECT A.[address] 
                                               FROM [apde].[notify_list] L
                                               INNER JOIN [apde].[notify_addresses] A ON L.[address_id] = A.[id]
                                               WHERE L.[msg_id] = {as.numeric(msg$id)}",                                                
                                               .con = conn))
  vars <- as.list(vars)
  email <- compose_email(
    body = md(glue::glue(msg$msg_body)))
  email %>%
    smtp_send(
      to = email_list$address,
      from = msg$error_address,
      subject = glue::glue(msg$msg_subject),
      credentials = creds_key("outlook")
    )
}
