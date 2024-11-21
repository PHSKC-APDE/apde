# apde_notify_set_cred_f() ----
#' @title Set Outlook Credentials for APDE Notify
#' @description Creates and stores Outlook credentials for use with APDE Notify functions.
#' @importFrom svDialogs dlgInput
#' @importFrom blastula create_smtp_creds_key
#' @return None
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_set_cred_f <- function() {
  ## CREATING THE OUTLOOK CREDENTIAL
  ## ENTER EMAIL ADDRESS
  email <- svDialogs::dlg_Input("Enter Email address:", paste0(Sys.info()["user"], "@kingcounty.gov"))$res
  ## ENTERE YOUR PW IN POP UP
  blastula::create_smtp_creds_key(
    id = "outlook",
    user = email,
    provider = "outlook",
    overwrite = TRUE,
    use_ssl = TRUE
  )
}

# apde_notify_msgs_get_f() ----
#' @title Retrieve APDE Notify Messages
#' @description Fetches all parent messages from the APDE notify_msgs table.
#' @importFrom DBI dbGetQuery
#' @return A dataframe containing message information
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_msgs_get_f <- function() {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msgs <- DBI::dbGetQuery(conn, 
                          "SELECT *                           
                          FROM [apde].[notify_msgs]
                          WHERE msg_parent IS NULL
                          ORDER BY [msg_name] ASC")
  return(msgs)
}

# apde_notify_msg_set_f() ----
#' @title Set or Update APDE Notify Message
#' @param msg_id Integer. ID of the message to update. Use 0 for new messages.
#' @param msg_name Character. Name of the message.
#' @param msg_subject Character. Subject of the message.
#' @param msg_body Character. Body of the message.
#' @param msg_from Character. Sender's email address.
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue_sql
#' @return Integer. ID of the new or updated message.
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_msg_set_f <- function(msg_id = 0, 
                                  msg_name,
                                  msg_subject,
                                  msg_body,
                                  msg_from) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  if(msg_id > 0) { 
    msg <- apde_notify_msg_get_f(msg_id)
    if(msg$msg_name == msg_name & msg$msg_subject == msg_subject & 
        msg$msg_body == msg_body & msg$msg_from == msg_from) {
      return(msg_id)
    }
  }
  DBI::dbExecute(conn, 
                 glue::glue_sql("INSERT INTO [apde].[notify_msgs]                                     
                                ([msg_name], [msg_subject], [msg_body], 
                                [msg_from], [msg_datetime])  
                                VALUES                                     
                                ({msg_name}, {msg_subject}, {msg_body},     
                                {msg_from}, GETDATE())",
                                .con = conn))
  new_id <- DBI::dbGetQuery(conn, 
                            glue::glue_sql("SELECT TOP (1) id
                                           FROM [apde].[notify_msgs]
                                           WHERE [msg_name] = {msg_name}
                                            AND msg_parent IS NULL
                                           ORDER BY msg_datetime DESC",
                                           .con = conn))[1]
  if(msg_id > 0) {
    DBI::dbExecute(conn, 
                   glue::glue_sql("UPDATE [apde].[notify_msgs]                                
                                  SET [msg_parent] = {new_id}
                                  WHERE id = {msg_id}",
                                  .con = conn))
    DBI::dbExecute(conn, 
                   glue::glue_sql("UPDATE [apde].[notify_msgs]                                
                                  SET [msg_parent] = {new_id}
                                  WHERE [msg_parent] = {msg_id}",
                                  .con = conn))
    DBI::dbExecute(conn, 
                   glue::glue_sql("UPDATE [apde].[notify_list]                                
                                  SET [msg_id] = {new_id}
                                  WHERE [msg_id] = {msg_id}",
                                  .con = conn))
  }
  return(new_id)
}

# apde_notify_msg_get_f() ----
#' @title Retrieve a Specific APDE Notify Message
#' @param msg_id Integer. ID of the message to retrieve.
#' @param msg_name Character. Name of the message to retrieve.
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue_sql
#' @return A dataframe containing the message information.
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_msg_get_f <- function(msg_id = NULL,
                                  msg_name = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  if(is.null(msg_id)) {
    msg_id <- apde_notify_msg_id_get_f(msg_name = msg_name)
  }
  if(is.null(msg_id)) {
    stop("No Message in the system with the provided Name or ID.")
  }
  msg <- DBI::dbGetQuery(conn, 
                         glue::glue_sql("SELECT TOP (1) *                                               
                                        FROM [apde].[notify_msgs]                                                                           
                                        WHERE [id] = {msg_id}
                                          AND [msg_parent] IS NULL",                                                                                                      
                                        .con = conn))
  return(msg)
}

# apde_notify_msg_id_get_f() ----
#' @title Get APDE Notify Message ID
#' @param msg_id Integer. ID of the message.
#' @param msg_name Character. Name of the message.
#' @return Integer. ID of the message.
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_msg_id_get_f <- function(msg_id = NULL,
                                     msg_name = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msgs <- apde_notify_msgs_get_f()
  if(is.null(msg_id)) {
    msg_id <- msgs[msgs$msg_name == msg_name, ]$id
  } else {
    msg_id <- msgs[msgs$id == msg_id, ]$id
    if (length(msg_id) == 0) {
      msg_id <- NULL
    }
  }
  return(msg_id)
}

# apde_notify_addresses_get_f() ----
#' @title Retrieve All APDE Notify Addresses
#' @importFrom DBI dbGetQuery
#' @return A dataframe containing all notify addresses.
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_addresses_get_f <- function() {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  addresses <- DBI::dbGetQuery(conn, "SELECT * 
                               FROM [apde].[notify_addresses] 
                               ORDER BY [address] ASC")
  return(addresses)
}

# apde_notify_address_get_f() ----
#' @title Retrieve a Specific APDE Notify Address
#' @param address_id Integer. ID of the address to retrieve.
#' @param address Character. Email address to retrieve.
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue_sql
#' @return A dataframe containing the address information.
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_address_get_f <- function(address_id = NULL,
                                      address = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_id <- apde_notify_address_id_get_f(address_id = address_id,
                                             address = address)
  if(is.null(address_id)) {
    stop("No Address in the system with the provided Address or ID.")
  }
  address <- DBI::dbGetQuery(conn, glue::glue_sql("SELECT TOP (1) *                                               
                                                  FROM [apde].[notify_addresses]                                          
                                                  WHERE [id] = {address_id}",                                      
                                                  .con = conn))
  return(address)
}

# apde_notify_address_set_f() ----
#' @title Update an APDE Notify Address
#' @param address_id Integer. ID of the address to update.
#' @param address Character. Current email address.
#' @param new_address Character. New email address.
#' @importFrom DBI dbExecute
#' @importFrom glue glue_sql
#' @return None
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_address_set_f <- function(address_id = NULL,
                                      address = NULL,
                                      new_address) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_id <- apde_notify_address_id_get_f(address_id = address_id,
                                             address = address)
  if(is.null(address_id)) {
    stop("No Address in the system with the provided Address or ID.")
  }
  new_address_check <- apde_notify_address_id_get_f(address = new_address)
  if(length(new_address_check) > 0) {
    stop("New Address already exists.")
  }
  DBI::dbExecute(conn, glue::glue_sql("UPDATE [apde].[notify_addresses]
                                      SET [address] = {new_address}
                                      WHERE [id] = {address_id}",
                                      .con = conn))
}

# apde_notify_address_create_f() ----
#' @title Create a New APDE Notify Address
#' @param address Character. Email address to create.
#' @importFrom DBI dbExecute
#' @importFrom glue glue_sql
#' @return Integer. ID of the newly created address.
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_address_create_f <- function(address) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_id <- apde_notify_address_id_get_f(address = address)
  if(length(address_id) > 0) {
    stop("Address already exists.")
  }
  DBI::dbExecute(conn, glue::glue_sql("INSERT INTO [apde].[notify_addresses]
                                      ([address])
                                      VALUES
                                      ({address})",
                                      .con = conn))
  address_id <- apde_notify_address_id_get_f(address = address)
  return(address_id)
}

# apde_notify_address_delete_f() ----
#' @title Delete an APDE Notify Address
#' @param address_id Integer. ID of the address to delete.
#' @param address Character. Email address to delete.
#' @importFrom DBI dbExecute
#' @importFrom glue glue_sql
#' @return None
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_address_delete_f <- function(address_id = NULL,
                                         address = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_id <- apde_notify_address_id_get_f(address_id = address_id,
                                             address = address)
  if(is.null(address_id)) {
    stop("No Address in the system with the provided Address or ID.")
  }
  DBI::dbExecute(conn, 
                 glue::glue_sql("DELETE FROM [apde].[notify_list]                             
                                WHERE [address_id] = {address_id}",                              
                                .con = conn))
  DBI::dbExecute(conn, 
                 glue::glue_sql("DELETE FROM [apde].[notify_addresses]                             
                                WHERE [id] = {address_id}",                              
                                .con = conn))
}

# apde_notify_address_id_get_f() ----
#' @title Get APDE Notify Address ID
#' @param address_id Integer. ID of the address.
#' @param address Character. Email address.
#' @return Integer. ID of the address.
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_address_id_get_f <- function(address_id = NULL,
                                         address = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  addresses <- apde_notify_addresses_get_f()
  if(is.null(address_id)) {
    address_id <- addresses[addresses$address == address, ]$id
  } else {
    address_id <- addresses[addresses$id == address_id, ]$id
    if (length(address_id) == 0) {
      address_id <- NULL
    }
  }
  return(address_id)
}

# apde_notify_list_get_f() ----
#' @title Retrieve APDE Notify List for a Message
#' @param msg_id Integer. ID of the message.
#' @param msg_name Character. Name of the message.
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue_sql
#' @return A dataframe containing the notify list for the specified message.
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_list_get_f <- function(msg_id = NULL,
                                   msg_name = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msg_id <- apde_notify_msg_id_get_f(msg_id = msg_id,
                                     msg_name = msg_name)
  if(is.null(msg_id)) {
    stop("No Message in the system with the provided Name or ID.")
  }
  mlist <- DBI::dbGetQuery(conn, 
                           glue::glue_sql("SELECT L.[msg_id], L.[address_id], A.[address]
                                          FROM [apde].[notify_list] L                                                                             
                                          INNER JOIN [apde].[notify_addresses] A ON L.[address_id] = A.[id]
                                          WHERE L.[msg_id] = {msg_id}
                                          ORDER BY A.[address]",                                         
                                          .con = conn))
  return(mlist)  
}

# apde_notify_list_set_f() ----
#' @title Set APDE Notify List for a Message
#' @param msg_id Integer. ID of the message.
#' @param msg_name Character. Name of the message.
#' @param choices Vector. List of email addresses to associate with the message.
#' @importFrom DBI dbExecute
#' @importFrom glue glue_sql
#' @importFrom dplyr inner_join
#' @return None
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_list_set_f <- function(msg_id = NULL,
                                   msg_name = NULL,
                                   choices) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msg_id <- apde_notify_msg_id_get_f(msg_id = msg_id,
                                     msg_name = msg_name)
  if(is.null(msg_id)) {
    stop("No Message in the system with the provided Name or ID.")
  }
  DBI::dbExecute(conn, 
                 glue::glue_sql("DELETE FROM [apde].[notify_list]                             
                                WHERE [msg_id] = {msg_id}",                              
                                .con = conn))
  address_list <- apde_notify_addresses_get_f()
  if(length(choices) > 0) {
    choices <- as.data.frame(choices)
    colnames(choices) <- c("address")
    choices <- dplyr::inner_join(choices, address_list)
    for(i in 1:nrow(choices)) {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO [apde].[notify_list]
                                     ([msg_id], [address_id])
                                     VALUES
                                     ({msg_id}, {choices[i,]$id})",
                                    .con = conn))
    }
  }
}

# apde_notify_f() ----
#' @title Send APDE Notification
#' @param msg_id Integer. ID of the message to send.
#' @param msg_name Character. Name of the message to send.
#' @param vars List. Variables to substitute in the message body.
#' @importFrom blastula compose_email creds_key md smtp_send
#' @importFrom glue glue
#' @importFrom dplyr '%>%'
#' @return None
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_f <- function(msg_id = NULL,
                          msg_name = NULL,
                          vars) {
  emailReady <- tryCatch(
    { length(blastula::creds_key("outlook")) },
    error = function(x) { return(0) })
  
  if(emailReady == 0) {
    apde_notify_set_cred_f()
  }
  
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  if(is.null(msg_id)) {
    msg_id <- apde_notify_msg_id_get_f(msg_name = msg_name)  
  }
  if(is.null(msg_id)) {
    stop("No Message in the system with the provided Name or ID.")
  }
  msg <- apde_notify_msg_get_f(msg_id = msg_id)
  email_list <- apde_notify_list_get_f(msg_id = msg_id)
  vars <- as.list(vars)
  email <- compose_email(
    body = blastula::md(glue::glue(msg$msg_body)))
  email %>%
    smtp_send(
      to = email_list$address,
      from = msg$msg_from,
      subject = glue::glue(msg$msg_subject),
      credentials = creds_key("outlook")
    )
}

# apde_notify_menu_f() ----
#' @title Launch APDE Notify Menu
#' @description Launches a Shiny app for managing APDE Notify messages and email lists.
#' @importFrom shiny fluidPage titlePanel fluidRow column selectInput textInput textAreaInput actionButton hr textOutput observeEvent updateTextInput updateTextAreaInput updateSelectInput reactiveValues renderText shinyApp
#' @importFrom shinyWidgets multiInput updateMultiInput
#' @return A Shiny app object
#' @export
#' @examples
#'  \dontrun{
#'   # ENTER EXAMPLES HERE
#'  }
apde_notify_menu_f <- function() {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_list <- apde_notify_addresses_get_f()
  current_list <- NA
  
  ui <- shiny::fluidPage(
    shiny::titlePanel("APDE Notify Menu"),
    shiny::fluidRow(
      shiny::column(4, 
        shiny::selectInput(inputId = "msg_select", label = "Select Message",                
                    choices = c("New Message", as.list(apde_notify_msgs_get_f()$msg_name))),
        shiny::textInput(inputId = "msg_name_text", label = "Message Name"),
        shiny::textInput(inputId = "msg_subject_text", label = "Message Subject"),
        shiny::textInput(inputId = "msg_from_text", label = "Message From Address"),
        shiny::textAreaInput(inputId = "msg_body_textarea", label = "Message Body", 
                      height = 200),
        shiny::actionButton(inputId = "msg_save_btn", label = "Save Message")
      ),
      shiny::column(8, 
        shinyWidgets::multiInput(inputId = "multi_list", label = "Email List",
                   width = 600, choices = as.list(address_list$address)),
        shiny::actionButton(inputId = "list_save_btn", label = "Save Email List"),
        shiny::hr(),
        shiny::selectInput(inputId = "address_select", label = "Select Email Address",
                choices = c("New Email Address", as.list(address_list$address))),
        shiny::textInput(inputId = "address_text", label = "Email Address"),
        shiny::actionButton(inputId = "address_save_btn", label = "Save Email Address")
      )
    ),
    shiny::fluidRow(
      shiny::column(10, offset = 1,
                    shiny::hr(),
                    shiny::textOutput("output_msg"),
                    shiny::hr(),
             )
    )
  )
  
  server <- function(input, output, session) {
    text_reactive <- shiny::reactiveValues(text = "")
    
    shiny::observeEvent(input$msg_select, {
      if(input$msg_select != "New Message") {
        msg_id <- apde_notify_msg_id_get_f(msg_name = input$msg_select)
        current_list <- apde_notify_list_get_f(msg_id = msg_id)
        msg <- apde_notify_msg_get_f(msg_id = msg_id)
        shiny::updateTextInput(session = session,
                        inputId = "msg_name_text",
                        value = msg$msg_name)
        shiny::updateTextInput(session = session,
                        inputId = "msg_subject_text",
                        value = msg$msg_subject)
        shiny::updateTextInput(session = session,
                        inputId = "msg_from_text",
                        value = msg$msg_from)
        shiny::updateTextAreaInput(session = session,
                            inputId = "msg_body_textarea",
                            value = msg$msg_body)
      } else { 
        current_list = c() 
        shiny::updateTextInput(session = session,
                        inputId = "msg_name_text",
                        value = NA)
        shiny::updateTextInput(session = session,
                        inputId = "msg_subject_text",
                        value = NA)
        shiny::updateTextInput(session = session,
                        inputId = "msg_from_text",
                        value = NA)
        shiny::updateTextAreaInput(session = session,
                            inputId = "msg_body_textarea",
                            value = NA)
        
      }

      address_list <- apde_notify_addresses_get_f()
      shinyWidgets::updateMultiInput(session = session,
                                     inputId = "multi_list",
                                     choices = as.list(address_list$address),
                                     selected = as.list(current_list$address))
    })
    
    shiny::observeEvent(input$msg_save_btn, {
      if(input$msg_select == "New Message") {
        msg_id <- 0
      } else {
        msg_id <- apde_notify_msg_id_get_f(msg_name = input$msg_select)
      }
      mname <- input$msg_name_text
      apde_notify_msg_set_f(msg_id, 
                            msg_name = input$msg_name_text,
                            msg_subject = input$msg_subject_text,
                            msg_body = input$msg_body_textarea,
                            msg_from = input$msg_from_text)
      if(input$msg_select == "New Message") {
        text_reactive$text <- paste0("Message: '", input$msg_name_text, "' created - ", Sys.time())
      } else {
        text_reactive$text <- paste0("Message: '", input$msg_name_text, "' updated - ", Sys.time())
      }
      shiny::updateSelectInput(session = session,
                        inputId = "msg_select",                 
                        choices = c("New Message", as.list(apde_notify_msgs_get_f()$msg_name)),
                        selected = mname)
    })
    
    shiny::observeEvent(input$list_save_btn, {
      if(input$msg_select != "New Message") {
        msg_id <- apde_notify_msg_id_get_f(msg_name = input$msg_select)
        apde_notify_list_set_f(msg_id = msg_id, 
                               choices = input$multi_list) 
        text_reactive$text <- paste0("Email List: '", input$msg_name_text, "' updated - ", Sys.time())
      } else {
        text_reactive$text <- paste0("Error: Select a Message before updating the Email List - ", Sys.time())
      }
    })
    
    shiny::observeEvent(input$address_select, {
      if(input$address_select != "New Email Address") {
        shiny::updateTextInput(session = session,
                        inputId = "address_text",
                        value = input$address_select)
      } else {
        shiny::updateTextInput(session = session,
                        inputId = "address_text",
                        value = NA)
      }
    })
    
    shiny::observeEvent(input$address_save_btn, {
      if(input$address_select != "New Email Address") {
        print(input$address_select)
        address_id <- apde_notify_address_id_get_f(address = input$address_select)
        apde_notify_address_set_f(address_id = address_id, 
                                  new_address = input$address_text)
        text_reactive$text <- paste0("Email Address: '", input$address_text, "' updated - ", Sys.time())
      } else {
        apde_notify_address_create_f(address = input$address_text)
        text_reactive$text <- paste0("Email Address: '", input$address_text, "' created - ", Sys.time())
      }
      if(input$msg_select != "New Message") {
        current_list <- apde_notify_list_get_f(msg_name = input$msg_select)
      } else {
        current_list <- c()
      }
      address <- input$address_text
      address_list <- apde_notify_addresses_get_f()
      shinyWidgets::updateMultiInput(session = session,
                                     inputId = "multi_list",
                                     choices = as.list(address_list$address),
                                     selected = as.list(current_list$address))(session = session,
                                                                               inputId = "multi_list",
                                                                               choices = as.list(address_list$address),
                                                                               selected = as.list(current_list$address))
      shiny::updateSelectInput(session = session,
                        inputId = "address_select",                 
                        choices = c("New Email Address", as.list(address_list$address)),
                        selected = address)
    })
    
    output$output_msg <- shiny::renderText({ text_reactive$text })
  }
  
  shiny::shinyApp(ui = ui, server = server)
}



