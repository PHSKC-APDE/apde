suppressWarnings(require(odbc)) # Read to and write from SQL
suppressWarnings(require(keyring)) # Access stored credentials
suppressWarnings(require(glue)) # Safely combine code and variables
suppressWarnings(require(blastula)) # Email functionality
suppressWarnings(require(svDialogs)) # Create multi-select pop-ups
suppressWarnings(require(tidyverse)) # Manipulate data
suppressWarnings(require(dplyr)) # Manipulate data
suppressWarnings(require(lubridate)) # Manipulate dates
suppressWarnings(require(shiny)) # UI
suppressWarnings(require(shinyWidgets)) #UI Widgets

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")

apde_notify_msgs_get_f <- function() {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msgs <- DBI::dbGetQuery(conn, 
                          "SELECT *                           
                          FROM [apde].[notify_msgs]
                          WHERE msg_parent IS NULL
                          ORDER BY [msg_name] ASC")
  return(msgs)
}
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
apde_notify_addresses_get_f <- function() {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  addresses <- DBI::dbGetQuery(conn, "SELECT * 
                               FROM [apde].[notify_addresses] 
                               ORDER BY [address] ASC")
  return(addresses)
}
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
    choices <- inner_join(choices, address_list)
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

apde_notify_f <- function(msg_id = NULL,
                          msg_name = NULL,
                          vars) 
  {
  
  emailReady <- tryCatch(
    { length(creds_key("outlook")) },
    error = function(x) { return(0) })
  
  if(emailReady == 0) {
    ## CREATING THE OUTLOOK CREDENTIAL
    ## ENTER EMAIL ADDRESS
    email <- dlgInput("Enter Email address:", paste0(Sys.info()["user"], "@kingcounty.gov"))$res
    ## ENTERE YOUR PW IN POP UP
    create_smtp_creds_key(
      id = "outlook",
      user = email,
      provider = "outlook",
      overwrite = TRUE,
      use_ssl = TRUE
    )
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
    body = md(glue::glue(msg$msg_body)))
  email %>%
    smtp_send(
      to = email_list$address,
      from = msg$msg_from,
      subject = glue::glue(msg$msg_subject),
      credentials = creds_key("outlook")
    )
}

apde_notify_menu_f() <- function() {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_list <- apde_notify_addresses_get_f()
  current_list <- NA
  
  ui <- fluidPage(
    titlePanel("APDE Notify Menu"),
    fluidRow(
      column(4, 
        selectInput(inputId = "msg_select", label = "Select Message",                
                    choices = c("New Message", as.list(apde_notify_msgs_get_f()$msg_name))),
        textInput(inputId = "msg_name_text", label = "Message Name"),
        textInput(inputId = "msg_subject_text", label = "Message Subject"),
        textInput(inputId = "msg_from_text", label = "Message From Address"),
        textAreaInput(inputId = "msg_body_textarea", label = "Message Body", 
                      height = 200),
        actionButton(inputId = "msg_save_btn", label = "Save Message")
      ),
      column(8, 
        multiInput(inputId = "multi_list", label = "Email List:",
                   width = 600, choices = as.list(address_list$address)),
        actionButton(inputId = "list_save_btn", label = "Save Email List"),
        hr(),
        selectInput(inputId = "address_select", label = "Select Email Address",
                choices = c("New Email Address", as.list(address_list$address))),
        textInput(inputId = "address_text", label = "Email Address"),
        actionButton(inputId = "address_save_btn", label = "Save Email Address")
      )
    ),
    fluidRow(
      column(10, offset = 1,
             hr(),
             textOutput("output_msg"),
             hr(),
             )
    )
  )
  
  server <- function(input, output, session) {
    text_reactive <- reactiveValues(text = "")
    
    observeEvent(input$msg_select, {
      if(input$msg_select != "New Message") {
        msg_id <- apde_notify_msg_id_get_f(msg_name = input$msg_select)
        current_list <- apde_notify_list_get_f(msg_id = msg_id)
        msg <- apde_notify_msg_get_f(msg_id = msg_id)
        updateTextInput(session = session,
                        inputId = "msg_name_text",
                        value = msg$msg_name)
        updateTextInput(session = session,
                        inputId = "msg_subject_text",
                        value = msg$msg_subject)
        updateTextInput(session = session,
                        inputId = "msg_from_text",
                        value = msg$msg_from)
        updateTextAreaInput(session = session,
                            inputId = "msg_body_textarea",
                            value = msg$msg_body)
      } else { 
        current_list = c() 
        updateTextInput(session = session,
                        inputId = "msg_name_text",
                        value = NA)
        updateTextInput(session = session,
                        inputId = "msg_subject_text",
                        value = NA)
        updateTextInput(session = session,
                        inputId = "msg_from_text",
                        value = NA)
        updateTextAreaInput(session = session,
                            inputId = "msg_body_textarea",
                            value = NA)
        
      }

      address_list <- apde_notify_addresses_get_f()
      updateMultiInput(session = session,
                        inputId = "multi_list",
                        choices = as.list(address_list$address),
                        selected = as.list(current_list$address))
    })
    
    observeEvent(input$msg_save_btn, {
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
      updateSelectInput(session = session,
                        inputId = "msg_select",                 
                        choices = c("New Message", as.list(apde_notify_msgs_get_f()$msg_name)),
                        selected = mname)
    })
    
    observeEvent(input$list_save_btn, {
      if(input$msg_select != "New Message") {
        msg_id <- apde_notify_msg_id_get_f(msg_name = input$msg_select)
        apde_notify_list_set_f(msg_id = msg_id, 
                               choices = input$multi_list) 
        text_reactive$text <- paste0("Email List: '", input$msg_name_text, "' updated - ", Sys.time())
      } else {
        text_reactive$text <- paste0("Error: Select a Message before updating the Email List - ", Sys.time())
      }
    })
    
    observeEvent(input$address_select, {
      if(input$address_select != "New Email Address") {
        updateTextInput(session = session,
                        inputId = "address_text",
                        value = input$address_select)
      } else {
        updateTextInput(session = session,
                        inputId = "address_text",
                        value = NA)
      }
    })
    
    observeEvent(input$address_save_btn, {
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
      updateMultiInput(session = session,
                       inputId = "multi_list",
                       choices = as.list(address_list$address),
                       selected = as.list(current_list$address))
      updateSelectInput(session = session,
                        inputId = "address_select",                 
                        choices = c("New Email Address", as.list(address_list$address)),
                        selected = address)
    })
    
    output$output_msg <- renderText({ text_reactive$text })
  }
  
  shinyApp(ui = ui, server = server)
}



