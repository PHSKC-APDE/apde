# apde_utilities
Common functions used throughout APDE for its work

# APDE Notify
This fuction is used to send custom emails to an email list inside an R script.
## Required Libraries
- odbc
- keyring
- glue
- blastula
- svDialogs
- tidyverse
- dplyr
- lubridate
- shiny
- shinyWidgets
## Required R Code Before Use
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/notify.R")
## Functions

### apde_notify_menu_f()

<p>This function displays a menu to create and edit custom email messages, email addresses, and email address lists.</p>
<p>Select a message from the "Select Message" drop down or select "New Message". From here, the message name, subject address and body can all be set in the text boxes and then saved.</p>
<p>The email message is sent in HTML format. Common HTML tags and how to use them can be found here https://www.w3schools.com/TAgs/default.asp. The email message uses the "glue" function so any variables or other R code can be passed into the email dynamically when surrounded by { }. </p>
<p>  Example: To include the date and time the email is sent, insert {format(Sys.time(), "%m/%d/%Y %X")} into the message where it should be displayed.</p>
<p>Once a message is created or selected, the available email addresses are dislayed in the "Email List" section. In the left box, select all the email addresses to be sent by the selected message. Selected email addresses will fade in the left box and appear in the right box. To remove an email from the address list, select the email address in the right box.</p>
<p>If a new email adddress is needed or an email address needs to be changed, select the address to update or "New Email Address" from the "Select Email Address" drop down. Enter the email address and save it. New and updated email addresses will now be displayed in the "Email List" section.</p>

### apde_notify_f(msg_id, msg_name, vars)

<p>This function sends the email.</p>
<p>One of the variables msg_id or msg_name are required.</p>
<p>Any vaiables to send into the the message must be contained in the vars variable.</p>
<p>  Ex: vars$custom_output or vars$location</p>
<p>If the system does not have Outlook credentials saved, there will be a pop up asking for email address and password. This is stored on the system that runs the script in a keyring.  </p>
