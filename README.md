# apde_utilities
Common functions used throughout APDE for its work

----------------------------

## Purpose
This package provides a variety of tools to facilitate APDE's ETL and informatics processes. For example, there are functions to create SQL server connections, and load, create, copy, and QA tables. See the [Highlighted functions](#highlighted-functions-in-alphabetical-order) section below for more details. 

For APDE's custom *analytic tools*, please refer to the [rads](https://github.com/PHSKC-APDE/rads/) package. 

## Installation
1.  `apde` depends on version 17 or higher of [Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15). You can download it [here](https://go.microsoft.com/fwlink/?linkid=2187214)

2.  Make sure remotes is installed in R ... 
    - `install.packages("remotes")`.

3.  Install `apde` in R ... 
    - `remotes::install_github("PHSKC-APDE/apde", auth_token = NULL)`

4.  Load `apde` in R ... 
    - `library(apde)`

#### Optional (but recommended)

 - `install.packages("keyring")`
 -  `keyring::key_set(service = 'hhsaw', username = 'Your.KCUsername@kingcounty.gov')`
 - In the pop-up window type in your standard King County password (the one you use to log into your laptop) and click `OK`

#### Note!!

 - You will need permission to access the HHSAW Production SQL Server for many of the functions. 

## Highlighted functions (in alphabetical order)

### `add_index()`
 Add an index to a SQL Server table. 

### `apde_keyring_set_f()`
Create or update a keyring with a specified username.

### `apde_keyring_check_f()`
Check if a keyring exists and create it if it does not exist.

### `apde_notify_set_cred_f()`
Reset the saved Outlook credentials stored on the system's keyring. This function will ask for an email address and password.

### `apde_notify_menu_f()`
Displays a menu to create and edit custom email messages, email addresses, and email address lists.
Select a message from the "Select Message" drop down or select "New Message". From here, the message name, subject address and body can all be set in the text boxes and then saved.
The email message is sent in HTML format. Common HTML tags and how to use them can be found here https://www.w3schools.com/TAgs/default.asp. The email message uses the "glue" function so any variables or other R code can be passed into the email dynamically when surrounded by { }. 
  Example: To include the date and time the email is sent, insert {format(Sys.time(), "%m/%d/%Y %X")} into the message where it should be displayed.
Once a message is created or selected, the available email addresses are dislayed in the "Email List" section. In the left box, select all the email addresses to be sent by the selected message. Selected email addresses will fade in the left box and appear in the right box. To remove an email from the address list, select the email address in the right box.
If a new email adddress is needed or an email address needs to be changed, select the address to update or "New Email Address" from the "Select Email Address" drop down. Enter the email address and save it. New and updated email addresses will now be displayed in the "Email List" section.

### `apde_notify_f(msg_id, msg_name, vars)`
Send an automated notification email.
One of the variables `msg_id` or `msg_name` are required.
Any variables to send into the message must be contained in the vars variable.
*Ex:* `vars$custom_output` or `vars$location`
If the system does not have Outlook credentials saved, there will be a pop up asking for email address and password. This is stored on the system that runs the script in a keyring. If the credentials have changed, run apde_notify_set_cred_f() function to delete the saved Outlook credentials and set new credentials.

### `copy_into_f()`
Copy data from the data lake to the data warehouse.

### `create_db_connection()`
Create a connection to APDE's prod or dev servers.

### `create_table()`
Create a SQL table using specified variables or a YAML config file.

### `deduplicate_addresses()`
Remove duplicate addresses in the ref tables and synchronize the address tables between servers.

### `etl_qa_run_pipeline()`
Run a quality assurance pipeline for ETL (Extract, Transform, Load) processes. It analyzes data for missingness, variable distributions, and optionally check compliance with CHI (Community Health Indicators) standards. **_Coming soon!_**

### `external_table_check_f()`
Compare an external table to a source table to identify changes.

### `load_table_from_file()`
Load file data to a SQL table using specified variables or a YAML config file.

### `load_table_from_sql_f()`
Load data from one SQL table to another using specified variables or a YAML config file.

### `table_duplicate_f()`
Copy a (smaller) SQL table from one server to another.

### `table_duplicate_delete_f()`
Delete tables based on their suffix.

### `load_bcp_f()`
Load data to SQL Server Using [BCP (Bulk Copy Program)](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16&tabs=windows).

## Problems?

-   If you encounter a bug or have specific suggestions for improvement, please click on ["Issues"](https://github.com/PHSKC-APDE/apde/issues) at the top of this page and then click ["New Issue"](https://github.com/PHSKC-APDE/apde/issues/new/choose) and provide the necessary details.