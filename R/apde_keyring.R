## Install Packages
if("keyring" %in% rownames(installed.packages()) == F) {
  install.packages("keyring")
}
if("svDialogs" %in% rownames(installed.packages()) == F) {
  install.packages("svDialogs")
}
## Load Libraries
library(svDialogs) # Extra UI Elements
library(keyring) # Access stored credentials

### Function to create/update a keyring
apde_keyring_set_f <- function(keyring = NA){
  username <- NA
  if(is.na(keyring)) {
    keyring <- dlgInput("Keyring:")$res
  }
  username <- dlgInput("Username:")$res
  if(length(keyring) == 0 || is.na(keyring)) {
    stop("Missing Keyring!")
  } else if(length(username) == 0 || is.na(username)) {
    stop("Missing Username!")
  }
  keyring::key_set(service = keyring,
                   username = username)
  message(paste0("Keyring \"", keyring, "\" has been set for username \"", username, "\""))
}


### Function to check if keyring exists, creates if not
apde_keyring_check_f <- function(keyring){
  if(nrow(key_list(keyring)) == 0) {
    message(paste0("Keyring \"", keyring, "\" does not exist."))
    apde_keyring_set_f(keyring)
  } else {
    message(paste0("Keyring \"", keyring, "\" exists."))
  }
}

apde_keyring_set_f("hhsaw")
