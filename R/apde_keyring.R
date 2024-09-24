#' @title Set or Update a Keyring
#'
#' @description
#' This function creates or updates a keyring with a specified username.
#'
#' @param keyring A character string specifying the name of the keyring. If not provided, a dialog will prompt for input.
#' @return A message indicating the keyring and username that have been set.
#' @examples
#' \dontrun{
#' apde_keyring_set_f("my_keyring")
#' }
#' 
#' @importFrom keyring key_set
#' @importFrom svDialogs dlgInput
#' 
#' @export
#' 
apde_keyring_set_f <- function(keyring = NA){
  username <- NA
  if(is.na(keyring)) {
    keyring <- svDialogs::dlgInput("Keyring:")$res
  }
  username <- svDialogs::dlgInput("Username:")$res
  if(length(keyring) == 0 || is.na(keyring)) {
    stop("Missing Keyring!")
  } else if(length(username) == 0 || is.na(username)) {
    stop("Missing Username!")
  }
  keyring::key_set(service = keyring,
                   username = username)
  message(paste0("Keyring \"", keyring, "\" has been set for username \"", username, "\""))
}


#' Check and Create Keyring if Needed
#'
#' This function checks if a keyring exists and creates it if it does not.
#'
#' @param keyring A character string specifying the name of the keyring.
#' @return A message indicating whether the keyring exists or has been created.
#' @examples
#' \dontrun{
#' apde_keyring_check_f("my_keyring")
#' }
#' 
#' @importFrom keyring key_list
#' 
#' @export
#' 
apde_keyring_check_f <- function(keyring){
  if(nrow(keyring::key_list(keyring)) == 0) {
    message(paste0("Keyring \"", keyring, "\" does not exist."))
    apde_keyring_set_f(keyring)
  } else {
    message(paste0("Keyring \"", keyring, "\" exists."))
  }
}

# ### Create 'hhsaw' key only if needed
# if("hhsaw" %in% keyring::key_list()$service == FALSE){
#   apde_keyring_set_f("hhsaw")
# }