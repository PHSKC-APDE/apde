% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/apde_keyring.R
\name{apde_keyring_set_f}
\alias{apde_keyring_set_f}
\title{Set or Update a Keyring}
\usage{
apde_keyring_set_f(keyring = NA)
}
\arguments{
\item{keyring}{A character string specifying the name of the keyring. If not provided, a dialog will prompt for input.}
}
\value{
A message indicating the keyring and username that have been set.
}
\description{
This function creates or updates a keyring with a specified username.
}
\examples{
\dontrun{
apde_keyring_set_f("my_keyring")
}

}
