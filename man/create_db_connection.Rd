% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/create_db_connection.R
\name{create_db_connection}
\alias{create_db_connection}
\title{Create Database Connection Function}
\usage{
create_db_connection(
  server = c("phextractstore", "hhsaw", "inthealth"),
  prod = T,
  interactive = F
)
}
\arguments{
\item{server}{Whether we are connecting to HHSAW, inthealth, or phextractstore}

\item{prod}{Whether the production or dev/WIP servers are to be used. Defaults to T.}

\item{interactive}{Whether to input password manually for HHSAW/inthealth or to 
use keyring. True should be used when connecting on VM, typically, or if
keyring/odbc are not set up yet for a given server. Defaults to F.

Run from master_mcaid_partial script 
https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R}
}
\description{
Create a connection to the prod or dev servers that APDE uses
}
\author{
Jeremy Whitehurst

Kai Fukutaki
}
