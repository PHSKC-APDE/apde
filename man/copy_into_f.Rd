% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/copy_into.R
\name{copy_into_f}
\alias{copy_into_f}
\title{Copy Data from the Data Lake to the Data Warehouse}
\usage{
copy_into_f(
  conn,
  server = NULL,
  config = NULL,
  config_url = NULL,
  config_file = NULL,
  to_schema = NULL,
  to_table = NULL,
  db_name = NULL,
  dl_path = NULL,
  file_type = c("csv", "parquet", "orc"),
  identity = NULL,
  secret = NULL,
  max_errors = 100,
  compression = c("none", "gzip", "defaultcodec", "snappy"),
  field_quote = "",
  field_term = NULL,
  row_term = NULL,
  first_row = 2,
  overwrite = T,
  rodbc = F,
  rodbc_dsn = "int_edw_16"
)
}
\arguments{
\item{conn}{SQL server connection created using \code{odbc} package}

\item{server}{server name, i.e., 'hhsaw' or 'phclaims'}

\item{config}{A object in memory with the YAML config file contents (should be blank if using config_url or config_file)}

\item{config_url}{The URL location of the YAML config file (should be blank if using config or config_file).}

\item{config_file}{The path and file name of the YAML config file (should be blank if using config or config_url).}

\item{to_schema}{schema name}

\item{to_table}{table name}

\item{db_name}{database name, e.g., "hhs_analytics_workspace", "inthealth_edw", etc.}

\item{dl_path}{The path to the data lake where the source files are located.}

\item{file_type}{file type, i.e., "csv", "parquet", or "orc".}

\item{identity}{The identity (username or account name) used for authentication when accessing the data lake.}

\item{secret}{The secret key or password associated with the identity for authentication.}

\item{max_errors}{The total number of records that can be rejected before entire file will be rejected by the system.}

\item{compression}{compression used, i.e., "none", "gzip", "defaultcodec", or "snappy".}

\item{field_quote}{The character used to quote fields in the input file (e.g., double quotes). Default is \code{field_quote = ""}}

\item{field_term}{The character or string used to separate fields in the input file (e.g., comma for CSV).}

\item{row_term}{The character or string used to separate rows in the input file (e.g., newline character).}

\item{first_row}{The row number where data begins in the input file (excluding headers if present).}

\item{overwrite}{Logical; if TRUE, truncate the table first before creating it, if it exists (default is \code{TRUE}).}

\item{rodbc}{Logical; if TRUE, use the RODBC package to run the query (avoids encoding error if using a secret key).}

\item{rodbc_dsn}{The DSN name of the RODBC connection to use with RODBC (only need to set if not using prod server).}
}
\value{
None
}
\description{
This function copies data from the data lake to the data warehouse.
}
\details{
Plans for future improvements:
- Add warning when table is about to be overwritten.
- Add other options for things we're not using (e.g., file_format).
}
\note{
Plans future improvement:
\itemize{
 \item Add warning when table is about to be overwritten
 \item Add in other options for things we're not using (e.g., file_format)
}
}
\examples{
 \dontrun{
  # ENTER EXAMPLES HERE
 }

}
\author{
Alastair Matheson, 2019-04-04
}
