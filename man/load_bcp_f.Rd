% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/table_duplicate.R
\name{load_bcp_f}
\alias{load_bcp_f}
\title{Load Data to SQL Server Using BCP (Bulk Copy Program)}
\usage{
load_bcp_f(
  dataset,
  server,
  db_name,
  schema_name,
  table_name,
  user = NULL,
  pass = NULL
)
}
\arguments{
\item{dataset}{A data.frame or data.table to be loaded, or a character string 
specifying the filepath of a text file to be loaded.}

\item{server}{The name of the SQL Server instance.}

\item{db_name}{The name of the target database.}

\item{schema_name}{The name of the schema containing the target table.}

\item{table_name}{The name of the target table.}

\item{user}{Optional. The username for SQL Server authentication.}

\item{pass}{Optional. The password for SQL Server authentication.}
}
\value{
A character vector containing the output of the BCP command.
}
\description{
This function loads data into a SQL Server database table using the BCP (Bulk Copy Program) utility.
}
\details{
This function uses the BCP utility to efficiently load data into a SQL Server 
table. It supports both Windows authentication and SQL Server authentication. 
If a data.frame or data.table is provided, it is first written to a temporary 
file before being loaded.

Plans for future improvement ...
}
\examples{
 \dontrun{
  # Load data from a data.frame
  df <- data.frame(id = 1:5, name = c("Alice", "Bob", "Charlie", "David", "Eve"))
  load_bcp_f(df, "SQLSERVER01", "MyDatabase", "MySchema", "MyTable")
  
  # Load data from a file
  load_bcp_f("C:/data/mydata.txt", "SQLSERVER01", "MySchema", "dbo", "MyTable")
  
  # Load data using SQL Server authentication
  load_bcp_f(df, "SQLSERVER01", "MyDatabase", "dbo", "MyTable", 
            user = "myuser", pass = "mypassword")
 }

}
\author{
Jeremy Whitehurst, 2024-05-01
}
