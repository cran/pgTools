% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pgTools2.R
\name{alterTABLE}
\alias{alterTABLE}
\title{Generate a postgreSQL ALTER TABLE statement, optionally execute the statement if con is not NULL.}
\usage{
alterTABLE(
  name,
  if_exists = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  action,
  con = NULL
)
}
\arguments{
\item{name}{A string, the "name" parameter for postgreSQL ALTER TABLE statement.}

\item{if_exists}{TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL ALTER TABLE statement.}

\item{cascade}{TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL ALTER TABLE statement.}

\item{restrict}{TRUE/FALSE, if TRUE, adds "RESTRICT" to postgreSQL ALTER TABLE statement.}

\item{action}{A string, the "action" parameter for postgreSQL ALTER TABLE statement.}

\item{con}{A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.}
}
\value{
A string, postgreSQL ALTER TABLE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
}
\description{
Generate a postgreSQL ALTER TABLE statement, optionally execute the statement if con is not NULL.
}
\examples{
alterTABLE("sample", action = "ADD COLUMN IF NOT EXISTS col4 BOOLEAN")
}
