% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pgTools2.R
\name{dropSCHEMA}
\alias{dropSCHEMA}
\title{Generate a postgreSQL DROP SCHEMA statement, optionally execute the statement if con is not NULL.}
\usage{
dropSCHEMA(
  name,
  if_exists = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  con = NULL
)
}
\arguments{
\item{name}{A string, the "name" parameter for postgreSQL DROP SCHEMA.}

\item{if_exists}{TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL DROP SCHEMA statement.}

\item{cascade}{TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL DROP SCHEMA statement.}

\item{restrict}{TRUE/FALSE, if TRUE, adds "RESTRICT" to postgreSQL DROP SCHEMA statement.}

\item{con}{A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.}
}
\value{
A string, postgreSQL DROP SCHEMA statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
}
\description{
Generate a postgreSQL DROP SCHEMA statement, optionally execute the statement if con is not NULL.
}
\examples{
dropSCHEMA("dev")
}
