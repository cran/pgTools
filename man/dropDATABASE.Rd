% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pgTools2.R
\name{dropDATABASE}
\alias{dropDATABASE}
\title{Generate a postgreSQL DROP DATABASE statement, optionally execute the statement if con is not NULL.}
\usage{
dropDATABASE(name, if_exists = FALSE, force = FALSE, con = NULL)
}
\arguments{
\item{name}{A string, the "name" parameter for postgreSQL DROP DATABASE.}

\item{if_exists}{TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL DROP DATABASE statement.}

\item{force}{TRUE/FALSE, if TRUE, adds "FORCE" to postgreSQL DROP DATABASE statement.}

\item{con}{A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.}
}
\value{
A string, postgreSQL DROP DATABASE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
}
\description{
Generate a postgreSQL DROP DATABASE statement, optionally execute the statement if con is not NULL.
}
\examples{
dropDATABASE("dbTest01")
}
