
# pg_data_types <- c(
#   "bigint",
#   "bigserial",
#   "bit",
#   "bit varying",
#   "boolean",
#   "box",
#   "bytea",
#   "character",
#   "character varying",
#   "cidr",
#   "circle",
#   "date",
#   "double precision",
#   "inet",
#   "integer",
#   "interval",
#   "json",
#   "jsonb",
#   "line",
#   "lseg",
#   "macaddr",
#   "macaddr8",
#   "money",
#   "numeric",
#   "path",
#   "pg_lsn",
#   "pg_snapshot",
#   "point",
#   "polygon",
#   "real",
#   "smallint",
#   "smallserial",
#   "serial",
#   "text",
#   "time without time zone",
#   "time with time zone",
#   "timestamp without time zone",
#   "timestamp with time zone",
#   "tsquery",
#   "tsvector",
#   "txid_snapshot",
#   "uuid",
#   "xml"
# )
#
# save(
#   pg_data_types,
#   file = "/home/tim/r_packages/pgTools/R/sysdata.rda"
# )

#' PostgreSQL data types
#'
#' @description A vector of PostgreSQL data types
#' @format A vector
"pg_data_types"

#' Write a PostgreSQL array as a string from a vector.
#'
#' @param x A vector.
#' @param double_quote TRUE/FALSE, if TRUE, the elements of x will be double quoted.
#' @return A string.
#' @examples
#' vecToArrayStr(c("a", "b"))
vecToArrayStr <- function(x, double_quote = TRUE){
  return(paste0("{", paste0(if(double_quote){doubleQuoteText(x)}else{x}, collapse = ", "), "}"))
}

#' Write a PostgreSQL array as a string from a vector.
#'
#' @param x A vector.
#' @return A string.
#' @examples
#' arrayStrToVec(vecToArrayStr(c("a", "b")))
arrayStrToVec <- function(x){
  return(strsplit(gsub("\\{|\\}", "", x), ",")[[1]])
}

#' Connect to a local database with local credentials using DBI/odbc.
#'
#' @param db A string, a database you can connect to locally.
#' @return A database connection.
#' @examples
#' connect(NULL)
connect <- function(db){
  if(is.null(db)){
    warning("db should not be NULL")
    return()
  }else{
    return(
      odbc::dbConnect(
        odbc::odbc(),
        driver = "PostgreSQL ANSI",
        database = db,
        host = "localhost",
        port = 5432
      )
    )
  }
}

#' Get the PostgreSQL data type for a given R data type.
#'
#' @param x A string, a R data type.
#' @return A string, the PostgreSQL data type for x.
#' @examples
#' sqlTypeWalk(100.1209)
sqlTypeWalk <- function(x){
  if (is.factor(x)) return("TEXT")
  if (inherits(x, "POSIXt")) return("TIMESTAMP WITH TIME ZONE")
  if (inherits(x, "Date")) return("DATE")
  if (inherits(x, "difftime")) return("TIME")
  if (inherits(x, "integer64")) return("BIGINT")
  switch(
    typeof(x),
    integer = "INTEGER",
    double = "DOUBLE PRECISION",
    character = "TEXT",
    logical = "BOOLEAN",
    list = "BYTEA",
    stop("Unsupported type", call. = FALSE)
  )
}

#' Convert a column name into a PostgreSQL compatible name.
#'
#' @param x A string, a column name.
#' @param double_quote TRUE/FALSE, if true, will add double quotes rather than replace non-compatible characters with underscores.
#' @return A string, a PostgreSQL compatible column name.
#' @examples
#' sqlNameWalk("column 100 - sample b")
sqlNameWalk <- function(
  x,
  double_quote = FALSE
){
  return(
    if(double_quote == TRUE){
      doubleQuoteText(x, char_only = FALSE)
    }else{
      gsub(
        "_+",
        "_",
        gsub(
          pattern = "[^[:alnum:]]",
          replacement = "_",
          x = x
        )
      )
    }
  )
}

#' Add a single line SQL comment.
#'
#' @param x A string.
#' @return A string prefixed with "--".
#' @examples
#' sql_comment("Sample single line comment.")
sql_comment <- function(
  x
){
  return(paste(paste0("--", x), sep = "", collapse = "\n"))
}

#' Add a 80 char SQL comment, intended to be used for visual breaks in documents.
#'
#' @return A string, 80 chars of "-".
#' @examples
#' sql_80_char_comment()
sql_80_char_comment <- function(){
  return(paste0(rep("-", 80), collapse = ""))
}

#' Create a SQL script, optionally execute the statement if con is not NULL.
#'
#' @param ... A string, SQL command to be combined into one document or statement.
#' @param path A string, the file path (include the file name) to save the script.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, SQL commands combined into one document or statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' create_sql_script(
#' createSCHEMA("dev"),
#' sql_80_char_comment(),
#' createTABLE(name = "sample",
#' columns = list(col1 = "SERIAL NOT NULL", col2 = "INTEGER", col3 = "TEXT"),
#' constraints = list(sample_constraint = "UNIQUE(col3)")
#' ))
create_sql_script <- function(
  ...,
  path = NULL,
  con = NULL
){
  x <- paste(..., sep = "\n\n")
  if(is.null(path) == FALSE){
    writeLines(x, path)
  }
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL CREATE DATABASE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL CREATE DATABASE.
#' @param owner A string, the "user_name" parameter for PostgreSQL CREATE DATABASE.
#' @param template A string, the "template" parameter for PostgreSQL CREATE DATABASE.
#' @param encoding A string, the "encoding" parameter for PostgreSQL CREATE DATABASE.
#' @param locale A string, the "locale" parameter for PostgreSQL CREATE DATABASE
#' @param lc_collate A string, the "lc_collate" parameter for PostgreSQL CREATE DATABASE.
#' @param lc_ctype A string, the "lc_ctype" parameter for PostgreSQL CREATE DATABASE.
#' @param tablespace A string, the "tablespace_name" parameter for PostgreSQL CREATE DATABASE.
#' @param allow_connections A string, the "allowconn" parameter for PostgreSQL CREATE DATABASE.
#' @param connection_limit A string, the "connlimit" parameter for PostgreSQL CREATE DATABASE.
#' @param is_template A string, the "istemplate" parameter for PostgreSQL CREATE DATABASE.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL CREATE DATABASE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' createDATABASE("dbTest01")
createDATABASE <- function(
  name,
  owner = NULL,
  template = NULL,
  encoding = NULL,
  locale = NULL,
  lc_collate = NULL,
  lc_ctype = NULL,
  tablespace = NULL,
  allow_connections = NULL,
  connection_limit = NULL,
  is_template = NULL,
  con = NULL
){
  with <- c(
    "OWNER" = owner,
    "TEMPLATE" = template,
    "ENCODING" = encoding,
    "LOCALE" = locale,
    "LC_COLLATE" = lc_collate,
    "LC_CTYPE" = lc_ctype,
    "TABLESPACE" = tablespace,
    "ALLOW_CONNECTIONS" = allow_connections,
    "CONNECTION LIMIT" = connection_limit,
    "IS_TEMPLATE" = is_template
  )
  if(is.null(with) == FALSE){
    sql_with <- paste(names(with), with, sep = " = ", collapse = ", \n\t")
    sql <- paste0(paste(paste("CREATE DATABASE", name), "WITH", sql_with, sep = "\n\t"), ";")
  }else{
    sql <- paste0(paste("CREATE DATABASE", name), ";")
  }
  x <- paste(
    paste(sql, sep = "", collapse = "\n"),
    sep = "", collapse = "\n"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL ALTER DATABASE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL ALTER DATABASE.
#' @param allow_connections A string, the "allowconn" parameter for PostgreSQL ALTER DATABASE.
#' @param connection_limit A string, the "connlimit" parameter for PostgreSQL ALTER DATABASE.
#' @param is_template A string, the "istemplate" parameter for PostgreSQL ALTER DATABASE.
#' @param rename_to A string, the "new_name" parameter for PostgreSQL ALTER DATABASE.
#' @param owner_to A string, the "new_owner" parameter for PostgreSQL ALTER DATABASE.
#' @param set_tablespace A string, the "new_tablespace" parameter for PostgreSQL ALTER DATABASE.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL ALTER DATABASE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' alterDATABASE("dbTest01", rename_to = "dbProd01")
alterDATABASE <- function(
  name,
  allow_connections = NULL,
  connection_limit = NULL,
  is_template = NULL,
  rename_to = NULL,
  owner_to = NULL,
  set_tablespace = NULL,
  con = NULL
){
  with <- c("ALLOW_CONNECTIONS" = allow_connections, "CONNECTION LIMIT" = connection_limit, "IS_TEMPLATE" = is_template)
  if(is.null(with) == FALSE){
    a1 <- paste(names(with), with, sep = " = ", collapse = "\n\t")
    a1 <- paste0(paste(paste("ALTER DATABASE", name), "WITH", with, sep = "\n\t"), ";")
  }else{
    a1 <- NULL
  }
  if(is.null(rename_to) == FALSE){
    a2 <- paste0(paste("ALTER DATABASE", name, "RENAME TO", rename_to), ";")
  }else{
    a2 <- NULL
  }
  if(is.null(owner_to) == FALSE){
    a3 <- paste0(paste("ALTER DATABASE", name, "OWNER TO", owner_to), ";")
  }else{
    a3 <- NULL
  }
  if(is.null(set_tablespace) == FALSE){
    a4 <- paste0(paste("ALTER DATABASE", name, "SET TABLESPACE", set_tablespace), ";")
  }else{
    a4 <- NULL
  }
  x <- paste(
    paste(paste(c(a1, a2, a3, a4), sep = "", collapse = "\n"), sep = "", collapse = "\n"),
    sep = "", collapse = "\n"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL DROP DATABASE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL DROP DATABASE.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to PostgreSQL DROP DATABASE statement.
#' @param force TRUE/FALSE, if TRUE, adds "FORCE" to PostgreSQL DROP DATABASE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL DROP DATABASE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' dropDATABASE("dbTest01")
dropDATABASE <- function(
  name,
  if_exists = FALSE,
  force = FALSE,
  con = NULL
){
  x <- paste(
    paste(paste0("DROP DATABASE ", if(if_exists == TRUE){"IF EXISTS "}else{""}, name, if(force == TRUE){" FORCE;"}else{";"}), sep = "", collapse = "\n"),
    sep = "", collapse = "\n"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL CREATE SCHEMA statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "schema_name" parameter for PostgreSQL CREATE SCHEMA.
#' @param authorization A string, the "role_specification" parameter for PostgreSQL CREATE SCHEMA.
#' @param if_not_exists TRUE/FALSE, if TRUE, adds "IF NOT EXISTS" to PostgreSQL CREATE SCHEMA statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL CREATE SCHEMA statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' createSCHEMA("dev")
createSCHEMA <- function(
  name,
  authorization = NULL,
  if_not_exists = FALSE,
  con = NULL
){
  x <- paste(paste0("CREATE SCHEMA ", if(if_not_exists == TRUE){"IF NOT EXISTS "}else{""}, name, if(is.null(authorization) == FALSE){paste0(" AUTHORIZATION ", authorization, ";")}else{";"}), sep = "", collapse = "\n")
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL ALTER SCHEMA statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL ALTER SCHEMA.
#' @param rename_to A string, the "new_name" parameter for PostgreSQL ALTER SCHEMA.
#' @param owner_to A string, the "new_owner" parameter for PostgreSQL ALTER SCHEMA.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL CREATE SCHEMA statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' alterSCHEMA("dev", rename_to = "prod")
alterSCHEMA <- function(
  name,
  rename_to = NULL,
  owner_to = NULL,
  con = NULL
){
  alt <- c("RENAME TO" = rename_to, "OWNER TO" = owner_to)
  alt <- paste(names(alt), alt)
  x <- paste(
    paste(
      paste0("ALTER SCHEMA ", name, " ", alt, ";"),
      sep = " ",
      collapse = "\n"
    ),
    sep = "", collapse = "\n"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL DROP SCHEMA statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL DROP SCHEMA.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to PostgreSQL DROP SCHEMA statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to PostgreSQL DROP SCHEMA statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to PostgreSQL DROP SCHEMA statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL DROP SCHEMA statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' dropSCHEMA("dev")
dropSCHEMA <- function(
  name,
  if_exists = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  con = NULL
){
  x <- paste(
    paste0("DROP SCHEMA ", if(if_exists == TRUE){"IF EXISTS "}else{""}, name, if(cascade == TRUE){" CASCADE"}else{""}, if(restrict == TRUE){" RESTRICT"}else{""}, ";"),
    sep = "", collapse = "\n"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL CREATE EXTENSION statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "extension_name" parameter for PostgreSQL CREATE EXTENSION.
#' @param if_not_exists TRUE/FALSE, if TRUE, adds "IF NOT EXISTS" to PostgreSQL CREATE EXTENSION statement.
#' @param schema A string, the "schema_name" parameter for PostgreSQL CREATE EXTENSION.
#' @param version A string, the "version" parameter for PostgreSQL CREATE EXTENSION.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to PostgreSQL CREATE EXTENSION statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL CREATE EXTENSION statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' createEXTENSION("pgcrypto")
createEXTENSION <- function(
  name,
  if_not_exists = FALSE,
  schema = NULL,
  version = NULL,
  cascade = FALSE,
  con = NULL
){
  x <- paste(
    paste0(
      "CREATE EXTENSION ",
      if(if_not_exists == TRUE){"IF NOT EXISTS "}else{""},
      name,
      if(is.null(schema) == FALSE){paste0(" SCHEMA ", schema)}else{""},
      if(is.null(version) == FALSE){paste0(" VERSION ", version)}else{""},
      if(cascade == TRUE){" CASCADE"}else{""},
      ";"
    ),
    sep = "", collapse = "\n"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL DROP EXTENSION statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL DROP EXTENSION.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to PostgreSQL DROP EXTENSION statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to PostgreSQL DROP EXTENSION statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to PostgreSQL DROP EXTENSION statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL DROP EXTENSION statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' dropEXTENSION("pgcrypto")
dropEXTENSION <- function(
  name,
  if_exists = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  con = NULL
){
  x <- paste(
    paste0("DROP EXTENSION ", if(if_exists == TRUE){"IF EXISTS "}else{""}, name, if(cascade == TRUE){" CASCADE"}else{""}, if(restrict == TRUE){" RESTRICT"}else{""}, ";"),
    sep = "", collapse = "\n"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL CREATE TABLE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "table_name" parameter for PostgreSQL CREATE TABLE.
#' @param columns A named list, names are the SQL column names, values are strings with the SQL column data types, constraints, etc.
#' @param select A string, the select statement to use to create the table.
#' @param constraints A named list, names are the SQL constraint names, values are strings with the SQL constraint.
#' @param temporary TRUE/FALSE, if TRUE, adds "TEMPORARY" to PostgreSQL CREATE TABLE statement.
#' @param unlogged TRUE/FALSE, if TRUE, adds "UNLOGGED" to PostgreSQL CREATE TABLE statement.
#' @param if_not_exists TRUE/FALSE, if TRUE, adds "IF NOT EXISTS" to PostgreSQL CREATE TABLE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL CREATE TABLE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' createTABLE(
#' name = "sample",
#' columns = list(col1 = "SERIAL NOT NULL", col2 = "INTEGER", col3 = "TEXT"),
#' constraints = list(sample_constraint = "UNIQUE(col3)")
#' )
createTABLE <- function(
  name,
  columns,
  select = NULL,
  constraints = NULL,
  temporary = FALSE,
  if_not_exists = FALSE,
  unlogged = FALSE,
  con = NULL
){
  if(is.null(select) == FALSE){
    x <- paste0(
      "CREATE ", if(temporary == TRUE){"TEMPORARY "}else{""}, if(unlogged){"UNLOGGED "}else{""}, "TABLE ", if(if_not_exists == TRUE){"IF NOT EXISTS "}else{""}, name, "\n",
      "AS\n",
      select,
      "\n;"
    )
  }else{
    columns <- lapply(columns, paste, sep = " ", collapse = " ")
    columns <- paste(names(columns), columns, sep = " ")
    x <- paste0(
      "CREATE ", if(temporary == TRUE){"TEMPORARY "}else{""}, if(unlogged){"UNLOGGED "}else{""}, "TABLE ", if(if_not_exists == TRUE){"IF NOT EXISTS "}else{""}, name, "\n",
      "(\n",
      paste(columns, sep = " ", collapse = ",\n"),
      if(is.null(constraints) == FALSE){paste0(",\n", paste(paste("CONSTRAINT", names(constraints), constraints), sep = " ", collapse = ",\n"))}else{""},
      "\n);"
    )
  }
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL ALTER TABLE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL ALTER TABLE statement.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to PostgreSQL ALTER TABLE statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to PostgreSQL ALTER TABLE statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to PostgreSQL ALTER TABLE statement.
#' @param action A string or vector of strings, the "action" parameter for PostgreSQL ALTER TABLE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL ALTER TABLE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' alterTABLE("sample", action = c("ADD COLUMN IF NOT EXISTS col4 BOOLEAN"))
alterTABLE <- function(
  name,
  if_exists = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  action,
  con = NULL
){
  x <- paste(
    paste0(
      "ALTER TABLE ",
      if(if_exists == TRUE){"IF EXISTS "}else{""},
      name,
      if(cascade == TRUE){" CASCADE"}else{""},
      if(restrict == TRUE){" RESTRICT"}else{""},
      " ",
      action,
      ";"
    ),
    sep = "", collapse = "\n"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL DROP TABLE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL DROP TABLE.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to PostgreSQL DROP TABLE statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to PostgreSQL DROP TABLE statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to PostgreSQL DROP TABLE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL DROP TABLE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' dropTABLE("sample")
dropTABLE <- function(
  name,
  if_exists = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  con = NULL
){
  x <- paste(
    paste0(
      "DROP TABLE ",
      if(if_exists == TRUE){"IF EXISTS "}else{""},
      name,
      if(cascade == TRUE){" CASCADE"}else{""},
      if(restrict == TRUE){" RESTRICT"}else{""},
      ";"
    ),
    sep = "", collapse = "\n"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL CREATE PROCEDURE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL CREATE PROCEDURE.
#' @param args A named list, names are the argument names, values are strings with the argument data types.
#' @param or_replace TRUE/FALSE, if TRUE, adds "OR REPLACE" to PostgreSQL CREATE PROCEDURE statement.
#' @param language A string, the "language" parameter for PostgreSQL CREATE PROCEDURE.
#' @param definition A string, the "definition" parameter for PostgreSQL CREATE PROCEDURE.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL CREATE PROCEDURE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' createPROCEDURE(
#' name = "sample",
#' args = list(a = "INTEGER", b = "TEXT"),
#' definition = "INSERT INTO tbl(col1, col2) VALUES (a, b);"
#' )
createPROCEDURE <- function(
  name,
  args = NULL,
  or_replace = FALSE,
  language = "SQL",
  definition,
  con = NULL
){
  x <- paste0(
    "CREATE ", if(or_replace == TRUE){"OR REPLACE "}else{""}, "PROCEDURE ",
    name,
    "(", if(is.null(args) == FALSE){paste(names(args), args, sep = " ", collapse = ", ")}else{""}, ")", " \n",
    "LANGUAGE ", language, "\n",
    "AS $$\n",
    definition, "\n",
    "$$;"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL DROP PROCEDURE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL DROP PROCEDURE.
#' @param args A named list, names are the argument names, values are strings with the argument data types.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to PostgreSQL DROP PROCEDURE statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to PostgreSQL DROP PROCEDURE statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to PostgreSQL DROP PROCEDURE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL DROP PROCEDURE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' dropPROCEDURE(
#' name = "sample",
#' args = list(a = "INTEGER", b = "TEXT")
#' )
dropPROCEDURE <- function(
  name,
  args = NULL,
  if_exists = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  con = NULL
){
  x <- paste0(
    "DROP PROCEDURE ", if(if_exists == TRUE){"IF EXISTS "}else{""},
    name,
    "(", if(is.null(args) == FALSE){paste(names(args), args, sep = " ", collapse = ", ")}else{""}, ")",
    if(cascade == TRUE){" CASCADE"}else{""},
    if(restrict == TRUE){" RESTRICT"}else{""},
    ";"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL CREATE FUNCTION statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL CREATE FUNCTION.
#' @param args A named list, names are the argument names, values are strings with the argument data types.
#' @param or_replace TRUE/FALSE, if TRUE, adds "OR REPLACE" to PostgreSQL CREATE FUNCTION statement.
#' @param returns A string, the "returns" parameter for PostgreSQL CREATE FUNCTION.
#' @param returns_table A named list, names are the return table column names, values are strings with the return table data types.
#' @param language A string, the "language" parameter for PostgreSQL CREATE FUNCTION.
#' @param definition A string, the "definition" parameter for PostgreSQL CREATE FUNCTION.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL CREATE PROCEDURE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' createFUNCTION(
#' name = "sample_add",
#' args = list(a = "INTEGER", b = "INTEGER"),
#' returns = "INT",
#' language = "plpgsql",
#' definition = "BEGIN RETURN sample_add.a + sample_add.b; END;"
#' )
createFUNCTION <- function(
  name,
  args = NULL,
  or_replace = FALSE,
  returns = NULL,
  returns_table = NULL,
  language = "SQL",
  definition,
  con = NULL
){
  x <- paste0(
    "CREATE ", if(or_replace == TRUE){"OR REPLACE "}else{""}, "FUNCTION ",
    name,
    "(", if(is.null(args) == FALSE){paste(names(args), args, sep = " ", collapse = ", ")}else{""}, ")", " \n",
    if(is.null(returns) == FALSE){paste0("RETURNS ", returns, "\n")}else{""},
    if(is.null(returns_table) == FALSE){paste0("RETURNS TABLE (", paste(names(returns_table), returns_table, sep = " ", collapse = ", "), ")", "\n")}else{""},
    "LANGUAGE ", language, "\n",
    "AS $$\n",
    definition, "\n",
    "$$;"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL DROP FUNCTION statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL DROP FUNCTION.
#' @param args A named list, names are the argument names, values are strings with the argument data types.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to PostgreSQL DROP FUNCTION statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to PostgreSQL DROP FUNCTION statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to PostgreSQL DROP FUNCTION statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL DROP FUNCTION statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' dropFUNCTION(
#' name = "sample",
#' args = list(a = "INTEGER", b = "TEXT")
#' )
dropFUNCTION <- function(
  name,
  args = NULL,
  if_exists = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  con = NULL
){
  x <- paste0(
    "DROP FUNCTION ", if(if_exists == TRUE){"IF EXISTS "}else{""},
    name,
    "(", if(is.null(args) == FALSE){paste(names(args), args, sep = " ", collapse = ", ")}else{""}, ")",
    if(cascade == TRUE){" CASCADE"}else{""},
    if(restrict == TRUE){" RESTRICT"}else{""},
    ";"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL CREATE TRIGGER statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL CREATE TRIGGER.
#' @param when A string, the "when" parameter (BEFORE, AFTER, INSTEAD OF) for PostgreSQL CREATE TRIGGER.
#' @param event A string, the "event" parameter (INSERT/UPDATE/DELETE/TRUNCATE) for PostgreSQL CREATE TRIGGER.
#' @param on A string, the "table_name" parameter for PostgreSQL CREATE TRIGGER.
#' @param for_each_row TRUE/FALSE, if TRUE, adds "FOR EACH ROW" to PostgreSQL CREATE TRIGGER statement.
#' @param func A string, the function call to be executed by the PostgreSQL CREATE TRIGGER.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL CREATE TRIGGER statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' createTRIGGER(
#' name = "sample_trigger",
#' when = "AFTER",
#' event = "INSERT",
#' on = "sample_table",
#' for_each_row = TRUE,
#' func = "function_sample()"
#' )
createTRIGGER <- function(
  name,
  when,
  event,
  on,
  for_each_row = FALSE,
  func,
  con = NULL
){
  x <- paste0(
    "CREATE TRIGGER ", name, "\n",
    when, " ", event, " ON ", on, "\n",
    if(for_each_row == TRUE){"FOR EACH ROW\n"}else{""},
    "EXECUTE FUNCTION ", func, ";"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL DROP TRIGGER statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for PostgreSQL DROP TRIGGER.
#' @param on A string, the "table_name" parameter for PostgreSQL DROP TRIGGER.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to PostgreSQL DROP TRIGGER statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to PostgreSQL DROP TRIGGER statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to PostgreSQL DROP TRIGGER statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL DROP TRIGGER statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' dropTRIGGER(
#' name = "sample_trigger",
#' on = "sample_table"
#' )
dropTRIGGER <- function(
  name,
  on,
  if_exists = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  con = NULL
){
  x <- paste0(
    "DROP TRIGGER ", if(if_exists == TRUE){"IF EXISTS "}else{""},
    name,
    " ON ", on,
    if(cascade == TRUE){" CASCADE"}else{""},
    if(restrict == TRUE){" RESTRICT"}else{""},
    ";"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Add single quotes to strings using stringi::stri_join, useful for converting R strings into SQL formatted strings.
#'
#' @param x A string.
#' @param char_only TRUE/FALSE, if TRUE, adds quotes only if is.character(x) is TRUE.
#' @param excluded_chars A character vector, will not add quotes if a value is in excluded_chars.
#' @return A string, with single quotes added to match PostgreSQL string formatting.
#' @examples
#' quoteText2("Sample quotes.")
quoteText2 <- function(
  x,
  char_only = TRUE,
  excluded_chars = c("NULL")
){
  if(char_only == TRUE){
    x[is.character(x) & !(x %in% excluded_chars) & !(is.null(x) == TRUE|is.na(x) == TRUE)] <- stringi::stri_join("'", gsub("\'", "''", x[is.character(x) & !(x %in% excluded_chars) & !(is.null(x) == TRUE|is.na(x) == TRUE)], fixed = TRUE), "'")
  }else{
    x[!(x %in% excluded_chars) & !(is.null(x) == TRUE|is.na(x) == TRUE)] <- stringi::stri_join("'", gsub("\'", "''", x[!(x %in% excluded_chars) & !(is.null(x) == TRUE|is.na(x) == TRUE)], fixed = TRUE), "'")
  }
  x[is.null(x) == TRUE|is.na(x) == TRUE] <- "NULL"
  return(
    x
  )
}

#' Helper function for INSERT
#'
#' @param x A vector of data to insert.
#' @param n_batches Integer, the number of batches needed to insert the data.
#' @param batch_size Integer, the size of each batch.
#' @return A list.
#' @examples
#' insert_batch_chunker(c(1, 2, 3), 1, 100)
insert_batch_chunker <- function(x, n_batches, batch_size){
  i <- seq_len(n_batches)
  starts <- batch_size*i - batch_size + 1
  ends <- batch_size*i
  str <- paste0("list(", paste0(shQuote(i), " = x[", starts, ":", ends,"]", collapse = ", "), ")")
  return(eval(str2expression(str)))
}

#' Helper function for INSERT
#'
#' @param x A data table
#' @param n_batches Integer, the number of batches needed to insert the data.
#' @param batch_size Integer, the size of each batch.
#' @return A list.
#' @examples
#' insert_table_chunker(as.data.table(list(c1 = c(1, 2, 3))), 1, 100)
insert_table_chunker <- function(x, n_batches, batch_size){
  i <- seq_len(n_batches)
  starts <- batch_size*i - batch_size + 1
  ends <- batch_size*i
  str <- paste0("list(", paste0(shQuote(i), " = x[", starts, ":", ends,", ]", collapse = ", "), ")")
  return(eval(str2expression(str)))
}

#' Generate a PostgreSQL INSERT statement, optionally execute the statement if con is not NULL.
#'
#' @param x A list, data.frame or data.table, names must match the column names of the destination SQL table.
#' @param schema A string, the schema name of the destination SQL table.
#' @param table A string, the table name of the destination SQL table.
#' @param types A vector of character strings specifying the SQL data types of the destination columns, the position of the type should match the position of the column for that type in x. Required if prepare or cast is TRUE.
#' @param returning A vector of character strings specifying the SQL column names to be returned by the INSERT statement.
#' @param quote_text TRUE/FALSE, if TRUE, calls quoteText() to add single quotes around character strings.
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the data to be inserted to the specified type.
#' @param prepare TRUE/FALSE, if TRUE, creates a PostgreSQL prepared statement for inserting the data.
#' @param batch_size Integer, the maximum number of records to submit in one statement.
#' @param double_quote_names TRUE/FALSE, if TRUE, adds double quotes to column names.
#' @param select A string, a SELECT statement.
#' @param select_cols A character vector of the columns to insert the results of the select statement. Only used if select is not NULL.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @param n_cores A integer, the number of cores to use for parallel forking (passed to parallel::mclapply as mc.cores).
#' @param table_is_temporary TRUE/FALSE, if TRUE, prevents parallel processing.
#' @param retain_insert_order TRUE/FALSE, if TRUE, prevents parallel processing.
#' @param connect_db_name The name of the database to pass to connect() when inserting in parallel.
#' @return A string, PostgreSQL INSERT statement; or a string, PostgreSQL prepared statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' INSERT(
#' x = list(col1 = c("a", "b", "c"), col2 = c(1, 2, 3)),
#' schema = "test",
#' table = "table1",
#' prepare = TRUE,
#' types = c("TEXT", "INTEGER"),
#' returning = NULL,
#' quote_text = TRUE,
#' cast = TRUE
#' )
INSERT <- function(
  x = NULL,
  schema = NULL,
  table,
  types = NULL,
  returning = NULL,
  quote_text = TRUE,
  cast = TRUE,
  prepare = TRUE,
  batch_size = 50000,
  double_quote_names = FALSE,
  select = NULL,
  select_cols = NULL,
  con = NULL,
  n_cores = 1,
  table_is_temporary = FALSE,
  retain_insert_order = FALSE,
  connect_db_name = NULL
){
  if(is.null(select)){
    x <- as.list(x)
    x_names <- names(x)
    if(double_quote_names){
      x_names <- doubleQuoteText(names(x), char_only = FALSE)
    }else{
      x_names <- names(x)
    }
    if(is.null(types)){
      types <- unlist(lapply(x, function(y){
        sqlTypeWalk(typeof(y))
      }))
    }
    n_cols <- length(types)
    i <- seq_len(n_cols)
    n_rows <- length(x[[1]])
    n_batches <- ceiling(n_rows/batch_size)
    if(!prepare){
      parenthesis <- rep_len('"),"', n_cols)
      parenthesis[n_cols] <- '")"'
      if(quote_text & cast){
        str <- stringi::stri_join(
          'stringi::stri_join("(",', stringi::stri_join('"CAST(", quoteText2(x[[', i, ']]), " AS ", types[[', i, ']], ', parenthesis, collapse = ","), ', ")")'
        )
      }else if(cast){
        str <- stringi::stri_join(
          'stringi::stri_join("(",', stringi::stri_join('"CAST(", x[[', i, ']], " AS ", types[[', i, ']], ', parenthesis, collapse = ","), ',")")'
        )
      }else if(quote_text){
        str <- stringi::stri_join(
          'stringi::stri_join("(",', stringi::stri_join('quoteText2(x[[', i, ']])', collapse = ", ',', "), ', ")")'
        )
      }else{
        str <- stringi::stri_join(
          'stringi::stri_join("(",', stringi::stri_join('x[[', i, ']]', collapse = ", ',', "), ', ")")'
        )
      }
      insert <- eval(str2expression(str))
      rm(x)
      if(is.null(con)){
        return(
          stringi::stri_join(
            "INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, " (", stringi::stri_join(x_names, collapse = ", "), ")",
            " VALUES \n", stringi::stri_join(insert, collapse = ",\n"),
            if(is.null(returning)){""}else{stringi::stri_join(" \nRETURNING ", stringi::stri_join(if(double_quote_names){doubleQuoteText(returning)}else{returning}, collapse = ", "))},
            ";"
          )
        )
      }else{
        n_batches <- ceiling(length(insert)/batch_size)
        if(n_batches == 1){
          return(
            as.data.table(DBI::dbGetQuery(
              con,
              stringi::stri_join(
                "INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, " (", stringi::stri_join(x_names, collapse = ", "), ")",
                " VALUES \n", stringi::stri_join(insert, collapse = ",\n"),
                if(is.null(returning)){""}else{stringi::stri_join(" \nRETURNING ", stringi::stri_join(if(double_quote_names){doubleQuoteText(returning)}else{returning}, collapse = ", "))},
                ";"
              )
            ))
          )
        }else{
          insert_batches <- insert_batch_chunker(insert, n_batches, batch_size)
          if(n_cores > 1 & !table_is_temporary & !retain_insert_order){
            parallel::mclapply(insert_batches, function(i){
              con <- connect(connect_db_name)
              DBI::dbGetQuery(
                conn = con,
                statement = stringi::stri_join(
                  "INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, " (", stringi::stri_join(x_names, collapse = ", "), ")",
                  " VALUES \n", stringi::stri_join(i, collapse = ",\n"),
                  if(is.null(returning)){""}else{stringi::stri_join(" \nRETURNING ", stringi::stri_join(if(double_quote_names){doubleQuoteText(returning)}else{returning}, collapse = ", "))},
                  ";"
                )
              )
              DBI::dbDisconnect(con)
              return()
            },
            mc.cores = n_cores)
          }else{
            lapply(insert_batches, function(i){
              DBI::dbGetQuery(
                conn = con,
                statement = stringi::stri_join(
                  "INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, " (", stringi::stri_join(x_names, collapse = ", "), ")",
                  " VALUES \n", stringi::stri_join(i, collapse = ",\n"),
                  if(is.null(returning)){""}else{stringi::stri_join(" \nRETURNING ", stringi::stri_join(if(double_quote_names){doubleQuoteText(returning)}else{returning}, collapse = ", "))},
                  ";"
                )
              )
              return()
            })
          }
          return()
        }
      }
    }else{
      if(n_cores == 1 | table_is_temporary | retain_insert_order | is.null(con)){
        prep_id <- stringi::stri_join("insert_", sampleStr(16))
        if(is.null(con) == FALSE){
          existing_statements <- DBI::dbGetQuery(
            conn = con,
            "SELECT name FROM pg_prepared_statements"
          )$name
          while(prep_id %in% existing_statements){
            prep_id <- stringi::stri_join("insert_", sampleStr(16))
          }
        }
        sql_prep <- stringi::stri_join("PREPARE ", prep_id, " (", stringi::stri_join(types, collapse = ", "), ") AS INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, "(", paste0(x_names, collapse = ", "), ") VALUES (", paste0("$", i, collapse = ", "), ")", if(is.null(returning) == FALSE){paste0(" RETURNING ", paste0(returning, collapse = ", "))}else{""}, ";")
        sql_deallocate <- stringi::stri_join("DEALLOCATE ", prep_id, ";")
        parenthesis <- rep_len('"),"', n_cols)
        parenthesis[n_cols] <- '")"'
        if(quote_text & cast){
          str <- stringi::stri_join(
            'stringi::stri_join("EXECUTE ", ', shQuote(prep_id), ', "(",', stringi::stri_join('"CAST(", quoteText2(x[[', i, ']]), " AS ", types[[', i, ']], ', parenthesis, collapse = ","), ', ");")'
          )
        }else if(cast){
          str <- stringi::stri_join(
            'stringi::stri_join("EXECUTE ", ', shQuote(prep_id), ', "(",', stringi::stri_join('"CAST(", x[[', i, ']], " AS ", types[[', i, ']], ', parenthesis, collapse = ","), ',");")'
          )
        }else if(quote_text){
          str <- stringi::stri_join(
            'stringi::stri_join("EXECUTE ", ', shQuote(prep_id), ', "(",', stringi::stri_join('quoteText2(x[[', i, ']])', collapse = ", ',', "), ', ");")'
          )
        }else{
          str <- stringi::stri_join(
            'stringi::stri_join("EXECUTE ", ', shQuote(prep_id), ', "(",', stringi::stri_join('x[[', i, ']]', collapse = ", ',', "), ', ");")'
          )
        }
        insert <- eval(str2expression(str))
        rm(x)
        if(is.null(con)){
          return(
            stringi::stri_join(
              sql_prep, "\n",
              stringi::stri_join(insert, collapse = "\n"), "\n",
              sql_deallocate
            )
          )
        }else{
          prep <- DBI::dbSendQuery(
            conn = con,
            statement = sql_prep
          )
          DBI::dbClearResult(prep)
          if(n_batches > 1){
            insert_batches <- insert_batch_chunker(insert, n_batches, batch_size)
            lapply(insert_batches, function(i){
              DBI::dbGetQuery(
                conn = con,
                statement = stringi::stri_join(i, collapse = "\n")
              )
              return()
            })
            deallocate <- DBI::dbSendQuery(
              conn = con,
              statement = sql_deallocate
            )
            DBI::dbClearResult(deallocate)
            return()
          }else{
            res <- as.data.table(DBI::dbGetQuery(
              con,
              stringi::stri_join(insert, collapse = "\n")
            ))
            deallocate <- DBI::dbSendQuery(
              conn = con,
              statement = sql_deallocate
            )
            DBI::dbClearResult(deallocate)
            return(
              res
            )
          }
        }
      }else if(n_cores > 1 & !table_is_temporary & !retain_insert_order & !is.null(con)){
        insert_tables <- insert_table_chunker(as.data.table(x), n_batches, batch_size)
        rm(x)
        parallel::mclapply(insert_tables, function(x){
          con <- connect(connect_db_name)
          prep_id <- stringi::stri_join("insert_", sampleStr(16))
          existing_statements <- DBI::dbGetQuery(
            conn = con,
            "SELECT name FROM pg_prepared_statements"
          )$name
          while(prep_id %in% existing_statements){
            prep_id <- stringi::stri_join("insert_", sampleStr(16))
          }
          sql_prep <- stringi::stri_join("PREPARE ", prep_id, " (", stringi::stri_join(types, collapse = ", "), ") AS INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, "(", paste0(x_names, collapse = ", "), ") VALUES (", paste0("$", i, collapse = ", "), ")", if(is.null(returning) == FALSE){paste0(" RETURNING ", paste0(returning, collapse = ", "))}else{""}, ";")
          sql_deallocate <- stringi::stri_join("DEALLOCATE ", prep_id, ";")
          parenthesis <- rep_len('"),"', n_cols)
          parenthesis[n_cols] <- '")"'
          if(quote_text & cast){
            str <- stringi::stri_join(
              'stringi::stri_join("EXECUTE ", ', shQuote(prep_id), ', "(",', stringi::stri_join('"CAST(", quoteText2(x[[', i, ']]), " AS ", types[[', i, ']], ', parenthesis, collapse = ","), ', ");")'
            )
          }else if(cast){
            str <- stringi::stri_join(
              'stringi::stri_join("EXECUTE ", ', shQuote(prep_id), ', "(",', stringi::stri_join('"CAST(", x[[', i, ']], " AS ", types[[', i, ']], ', parenthesis, collapse = ","), ',");")'
            )
          }else if(quote_text){
            str <- stringi::stri_join(
              'stringi::stri_join("EXECUTE ", ', shQuote(prep_id), ', "(",', stringi::stri_join('quoteText2(x[[', i, ']])', collapse = ", ',', "), ', ");")'
            )
          }else{
            str <- stringi::stri_join(
              'stringi::stri_join("EXECUTE ", ', shQuote(prep_id), ', "(",', stringi::stri_join('x[[', i, ']]', collapse = ", ',', "), ', ");")'
            )
          }
          insert <- eval(str2expression(str))
          rm(x)
          prep <- DBI::dbSendQuery(
            conn = con,
            statement = sql_prep
          )
          DBI::dbClearResult(prep)
          DBI::dbGetQuery(
            conn = con,
            statement = stringi::stri_join(insert, collapse = "\n")
          )
          deallocate <- DBI::dbSendQuery(
            conn = con,
            statement = sql_deallocate
          )
          DBI::dbClearResult(deallocate)
          DBI::dbDisconnect(con)
        },
        mc.cores = n_cores)
        return()
      }
    }
  }else{
    if(prepare == FALSE){
      insert <- paste0("INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, " (", paste0(select_cols, collapse = ", "), ") \n")
      insert <- paste0(insert, select, ";")
      if(is.null(con) == FALSE){
        return(insert)
      }else{
        res <- as.data.table(DBI::dbGetQuery(
          con,
          insert
        ))
        return(res)
      }
    }else{
      prep_id <- paste0("insert_", sampleStr(16))
      if(is.null(con) == FALSE){
        existing_statements <- DBI::dbGetQuery(
          conn = con,
          "SELECT name FROM pg_prepared_statements"
        )$name
        while(prep_id %in% existing_statements){
          prep_id <- paste0("insert_", sampleStr(16))
        }
      }
      sql_prep <- paste0("PREPARE ", prep_id, " (", paste0(types, collapse = ", "), ") AS INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, "(", paste0(select_cols, collapse = ", "), ") ", select, if(is.null(returning) == FALSE){paste0(" RETURNING ", paste0(returning, collapse = ", "))}else{""}, ";")
      insert <- paste0(paste0("EXECUTE ", prep_id, "();"), "\nDEALLOCATE ", prep_id, ";")
      if(is.null(con) == FALSE){
        return(paste0(sql_prep, "\n", insert, ";"))
      }else{
        prep <- DBI::dbSendQuery(
          conn = con,
          statement = sql_prep
        )
        DBI::dbClearResult(prep)
        res <- as.data.table(DBI::dbGetQuery(
          con,
          insert
        ))
        deallocate <- DBI::dbSendQuery(
          conn = con,
          statement = sql_deallocate
        )
        DBI::dbClearResult(deallocate)
        return(res)
      }
    }
  }
}

#' Generate a PostgreSQL UPDATE statement, optionally execute the statement if con is not NULL.
#'
#' @param x A named list, names must match the column names of the destination SQL table, values are the values to set the SQL records to.
#' @param schema A string, the schema name of the destination SQL table.
#' @param table A string, the table name of the destination SQL table.
#' @param where A named list, names are the columns for comparison, values are lists with a comparison operator and a value the comparison operator will check against. ex: list(col1 = list(comparison = "=", value = quoteText("b")))
#' @param prepare TRUE/FALSE, if TRUE, creates a PostgreSQL prepared statement for inserting the data.
#' @param types A vector of character strings specifying the SQL data types of the destination columns, the position of the type should match the position of the column for that type in x. Required if prepare or cast is TRUE.
#' @param returning A vector of character strings specifying the SQL column names to be returned by the INSERT statement.
#' @param quote_text TRUE/FALSE, if TRUE, calls quoteText() to add single quotes around character strings.
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the data to be inserted to the specified type.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL UPDATE statement; or a string, PostgreSQL prepared statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' UPDATE(
#' x = list(col1 = "a", col2 = 1),
#' schema = "test",
#' table = "table1",
#' where = list(
#'   col1 = list(comparison = "=", value = quoteText("b")),
#'   col2 = list(comparison = "IS", value = "NULL")
#' ),
#' prepare = FALSE,
#' types = c("TEXT", "INTEGER"),
#' returning = c("col3"),
#' quote_text = TRUE,
#' cast = TRUE
#' )
UPDATE <- function(
  x,
  schema = NULL,
  table,
  where = list(),
  prepare = TRUE,
  types = NULL,
  returning = NULL,
  quote_text = TRUE,
  cast = TRUE,
  con = NULL
){

  if(is.list(x) == TRUE){

    cols <- names(x)

    if(prepare == TRUE){

      prep_id <- sampleStr(16)

      if(is.null(con) == TRUE){

        sql_prep <- paste0(
          "PREPARE update_", prep_id, " (", paste0(types, collapse = ", "), ") AS UPDATE ",
          if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table},
          " SET ", paste0(paste(cols, paste0("$", 1:length(cols)), sep = " = "), collapse = ", "),
          if(length(where) == 0){
            ""
          }else{
            paste0(" WHERE ", paste(names(where), lapply(where, paste0, collapse = " "), collapse = " AND "))
          },
          if(is.null(returning) == FALSE){paste0(" RETURNING ", paste0(returning, collapse = ", "))}else{""}, ";")

      }else{

        existing_statements <- DBI::dbGetQuery(
          conn = con,
          "SELECT name FROM pg_prepared_statements"
        )$name

        while(paste0("update_", prep_id) %in% existing_statements){
          prep_id <- sampleStr(16)
        }

        prep <- DBI::dbSendQuery(
          conn = con,
          statement = paste0(
            "PREPARE update_", prep_id, " (", paste0(types, collapse = ", "), ") AS UPDATE ",
            if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table},
            " SET ", paste0(paste(cols, paste0("$", 1:length(cols)), sep = " = "), collapse = ", "),
            if(length(where) == 0){
              ""
            }else{
              paste0(" WHERE ", paste(names(where), lapply(where, paste0, collapse = " "), collapse = " AND "))
            },
            if(is.null(returning) == FALSE){paste0(" RETURNING ", paste0(returning, collapse = ", "))}else{""}, ";")
        )

        DBI::dbClearResult(prep)

      }

      if(quote_text == TRUE){
        x <- lapply(x, quoteText)
      }

      if(cast == TRUE){
        x <- paste0("CAST(", unlist(x), " AS ", types, ")")
      }

      x <- paste0("EXECUTE update_", prep_id, "(", paste0(x, collapse = ", "), ");")

      if(is.null(con) == TRUE){
        sql <- paste0(sql_prep, "\n", paste0(x, collapse = "\n"), "\nDEALLOCATE update_", prep_id, ";")
      }else{
        sql <- paste0(paste0(x, collapse = "\n"), "\nDEALLOCATE update_", prep_id, ";")
      }

    }else{

      if(quote_text == TRUE){
        x <- lapply(x, quoteText)
      }

      if(cast == TRUE){
        x <- paste0("CAST(", unlist(x), " AS ", types, ")")
      }

      sql <- paste0(
        "UPDATE ", if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table},
        " SET ", paste0(paste(cols, x, sep = " = "), collapse = ", "),
        if(length(where) == 0){
          ""
        }else{
          paste0(" WHERE ", paste(names(where), lapply(where, paste0, collapse = " "), collapse = " AND "))
        },
        if(is.null(returning) == FALSE){paste0(" RETURNING ", paste0(returning, collapse = ", "))}else{""}, ";"
      )

    }

    if(is.null(con) == TRUE){
      return(sql)
    }else{
      return(DBI::dbGetQuery(con, sql))
    }

  }else{
    print("x must be a named list.")
    return(c())
  }

}

#' Generate a PostgreSQL DELETE statement, optionally execute the statement if con is not NULL.
#'
#' @param schema A string, the schema name of the SQL table to DELETE from.
#' @param table A string, the table name of the SQL table to DELETE from.
#' @param where A named list, names are the columns for comparison, values are lists with a comparison operator and a value the comparison operator will check against. ex: list(col1 = list(comparison = "=", value = quoteText("b")))
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL DELETE statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' DELETE(
#' schema = "test",
#' table = "table1",
#' where = list(
#'   col1 = list(comparison = "=", value = quoteText("b")),
#'   col2 = list(comparison = "IS", value = "NULL")
#' )
#' )
DELETE <- function(
  schema = NULL,
  table,
  where = NULL,
  con = NULL
){

  x <- paste0(
    "DELETE FROM ", if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table},
    if(is.null(where) == FALSE){
      paste0(
        " WHERE ",
        paste(names(where), lapply(where, paste0, collapse = " "), collapse = " AND ")
      )
    }else{
      ""
    },
    ";"
  )

  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL TRUNCATE statement, optionally execute the statement if con is not NULL.
#'
#' @param schema A string, the schema name of the SQL table to TRUNCATE.
#' @param table A string, the table name of the SQL table to TRUNCATE.
#' @param restart_identity TRUE/FALSE, if TRUE, will add RESTART IDENTITY to the statement.
#' @param continue_identity TRUE/FALSE, if TRUE, will add CONTINUE IDENTITY to the statement.
#' @param cascade TRUE/FALSE, if TRUE, will add CASCADE to the statement.
#' @param restrict TRUE/FALSE, if TRUE, will add RESTRICT to the statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL DELETE statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' TRUNCATE(
#' schema = "test",
#' table = "table1"
#' )
TRUNCATE <- function(
  schema = NULL,
  table,
  restart_identity = FALSE,
  continue_identity = FALSE,
  cascade = FALSE,
  restrict = FALSE,
  con = NULL
){

  x <- paste0(
    "TRUNCATE ", if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table},
    if(restart_identity == TRUE){" RESTART IDENTITY"}else{""},
    if(continue_identity == TRUE){" CONTINUE IDENTITY"}else{""},
    if(cascade == TRUE){" CASCADE"}else{""},
    if(restrict == TRUE){" RESTRICT"}else{""},
    ";"
  )

  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a PostgreSQL statement to execute a function, optionally execute the statement if con is not NULL.
#'
#' @param x A named list, names must match the parameter names of the SQL function, values are the values to set the parameters to when executing the SQL function.
#' @param schema A string, the schema name of the SQL function.
#' @param func A string, the name of the SQL function.
#' @param quote_text TRUE/FALSE, if TRUE, calls quoteText() to add single quotes around character strings.
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the parameters to the specified type.
#' @param types A vector of character strings specifying the SQL data types of the function parameters, the position of the type should match the position of the parameter for that type in x.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL statement to execute a function; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' callFUNCTION(
#' x = list(a = 1, b = 2),
#' schema = NULL,
#' func = "sample_add",
#' quote_text = TRUE,
#' cast = FALSE,
#' types = c("INT", "INT")
#' )
callFUNCTION <- function(
  x = list(),
  schema = NULL,
  func,
  quote_text = TRUE,
  cast = TRUE,
  types,
  con = NULL
){

  if(is.list(x) == TRUE){

    if(cast == FALSE){

      x <- paste0("SELECT * FROM ", if(is.null(schema) == FALSE){paste(schema, func, sep = ".")}else{func}, "(", if(length(x) > 0){paste0(paste0(names(x), " := ", if(quote_text == TRUE){lapply(x, quoteText)}else{x}), collapse = ", ")}else{""}, ");")

    }else{

      x <- paste0("SELECT * FROM ", if(is.null(schema) == FALSE){paste(schema, func, sep = ".")}else{func}, "(", if(length(x) > 0){paste0(paste0(names(x), " := ", paste0("CAST(", if(quote_text == TRUE){lapply(x, quoteText)}else{x}, " AS ", types, ")")), collapse = ", ")}else{""}, ");")

    }

    if(is.null(con) == TRUE){
      return(x)
    }else{
      x <- DBI::dbGetQuery(con, x)
      return(x)
    }


  }else{
    print("x must be a named list")
    return(c())
  }

}

#' Generate a PostgreSQL statement to execute a procedure, optionally execute the statement if con is not NULL.
#'
#' @param x A named list, names must match the parameter names of the SQL procedure, values are the values to set the parameters to when executing the SQL procedure.
#' @param schema A string, the schema name of the SQL procedure.
#' @param proc A string, the name of the SQL procedure.
#' @param quote_text TRUE/FALSE, if TRUE, calls quoteText() to add single quotes around character strings.
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the parameters to the specified type.
#' @param types A vector of character strings specifying the SQL data types of the procedure parameters, the position of the type should match the position of the parameter for that type in x.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL statement to execute a procedure; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' callPROCEDURE(
#' x = list(a = 1, b = 2),
#' schema = NULL,
#' proc = "sample_add",
#' quote_text = TRUE,
#' cast = FALSE,
#' types = c("INT", "INT")
#' )
callPROCEDURE <- function(
  x = list(),
  schema = NULL,
  proc,
  quote_text = TRUE,
  cast = TRUE,
  types,
  con = NULL
){

  if(is.list(x) == TRUE){

    if(cast == FALSE){

      x <- paste0("CALL ", if(is.null(schema) == FALSE){paste(schema, proc, sep = ".")}else{proc}, "(", if(length(x) > 0){paste0(paste0(names(x), " := ", if(quote_text == TRUE){lapply(x, quoteText)}else{x}), collapse = ", ")}else{""}, ");")

    }else{

      x <- paste0("CALL ", if(is.null(schema) == FALSE){paste(schema, proc, sep = ".")}else{proc}, "(", if(length(x) > 0){paste0(paste0(names(x), " := ", paste0("CAST(", if(quote_text == TRUE){lapply(x, quoteText)}else{x}, " AS ", types, ")")), collapse = ", ")}else{""}, ");")

    }

    if(is.null(con) == TRUE){
      return(x)
    }else{
      x <- DBI::dbSendQuery(con, x)
      DBI::dbClearResult(x)
      return(x)
    }

  }else{
    print("x must be a named list")
    return(c())
  }

}

#' Generate a PostgreSQL select statement, optionally execute the statement if con is not NULL.
#'
#' @param select A vector of columns/items to select.
#' @param from A string, the table(s) to select from.
#' @param where A string, text to include in the where clause.
#' @param group_by A vector of columns/items to group by.
#' @param having A vector of conditions to be met by aggregations.
#' @param order_by A vector of columns/items to order by.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL select statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' querySELECT(
#' select = c("col1", "col2", "col1 + col2 AS col3"),
#' from = "schema1.table1"
#' )
querySELECT <- function(
  select,
  from = list(),
  where = NULL,
  group_by = NULL,
  having = NULL,
  order_by = NULL,
  con = NULL
){

  x <- paste0(
    "SELECT ", "\n",
    if(length(names(select)) > 0){
      paste0(select, " ", names(select), collapse = ", \n")
    }else{
      paste0(select, collapse = ", \n")
    }, "\n",
    "FROM ", "\n",
    if(length(names(from)) > 0){
      paste0(from, " ", names(from), collapse = ", \n")
    }else{
      paste0(from, collapse = ", \n")
    }, " \n",
    if(is.null(where) == FALSE){
      paste0(
        "WHERE ", "\n",
        paste0(where, collapse = "\n"), "\n"
      )
    }else{
      ""
    },
    if(is.null(group_by) == FALSE){
      paste0(
        "GROUP BY ", "\n",
        paste0(group_by, collapse = ", \n"), "\n"
      )
    }else{
      ""
    },
    if(is.null(having) == FALSE){
      paste0(
        "HAVING ", "\n",
        paste0(having, collapse = ", \n"), "\n"
      )
    }else{
      ""
    },
    if(is.null(order_by) == FALSE){
      paste0(
        "ORDER BY ", "\n",
        paste0(order_by, collapse = ", \n"), "\n"
      )
    }else{
      ""
    }
  )
  return(
    if(is.null(con) == FALSE){
      DBI::dbGetQuery(con, x)
    }else{
      x
    }
  )
}

#' Generate a PostgreSQL COPY command, optionally execute the statement if con is not NULL.
#'
#' @param schema A string, the schema of the table to copy from/to.
#' @param table A string, the table to copy from/to.
#' @param columns A vector, columns to read/write.
#' @param file A string, the file path and name to read/write.
#' @param type A string, "FROM" or "TO".
#' @param delimiter A string, the delimiter.
#' @param format A string, "CSV", "TEXT", or "BINARY".
#' @param query A string, the query used to select data for output.
#' @param header TRUE/FALSE, if TRUE, adds HEADER to statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, PostgreSQL COPY command; or the results retrieved by DBI::dbExecute after executing the statement.
#' @examples
#' COPY(
#' table = "table1",
#' file = "/home/test/test.csv"
#' )
COPY <- function(
  schema = NULL,
  table,
  columns = NULL,
  file,
  type = "FROM",
  delimiter = ",",
  format = "csv",
  query = NULL,
  header = TRUE,
  con = NULL
){

  if(header == TRUE){
    format <- "csv"
  }

  if(tolower(format) == "csv" & delimiter == ","){
    delimiter <- shQuote(delimiter)
  }else{
    delimiter <- paste0("E", shQuote(delimiter))
  }

  if(type == "TO" & is.null(query) == FALSE){

    x <- paste0(
      "COPY ", "(", query, ")", "\n",
      type, " ", shQuote(file), "\n",
      if(tolower(format) == "csv"){
        ""
      }else{
        paste0("DELIMITER ", delimiter, "\n",)
      },
      if(is.null(format) == TRUE){""}else if(tolower(format) == "csv"){format}else{""}, if(header == TRUE & tolower(format) == "csv"){" HEADER"}else{""}, ";"
    )

  }else{

    x <- paste0(
      "COPY ", if(is.null(schema) == FALSE){paste0(schema, ".")}else{""}, table, if(is.null(columns) == FALSE){paste0("(", paste0(columns, collapse = ", "), ")")}else{""}, "\n",
      type, " ", shQuote(file), "\n",
      if(tolower(format) == "csv"){
        ""
      }else{
        paste0("DELIMITER ", delimiter, "\n",)
      },
      if(is.null(format) == TRUE){
        ""
      }else if(tolower(format) == "csv"){
        format
      }else{
        ""
      },
      if(header == TRUE & tolower(format) == "csv"){
        " HEADER"
      }else{
        ""
      }, ";"
    )

  }

  return(
    if(is.null(con) == FALSE){
      DBI::dbExecute(con, x)
    }else{
      x
    }
  )

}

################################################################################
# alter table helpers
################################################################################

#' Helper command to add a column via ALTER TABLE.
#'
#' @param column_name A string, the name of the column to add.
#' @param data_type A string, the data type of the column to add.
#' @param default A string, a default value for the column to add.
#' @param constraint A string, a constraint for the column to add.
#' @param if_not_exists Boolean, if TRUE, adds IF NOT EXISTS to the ADD COLUMN statement.
#' @return A string, PostgreSQL helper statement to add a column using ALTER TABLE.
#' @examples
#' pg_addColumn(
#' column_name = "newCol",
#' data_type = "text"
#' )
pg_addColumn <- function(
  column_name,
  data_type,
  default = NULL,
  constraint = NULL,
  if_not_exists = FALSE
){
  return(
    paste0("ADD COLUMN ", if(if_not_exists == TRUE){"IF NOT EXISTS "}else{""}, column_name, " ", data_type, if(is.null(default) == TRUE){""}else{paste0(" DEFAULT ", default)}, if(is.null(constraint) == TRUE){""}else{paste0(" ", constraint)})
  )
}

#' Helper command to drop a column via ALTER TABLE.
#'
#' @param column_name A string, the name of the column to drop.
#' @param cascade Boolean, if TRUE, adds CASCADE to the DROP COLUMN statement.
#' @param restrict Boolean, if TRUE, adds RESTRICT to the DROP COLUMN statement.
#' @param if_exists Boolean, if TRUE, adds IF EXISTS to the DROP COLUMN statement.
#' @return A string, PostgreSQL helper statement to drop a column using ALTER TABLE.
#' @examples
#' pg_dropColumn(
#' column_name = "newCol"
#' )
pg_dropColumn <- function(
  column_name,
  cascade = FALSE,
  restrict = FALSE,
  if_exists = FALSE
){
  return(
    paste0("DROP COLUMN ", if(if_exists == TRUE){"IF EXISTS "}else{""}, column_name, if(cascade == TRUE){" CASCADE"}else{""}, if(restrict == TRUE){" RESTRICT"}else{""})
  )
}

#' Helper command to alter a column's data type via ALTER TABLE.
#'
#' @param column_name A string, the name of the column to add.
#' @param data_type A string, the data type of the column to add.
#' @param using A string, a command to cast the column into the appropriate type.
#' @return A string, PostgreSQL helper statement to alter a column type using ALTER TABLE.
#' @examples
#' pg_alterColumnType(
#' column_name = "newCol",
#' data_type = "text"
#' )
pg_alterColumnType <- function(
  column_name,
  data_type,
  using = NULL
){
  return(
    if(is.null(using) == TRUE){
      paste0("ALTER COLUMN ", column_name, " SET DATA TYPE ", data_type)
    }else{
      paste0("ALTER COLUMN ", column_name, " SET DATA TYPE ", data_type, " USING ", using)
    }
  )
}

#' Helper command to rename a column via ALTER TABLE.
#'
#' @param column_name A string, the name of the column to change.
#' @param new_column_name A string, the new name for the column.
#' @return A string, PostgreSQL helper statement to rename a column using ALTER TABLE.
#' @examples
#' pg_renameColumn(
#' column_name = "newCol",
#' new_column_name = "col1"
#' )
pg_renameColumn <- function(
  column_name,
  new_column_name
){
  return(
    paste0("RENAME COLUMN ", column_name, " TO ", new_column_name)
  )
}

#' Helper command to rename a table via ALTER TABLE.
#'
#' @param new_table_name A string, the new name for the table.
#' @return A string, PostgreSQL helper statement to rename a table using ALTER TABLE.
#' @examples
#' pg_renameTable(
#' new_table_name = "table1"
#' )
pg_renameTable <- function(
  new_table_name
){
  return(
    paste0("RENAME TO ", new_table_name)
  )
}
