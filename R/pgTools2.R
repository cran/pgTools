
#' Add single quotes to strings, useful for converting R strings into SQL formatted strings.
#'
#' @param x A string.
#' @param char_only TRUE/FALSE, if TRUE, adds quotes only if is.character(x) is TRUE.
#' @return A string, with single quotes added to match postgreSQL string formatting.
#' @examples
#' quoteText("Sample quotes.")
quoteText <- function(x, char_only = TRUE){

  if(char_only == TRUE){
    if(is.character(x) == TRUE){
      return(paste0("'", gsub("\'", "''", x), "'"))
    }else{
      return(x)
    }
  }else{
    return(paste0("'", gsub("\'", "''", x), "'"))
  }

}

#' Add double quotes to strings.
#'
#' @param x A string.
#' @param char_only TRUE/FALSE, if TRUE, adds quotes only if is.character(x) is TRUE.
#' @return A string, with double quotes added.
#' @examples
#' doubleQuoteText("Sample quotes.")
doubleQuoteText <- function(x, char_only = TRUE){

  if(char_only == TRUE){
    if(is.character(x) == TRUE){
      return(paste0('"', x, '"'))
    }else{
      return(x)
    }
  }else{
    return(paste0('"', x, '"'))
  }

}

#' Generates (pseudo)random strings of the specified char length.
#' Used in INSERT and UPDATE to avoid overwriting existing columns of data.tables.
#' Not as fast as stringi:stri_rand_strings.
#'
#' @param char A integer, the number of chars to include in the output string.
#' @return A string.
#' @examples
#' sampleStr(10)
sampleStr <- function(char){

  x <- c()

  for(i in 1:char){
    x <- c(x, sample(c(letters, LETTERS, 0:9), 1))
  }

  return(
    paste0(x, collapse = "")
  )
}

#' Add a single line SQL comment.
#'
#' @param x A string.
#' @return A string prefixed with "--".
#' @examples
#' sql_comment("Sample single line comment.")
sql_comment <- function(x){
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
#' @param path A string, the file path (inlcude the file name) to save the script
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, SQL commands combined into one document or statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' sql_script(
#' createSCHEMA("dev"),
#' sql_80_char_comment(),
#' createTABLE(name = "sample",
#' columns = list(col1 = "SERIAL NOT NULL", col2 = "INTEGER", col3 = "TEXT"),
#' constraints = list(sample_constraint = "UNIQUE(col3)")
#' ))
sql_script <- function(..., path = NULL, con = NULL){

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

#' Generate a postgreSQL CREATE DATABASE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL CREATE DATABASE.
#' @param owner A string, the "user_name" parameter for postgreSQL CREATE DATABASE.
#' @param template A string, the "template" parameter for postgreSQL CREATE DATABASE.
#' @param encoding A string, the "encoding" parameter for postgreSQL CREATE DATABASE.
#' @param locale A string, the "locale" parameter for postgreSQL CREATE DATABASE
#' @param lc_collate A string, the "lc_collate" parameter for postgreSQL CREATE DATABASE.
#' @param lc_ctype A string, the "lc_ctype" parameter for postgreSQL CREATE DATABASE.
#' @param tablespace A string, the "tablespace_name" parameter for postgreSQL CREATE DATABASE.
#' @param allow_connections A string, the "allowconn" parameter for postgreSQL CREATE DATABASE.
#' @param connection_limit A string, the "connlimit" parameter for postgreSQL CREATE DATABASE.
#' @param is_template A string, the "istemplate" parameter for postgreSQL CREATE DATABASE.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL CREATE DATABASE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL ALTER DATABASE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL ALTER DATABASE.
#' @param allow_connections A string, the "allowconn" parameter for postgreSQL ALTER DATABASE.
#' @param connection_limit A string, the "connlimit" parameter for postgreSQL ALTER DATABASE.
#' @param is_template A string, the "istemplate" parameter for postgreSQL ALTER DATABASE.
#' @param rename_to A string, the "new_name" parameter for postgreSQL ALTER DATABASE.
#' @param owner_to A string, the "new_owner" parameter for postgreSQL ALTER DATABASE.
#' @param set_tablespace A string, the "new_tablespace" parameter for postgreSQL ALTER DATABASE.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL ALTER DATABASE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL DROP DATABASE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL DROP DATABASE.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL DROP DATABASE statement.
#' @param force TRUE/FALSE, if TRUE, adds "FORCE" to postgreSQL DROP DATABASE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL DROP DATABASE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL CREATE SCHEMA statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "schema_name" parameter for postgreSQL CREATE SCHEMA.
#' @param authorization A string, the "role_specification" parameter for postgreSQL CREATE SCHEMA.
#' @param if_not_exists TRUE/FALSE, if TRUE, adds "IF NOT EXISTS" to postgreSQL CREATE SCHEMA statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL CREATE SCHEMA statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL ALTER SCHEMA statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL ALTER SCHEMA.
#' @param rename_to A string, the "new_name" parameter for postgreSQL ALTER SCHEMA.
#' @param owner_to A string, the "new_owner" parameter for postgreSQL ALTER SCHEMA.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL CREATE SCHEMA statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL DROP SCHEMA statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL DROP SCHEMA.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL DROP SCHEMA statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL DROP SCHEMA statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to postgreSQL DROP SCHEMA statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL DROP SCHEMA statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL CREATE EXTENSION statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "extension_name" parameter for postgreSQL CREATE EXTENSION.
#' @param if_not_exists TRUE/FALSE, if TRUE, adds "IF NOT EXISTS" to postgreSQL CREATE EXTENSION statement.
#' @param schema A string, the "schema_name" parameter for postgreSQL CREATE EXTENSION.
#' @param version A string, the "version" parameter for postgreSQL CREATE EXTENSION.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL CREATE EXTENSION statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL CREATE EXTENSION statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL DROP EXTENSION statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL DROP EXTENSION.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL DROP EXTENSION statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL DROP EXTENSION statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to postgreSQL DROP EXTENSION statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL DROP EXTENSION statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL CREATE TABLE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "table_name" parameter for postgreSQL CREATE TABLE.
#' @param columns A named list, names are the SQL column names, values are strings with the SQL column data types, constraints, etc.
#' @param constraints A named list, names are the SQL constraint names, values are strings with the SQL constraint.
#' @param temporary TRUE/FALSE, if TRUE, adds "TEMPORARY" to postgreSQL CREATE TABLE statement.
#' @param if_not_exists TRUE/FALSE, if TRUE, adds "IF NOT EXISTS" to postgreSQL CREATE TABLE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL CREATE TABLE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' createTABLE(
#' name = "sample",
#' columns = list(col1 = "SERIAL NOT NULL", col2 = "INTEGER", col3 = "TEXT"),
#' constraints = list(sample_constraint = "UNIQUE(col3)")
#' )
createTABLE <- function(
  name,
  columns,
  constraints = NULL,
  temporary = FALSE,
  if_not_exists = FALSE,
  con = NULL
){

  columns <- lapply(columns, paste, sep = " ", collapse = " ")
  columns <- paste(names(columns), columns, sep = " ")

  x <- paste0(
    "CREATE ", if(temporary == TRUE){"TEMPORARY "}else{""}, "TABLE ", if(if_not_exists == TRUE){"IF NOT EXISTS "}else{""}, name, "\n",
    "(\n",
    paste(columns, sep = " ", collapse = ",\n"),
    if(is.null(constraints) == FALSE){paste0(",\n", paste(paste("CONSTRAINT", names(constraints), constraints), sep = " ", collapse = ",\n"))}else{""},
    "\n);"
  )

  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }

}

#' Generate a postgreSQL ALTER TABLE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL ALTER TABLE statement.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL ALTER TABLE statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL ALTER TABLE statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to postgreSQL ALTER TABLE statement.
#' @param action A string, the "action" parameter for postgreSQL ALTER TABLE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL ALTER TABLE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' alterTABLE("sample", action = "ADD COLUMN IF NOT EXISTS col4 BOOLEAN")
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

#' Generate a postgreSQL DROP TABLE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL DROP TABLE.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL DROP TABLE statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL DROP TABLE statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to postgreSQL DROP TABLE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL DROP TABLE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL CREATE PROCEDURE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL CREATE PROCEDURE.
#' @param args A named list, names are the argument names, values are strings with the argument data types.
#' @param or_replace TRUE/FALSE, if TRUE, adds "OR REPLACE" to postgreSQL CREATE PROCEDURE statement.
#' @param language A string, the "language" parameter for postgreSQL CREATE PROCEDURE.
#' @param definition A string, the "definition" parameter for postgreSQL CREATE PROCEDURE.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL CREATE PROCEDURE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL DROP PROCEDURE statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL DROP PROCEDURE.
#' @param args A named list, names are the argument names, values are strings with the argument data types.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL DROP PROCEDURE statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL DROP PROCEDURE statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to postgreSQL DROP PROCEDURE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL DROP PROCEDURE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL CREATE FUNCTION statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL CREATE FUNCTION.
#' @param args A named list, names are the argument names, values are strings with the argument data types.
#' @param or_replace TRUE/FALSE, if TRUE, adds "OR REPLACE" to postgreSQL CREATE FUNCTION statement.
#' @param returns A string, the "returns" parameter for postgreSQL CREATE FUNCTION.
#' @param returns_table A named list, names are the return table column names, values are strings with the return table data types.
#' @param language A string, the "language" parameter for postgreSQL CREATE FUNCTION.
#' @param definition A string, the "definition" parameter for postgreSQL CREATE FUNCTION.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL CREATE PROCEDURE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL DROP FUNCTION statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL DROP FUNCTION.
#' @param args A named list, names are the argument names, values are strings with the argument data types.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL DROP FUNCTION statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL DROP FUNCTION statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to postgreSQL DROP FUNCTION statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL DROP FUNCTION statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL CREATE TRIGGER statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL CREATE TRIGGER.
#' @param when A string, the "when" parameter (BEFORE, AFTER, INSTEAD OF) for postgreSQL CREATE TRIGGER.
#' @param event A string, the "event" parameter (INSERT/UPDATE/DELETE/TRUNCATE) for postgreSQL CREATE TRIGGER.
#' @param on A string, the "table_name" parameter for postgreSQL CREATE TRIGGER.
#' @param for_each_row TRUE/FALSE, if TRUE, adds "FOR EACH ROW" to postgreSQL CREATE TRIGGER statement.
#' @param func A string, the function call to be executed by the postgreSQL CREATE TRIGGER.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL CREATE TRIGGER statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL DROP TRIGGER statement, optionally execute the statement if con is not NULL.
#'
#' @param name A string, the "name" parameter for postgreSQL DROP TRIGGER.
#' @param on A string, the "table_name" parameter for postgreSQL DROP TRIGGER.
#' @param if_exists TRUE/FALSE, if TRUE, adds "IF EXISTS" to postgreSQL DROP TRIGGER statement.
#' @param cascade TRUE/FALSE, if TRUE, adds "CASCADE" to postgreSQL DROP TRIGGER statement.
#' @param restrict TRUE/FALSE, if TRUE, adds "RESTRICT" to postgreSQL DROP TRIGGER statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL DROP TRIGGER statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
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

#' Generate a postgreSQL INSERT statement, optionally execute the statement if con is not NULL.
#'
#' @param x A data.table, column names must match the column names of the destination SQL table.
#' @param schema A string, the schema name of the destination SQL table.
#' @param table A string, the table name of the destination SQL table.
#' @param prepare TRUE/FALSE, if TRUE, creates a postgreSQL prepared statement for inserting the data.
#' @param types A vector of character strings specifying the SQL data types of the destination columns, the position of the type should match the position of the column for that type in x. Required if prepare or cast is TRUE.
#' @param returning A vector of character strings specifying the SQL column names to be returned by the INSERT statement.
#' @param quote_text TRUE/FALSE, if TRUE, calls quoteText() to add single quotes around character strings.
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the data to be inserted to the specified type.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL INSERT statement; or a string, postgreSQL prepared statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' INSERT(
#' x = as.data.table(list(col1 = c("a", "b", "c"), col2 = c(1, 2, 3))),
#' schema = "test",
#' table = "table1",
#' prepare = TRUE,
#' types = c("TEXT", "INTEGER"),
#' returning = NULL,
#' quote_text = TRUE,
#' cast = TRUE
#' )
INSERT <- function(x, schema = NULL, table, prepare = TRUE, types, returning = NULL, quote_text = TRUE, cast = TRUE, con = NULL){

  if(data.table::is.data.table(x) == TRUE){

    cols <- colnames(x)

    if(prepare == TRUE){

      prep_id <- sampleStr(16)

      if(is.null(con) == TRUE){

        sql_prep <- paste0("PREPARE insert_", prep_id, " (", paste0(types, collapse = ", "), ") AS INSERT INTO ", if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table}, "(", paste0(cols, collapse = ", "), ") VALUES (", paste0("$", 1:length(cols), collapse = ", "), ")", if(is.null(returning) == FALSE){paste0(" RETURNING ", paste0(returning, collapse = ", "))}else{""}, ";")

      }else{

        existing_statements <- DBI::dbGetQuery(
          conn = con,
          "SELECT name FROM pg_prepared_statements"
        )$name

        while(paste0("insert_", prep_id) %in% existing_statements){
          prep_id <- sampleStr(16)
        }

        prep <- DBI::dbSendQuery(
          conn = con,
          statement = paste0("PREPARE insert_", prep_id, " (", paste0(types, collapse = ", "), ") AS INSERT INTO ", if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table}, "(", paste0(cols, collapse = ", "), ") VALUES (", paste0("$", 1:length(cols), collapse = ", "), ")", if(is.null(returning) == FALSE){paste0(" RETURNING ", paste0(returning, collapse = ", "))}else{""}, ";")
        )

        DBI::dbClearResult(prep)

      }

      if(quote_text == TRUE){
        x <- x[, lapply(.SD, quoteText)]
      }

      cols <- colnames(x)

      rownum <- sampleStr(10)

      while(rownum %in% cols){
        rownum <- sampleStr(10)
      }

      x <- x[, paste0(rownum) := seq_len(.N)]

      cols <- colnames(x)

      sql_col <- sampleStr(10)

      while(sql_col %in% cols){
        sql_col <- sampleStr(10)
      }

      x <- x[, paste0(sql_col) := list(list(unlist(c(.SD)))), by = cols, .SDcols = cols[!(cols %in% rownum)]]

      if(cast == TRUE){
        x <- x[, paste0(sql_col) := list(list(unlist(c(paste0("CAST(", unlist(base::get(sql_col)), " AS ", types, ")"))))), by = c(cols[!(cols %in% sql_col)])]
      }

      x[, paste0(sql_col) := paste0("EXECUTE insert_", prep_id, "(", paste0(unlist(base::get(sql_col)), collapse = ", "), ");"), by = c(cols[!(cols %in% sql_col)])]

      sql <- x[, base::get(sql_col)]

      if(is.null(con) == TRUE){
        sql <- paste0(sql_prep, "\n", paste0(sql, collapse = "\n"), "\nDEALLOCATE insert_", prep_id, ";")
      }else{
        sql <- paste0(paste0(sql, collapse = "\n"), "\nDEALLOCATE insert_", prep_id, ";")
      }

      x[, paste0(sql_col) := NULL]
      x[, paste0(rownum) := NULL]

    }else{

      if(quote_text == TRUE){
        x <- x[, lapply(.SD, quoteText)]
      }

      cols <- colnames(x)

      rownum <- sampleStr(10)

      while(rownum %in% cols){
        rownum <- sampleStr(10)
      }

      x[, paste0(rownum) := seq_len(.N)]

      cols <- colnames(x)

      sql_col <- sampleStr(10)

      while(sql_col %in% cols){
        sql_col <- sampleStr(10)
      }

      x <- x[, paste0(sql_col) := list(list(unlist(c(.SD)))), cols, .SDcols = cols[!(cols %in% rownum)]]

      if(cast == TRUE){
        x <- x[, paste0(sql_col) := list(list(unlist(c(paste0("CAST(", unlist(base::get(sql_col)), " AS ", types, ")"))))), by = c(cols[!(cols %in% sql_col)])]
      }

      x[, paste0(sql_col) := list(paste0("(", paste0(unlist(base::get(sql_col)), collapse = ", "), ")")), by = c(cols[!(cols %in% sql_col)])]

      sql <- x[, base::get(sql_col)]

      sql <- paste0(sql, collapse = ",\n")

      x[, paste0(sql_col) := NULL]
      x[, paste0(rownum) := NULL]

      cols <- colnames(x)

      sql <- paste0("INSERT INTO ", if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table}, "(", paste0(cols, collapse = ", "), ") VALUES \n", sql, if(is.null(returning) == FALSE){paste0(" RETURNING ", paste0(returning, collapse = ", "))}else{""}, ";")

    }

    if(is.null(con) == TRUE){
      return(sql)
    }else{
      return(DBI::dbGetQuery(con, sql))
    }

  }else{
    print("x must be a data.table")
    return(c())
  }

}

#' Generate a postgreSQL UPDATE statement, optionally execute the statement if con is not NULL.
#'
#' @param x A named list, names must match the column names of the destination SQL table, values are the values to set the SQL records to.
#' @param schema A string, the schema name of the destination SQL table.
#' @param table A string, the table name of the destination SQL table.
#' @param where A named list, names are the columns for comparison, values are lists with a comparison operator and a value the comparison operator will check against. ex: list(col1 = list(comparison = "=", value = quoteText("b")))
#' @param prepare TRUE/FALSE, if TRUE, creates a postgreSQL prepared statement for inserting the data.
#' @param types A vector of character strings specifying the SQL data types of the destination columns, the position of the type should match the position of the column for that type in x. Required if prepare or cast is TRUE.
#' @param returning A vector of character strings specifying the SQL column names to be returned by the INSERT statement.
#' @param quote_text TRUE/FALSE, if TRUE, calls quoteText() to add single quotes around character strings.
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the data to be inserted to the specified type.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL UPDATE statement; or a string, postgreSQL prepared statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
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
UPDATE <- function(x, schema = NULL, table, where, prepare = TRUE, types, returning = NULL, quote_text = TRUE, cast = TRUE, con = NULL){

  if(is.list(x) == TRUE){

    cols <- names(x)

    if(prepare == TRUE){

      prep_id <- sampleStr(16)

      if(is.null(con) == TRUE){

        sql_prep <- paste0(
          "PREPARE update_", prep_id, " (", paste0(types, collapse = ", "), ") AS UPDATE ",
          if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table},
          " SET ", paste0(paste(cols, paste0("$", 1:length(cols)), sep = " = "), collapse = ", "),
          " WHERE ", paste(names(where), lapply(where, paste0, collapse = " "), collapse = " AND "),
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
            " WHERE ", paste(names(where), lapply(where, paste0, collapse = " "), collapse = " AND "),
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
        " WHERE ", paste(names(where), lapply(where, paste0, collapse = " "), collapse = " AND "),
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

#' Generate a postgreSQL DELETE statement, optionally execute the statement if con is not NULL.
#'
#' @param schema A string, the schema name of the SQL table to DELETE from.
#' @param table A string, the table name of the SQL table to DELETE from.
#' @param where A named list, names are the columns for comparison, values are lists with a comparison operator and a value the comparison operator will check against. ex: list(col1 = list(comparison = "=", value = quoteText("b")))
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL DELETE statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' DELETE(
#' schema = "test",
#' table = "table1",
#' where = list(
#'   col1 = list(comparison = "=", value = quoteText("b")),
#'   col2 = list(comparison = "IS", value = "NULL")
#' )
#' )
DELETE <- function(schema = NULL, table, where = NULL, con = NULL){

  sql <- paste0(
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
    return(sql)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a postgreSQL TRUNCATE statement, optionally execute the statement if con is not NULL.
#'
#' @param schema A string, the schema name of the SQL table to TRUNCATE.
#' @param table A string, the table name of the SQL table to TRUNCATE.
#' @param restart_identity TRUE/FALSE, if TRUE, will add RESTART IDENTITY to the statement.
#' @param continue_identity TRUE/FALSE, if TRUE, will add CONTINUE IDENTITY to the statement.
#' @param cascade TRUE/FALSE, if TRUE, will add CASCADE to the statement.
#' @param restrict TRUE/FALSE, if TRUE, will add RESTRICT to the statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL DELETE statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
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

  sql <- paste0(
    "TRUNCATE ", if(is.null(schema) == FALSE){paste(schema, table, sep = ".")}else{table},
    if(restart_identity == TRUE){" RESTART IDENTITY"}else{""},
    if(continue_identity == TRUE){" CONTINUE IDENTITY"}else{""},
    if(cascade == TRUE){" CASCADE"}else{""},
    if(restrict == TRUE){" RESTRICT"}else{""},
    ";"
  )

  if(is.null(con) == TRUE){
    return(sql)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a postgreSQL statement to execute a function, optionally execute the statement if con is not NULL.
#'
#' @param x A named list, names must match the parameter names of the SQL function, values are the values to set the parameters to when executing the SQL function.
#' @param schema A string, the schema name of the SQL function.
#' @param func A string, the name of the SQL function.
#' @param quote_text TRUE/FALSE, if TRUE, calls quoteText() to add single quotes around character strings.
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the parameters to the specified type.
#' @param types A vector of character strings specifying the SQL data types of the function parameters, the position of the type should match the position of the parameter for that type in x.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL statement to execute a function; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' callFUNCTION(
#' x = list(a = 1, b = 2),
#' schema = NULL,
#' func = "sample_add",
#' quote_text = TRUE,
#' cast = FALSE,
#' types = c("INT", "INT")
#' )
callFUNCTION <- function(x = list(), schema = NULL, func, quote_text = TRUE, cast = TRUE, types, con = NULL){

  if(is.list(x) == TRUE){

    if(cast == FALSE){

      sql <- paste0("SELECT * FROM ", if(is.null(schema) == FALSE){paste(schema, func, sep = ".")}else{func}, "(", if(length(x) > 0){paste0(paste0(names(x), " := ", if(quote_text == TRUE){lapply(x, quoteText)}else{x}), collapse = ", ")}else{""}, ");")

    }else{

      sql <- paste0("SELECT * FROM ", if(is.null(schema) == FALSE){paste(schema, func, sep = ".")}else{func}, "(", if(length(x) > 0){paste0(paste0(names(x), " := ", paste0("CAST(", if(quote_text == TRUE){lapply(x, quoteText)}else{x}, " AS ", types, ")")), collapse = ", ")}else{""}, ");")

    }

    if(is.null(con) == TRUE){
      return(sql)
    }else{
      x <- DBI::dbGetQuery(con, x)
      return(x)
    }


  }else{
    print("x must be a named list")
    return(c())
  }

}

#' Generate a postgreSQL statement to execute a procedure, optionally execute the statement if con is not NULL.
#'
#' @param x A named list, names must match the parameter names of the SQL procedure, values are the values to set the parameters to when executing the SQL procedure.
#' @param schema A string, the schema name of the SQL procedure.
#' @param proc A string, the name of the SQL procedure.
#' @param quote_text TRUE/FALSE, if TRUE, calls quoteText() to add single quotes around character strings.
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the parameters to the specified type.
#' @param types A vector of character strings specifying the SQL data types of the procedure parameters, the position of the type should match the position of the parameter for that type in x.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, postgreSQL statement to execute a procedure; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' callPROCEDURE(
#' x = list(a = 1, b = 2),
#' schema = NULL,
#' proc = "sample_add",
#' quote_text = TRUE,
#' cast = FALSE,
#' types = c("INT", "INT")
#' )
callPROCEDURE <- function(x = list(), schema = NULL, proc, quote_text = TRUE, cast = TRUE, types, con = NULL){

  if(is.list(x) == TRUE){

    if(cast == FALSE){

      sql <- paste0("CALL ", if(is.null(schema) == FALSE){paste(schema, proc, sep = ".")}else{proc}, "(", if(length(x) > 0){paste0(paste0(names(x), " := ", if(quote_text == TRUE){lapply(x, quoteText)}else{x}), collapse = ", ")}else{""}, ");")

    }else{

      sql <- paste0("CALL ", if(is.null(schema) == FALSE){paste(schema, proc, sep = ".")}else{proc}, "(", if(length(x) > 0){paste0(paste0(names(x), " := ", paste0("CAST(", if(quote_text == TRUE){lapply(x, quoteText)}else{x}, " AS ", types, ")")), collapse = ", ")}else{""}, ");")

    }

    if(is.null(con) == TRUE){
      return(sql)
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
