#' DBI methods (simple wrappers)
#'
#' @name DBI-wrap
#' @param dbObj A DBI Driver][DBI::DBIDriver-class] or
#'   [DBI Connection][DBI::DBIConnection-class].
#' @param conn A [DBI Connection][DBI::DBIConnection-class].
NULL

setOldClass(c("Lease", "R6"))

# from https://github.com/rstudio/pool/blob/main/R/DBI-wrap.R
DBI_wrap <- function(fun_name) {
  fun <- utils::getFromNamespace(fun_name, "DBI")

  args <- formals(fun)
  con_arg <- sym(names(args)[[1]])

  call_args <- syms(set_names(names(args)))
  call_args[[1]] <- quote(db_con)
  ns_fun <- call2("::", quote(DBI), sym(fun_name))
  recall <- call2(ns_fun, !!!call_args)

  body <- expr({
    db_con <- with_leased_connection(!!con_arg)

    !!recall
  })

  new_function(args, body, env = ns_env("lease"))
}

#' @export
#' @inheritParams DBI::dbDataType
#' @rdname DBI-wrap
setMethod("dbDataType", "Lease", DBI_wrap("dbDataType"))

#' @export
#' @inheritParams DBI::dbGetQuery
#' @rdname DBI-wrap
setMethod("dbGetQuery", "Lease", DBI_wrap("dbGetQuery"))

#' @export
#' @inheritParams DBI::dbExecute
#' @rdname DBI-wrap
setMethod("dbExecute", "Lease", DBI_wrap("dbExecute"))

#' @export
#' @inheritParams DBI::dbListFields
#' @rdname DBI-wrap
setMethod("dbListFields", "Lease", DBI_wrap("dbListFields"))

#' @export
#' @inheritParams DBI::dbListTables
#' @rdname DBI-wrap
setMethod("dbListTables", "Lease", DBI_wrap("dbListTables"))

#' @export
#' @inheritParams DBI::dbListObjects
#' @rdname DBI-wrap
setMethod("dbListObjects", "Lease", DBI_wrap("dbListObjects"))

#' @export
#' @inheritParams DBI::dbReadTable
#' @rdname DBI-wrap
setMethod("dbReadTable", "Lease", DBI_wrap("dbReadTable"))

#' @export
#' @inheritParams DBI::dbWriteTable
#' @rdname DBI-wrap
setMethod("dbWriteTable", "Lease", DBI_wrap("dbWriteTable"))

#' @export
#' @inheritParams DBI::dbCreateTable
#' @rdname DBI-wrap
setMethod("dbCreateTable", "Lease", DBI_wrap("dbCreateTable"))

#' @export
#' @inheritParams DBI::dbAppendTable
#' @rdname DBI-wrap
setMethod("dbAppendTable", "Lease", DBI_wrap("dbAppendTable"))

#' @export
#' @inheritParams DBI::dbExistsTable
#' @rdname DBI-wrap
setMethod("dbExistsTable", "Lease", DBI_wrap("dbExistsTable"))

#' @export
#' @inheritParams DBI::dbRemoveTable
#' @rdname DBI-wrap
setMethod("dbRemoveTable", "Lease", DBI_wrap("dbRemoveTable"))

#' @export
#' @inheritParams DBI::dbIsReadOnly
#' @rdname DBI-wrap
setMethod("dbIsReadOnly", "Lease", DBI_wrap("dbIsReadOnly"))

#' @export
#' @inheritParams DBI::sqlData
#' @rdname DBI-wrap
setMethod("sqlData", "Lease", DBI_wrap("sqlData"))

#' @export
#' @inheritParams DBI::sqlCreateTable
#' @rdname DBI-wrap
setMethod("sqlCreateTable", "Lease", DBI_wrap("sqlCreateTable"))

#' @export
#' @inheritParams DBI::sqlAppendTable
#' @rdname DBI-wrap
setMethod("sqlAppendTable", "Lease", DBI_wrap("sqlAppendTable"))

#' @export
#' @inheritParams DBI::sqlInterpolate
#' @param .dots A list of named arguments to interpolate.
#' @rdname DBI-wrap
setMethod("sqlInterpolate", "Lease", DBI_wrap("sqlInterpolate"))

#' @export
#' @inheritParams DBI::sqlParseVariables
#' @rdname DBI-wrap
setMethod("sqlParseVariables", "Lease", DBI_wrap("sqlParseVariables"))

#' @export
#' @inheritParams DBI::dbQuoteIdentifier
#' @rdname DBI-wrap
setMethod("dbQuoteIdentifier", "Lease", DBI_wrap("dbQuoteIdentifier"))

#' @export
#' @inheritParams DBI::dbUnquoteIdentifier
#' @rdname DBI-wrap
setMethod("dbUnquoteIdentifier", "Lease", DBI_wrap("dbUnquoteIdentifier"))

#' @export
#' @inheritParams DBI::dbQuoteLiteral
#' @rdname DBI-wrap
setMethod("dbQuoteLiteral", "Lease", DBI_wrap("dbQuoteLiteral"))

#' @export
#' @inheritParams DBI::dbQuoteString
#' @rdname DBI-wrap
setMethod("dbQuoteString", "Lease", DBI_wrap("dbQuoteString"))

#' @export
#' @inheritParams DBI::dbAppendTableArrow
#' @rdname DBI-wrap
setMethod("dbAppendTableArrow", "Lease", DBI_wrap("dbAppendTableArrow"))

#' @export
#' @inheritParams DBI::dbCreateTableArrow
#' @rdname DBI-wrap
setMethod("dbCreateTableArrow", "Lease", DBI_wrap("dbCreateTableArrow"))

#' @export
#' @inheritParams DBI::dbGetQueryArrow
#' @rdname DBI-wrap
setMethod("dbGetQueryArrow", "Lease", DBI_wrap("dbGetQueryArrow"))

#' @export
#' @inheritParams DBI::dbReadTableArrow
#' @rdname DBI-wrap
setMethod("dbReadTableArrow", "Lease", DBI_wrap("dbReadTableArrow"))

#' @export
#' @inheritParams DBI::dbSendQueryArrow
#' @rdname DBI-wrap
setMethod("dbSendQueryArrow", "Lease", DBI_wrap("dbSendQueryArrow"))

#' @export
#' @inheritParams DBI::dbWriteTableArrow
#' @rdname DBI-wrap
setMethod("dbWriteTableArrow", "Lease", DBI_wrap("dbWriteTableArrow"))
