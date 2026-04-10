tbl.Lease <- function(src, from, ..., vars = NULL) {
  dbplyr::tbl_sql(c("lease", "dbi"), dbplyr::src_dbi(src), from, ..., vars = vars)
}

dbplyr_register_methods <- function() {
  s3_register("dplyr::tbl", "Lease")

  # Still need to implement
  # s3_register("dplyr::copy_to", "Lease")

  s3_register("dbplyr::dbplyr_edition", "Lease", function(con) 2L)

  # Wrappers inspect formals so can only be executed if dbplyr is available
  on_package_load("dbplyr", {
    check_dbplyr()

    dbplyr_s3_register <- function(fun_name) {
      s3_register(paste0("dbplyr::", fun_name), "Lease", dbplyr_wrap(fun_name))
    }
    dbplyr_s3_register("db_collect")
    dbplyr_s3_register("db_compute")
    dbplyr_s3_register("db_connection_describe")
    dbplyr_s3_register("db_copy_to")
    dbplyr_s3_register("db_col_types")
    dbplyr_s3_register("db_sql_render")
    dbplyr_s3_register("sql_translation")
    dbplyr_s3_register("sql_join_suffix")
    dbplyr_s3_register("sql_query_explain")
    dbplyr_s3_register("sql_query_fields")
  })
}

check_dbplyr <- function() {
  if (packageVersion("dbplyr") < "2.4.0") {
    inform(
      c(
        "!" = "Lease works best with dbplyr 2.4.0 or greater.",
        i = paste0("You have dbplyr ", packageVersion("dbplyr"), "."),
        i = "Please consider upgrading."
      ),
      class = c("packageStartupMessage", "simpleMessage")
    )
  }
}

dbplyr_wrap <- function(fun_name) {
  fun <- utils::getFromNamespace(fun_name, "dbplyr")
  args <- formals(fun)

  if ("temporary" %in% names(args)) {
    temporary <- list(quote(stop_if_temporary(temporary)))
  } else {
    temporary <- list()
  }

  con_arg <- sym(names(args)[[1]])

  call_args <- syms(set_names(names(args)))
  call_args[[1]] <- quote(db_con)
  ns_fun <- call2("::", quote(dbplyr), sym(fun_name))
  recall <- call2(ns_fun, !!!call_args)

  body <- expr({
    !!!temporary

    db_con <- with_leased_connection(!!con_arg)

    !!recall
  })

  new_function(args, body, env = ns_env("lease"))
}

stop_if_temporary <- function(temporary) {
  if (!temporary) {
    return()
  }

  abort(
    c(
      "Can't use temporary tables with Lease objects",
      x = "Temporary tables are local to a connection"
    ),
    call = NULL
  )
}
