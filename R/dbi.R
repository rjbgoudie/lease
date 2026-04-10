#' Create a lease to a database connection
#'
#' @param drv A [DBI Driver][DBI::DBIDriver-class], e.g. `duckdb::duckdb()`
#' @param ... Additional arguments passed to [DBI::dbConnect()].
#' @param timeout Seconds to hold the connection open for reuse.
#' @param max_retries Number of times to retry connecting using exponential back-off.
#' @return An object of class lease
#' @export
dbLease <- function(drv, ..., timeout = 60, max_retries = 10) {
  # Create a dedicated environment to hold the persistent connection state
  cache_env <- new.env(parent = emptyenv())

  # If this lease object is deleted/garbage collected,
  # guarantee the connection is gracefully closed to release the file lock
  reg.finalizer(cache_env, function(e) {
    if (!is.null(e$con) && DBI::dbIsValid(e$con)) {
      DBI::dbDisconnect(e$con, shutdown = TRUE)
    }
  }, onexit = TRUE)

  Lease$new(
    drv = drv,
    ...,
    timeout = as.numeric(timeout),
    max_retries = as.numeric(max_retries),
    cache = cache_env
  )
}

#' @importFrom later later
with_leased_connection <- function(lease, FUN = NULL, ...) {
  env <- lease$cache

  # check if already have a valid connection
  if (!is.null(env$con) && DBI::dbIsValid(env$con)) {
    env$last_used <- Sys.time()

    if (is.null(FUN)) {
      return(env$con)
    }
    return(FUN(env$con, ...))
  }


  base_delay <- 0.1
  for (i in 1:lease$max_retries) {
    con <- tryCatch(
      {
        do.call(DBI::dbConnect, c(list(lease$drv), lease$dots))
      },
      error = function(e) e
    )

    # If successful, break the loop
    if (!inherits(con, "error")) {
      break
    }

    if (i == lease$max_retries) {
      stop(
        "Failed to acquire database lock after ", lease$max_retries,
        " retries. Another process may be holding it. Last error: ", con$message
      )
    }

    Sys.sleep(base_delay * (2^(i - 1)))
  }

  env$con <- con
  env$last_used <- Sys.time()

  schedule_disconnect <- function(delay) {
    later::later(function() {
      if (!is.null(env$con) && DBI::dbIsValid(env$con)) {
        age <- as.numeric(difftime(Sys.time(), env$last_used, units = "secs"))

        if (age >= lease$timeout) {
          DBI::dbDisconnect(env$con, shutdown = TRUE)
          env$con <- NULL
        } else {
          # connection used recently -> reschedule
          schedule_disconnect(lease$timeout - age)
        }
      }
    }, delay = delay)
  }

  schedule_disconnect(lease$timeout)

  if (is.null(FUN)) {
    return(env$con)
  }
  FUN(env$con, ...)
}
