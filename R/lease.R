#' Lease R6 Class
#'
#' @description
#' An R6 class that manages a persistent, auto-disconnecting database connection
#' lease using exponential back-off for connection attempts.
#'
#' @export
Lease <- R6Class(
  classname = "Lease",
  public = list(
    #' @field drv A [DBI Driver][DBI::DBIDriver-class] used to connect to the database.
    drv = NULL,

    #' @field timeout Numeric. Seconds to hold the connection open for reuse.
    timeout = NULL,

    #' @field max_retries Numeric. Number of times to retry connecting using exponential back-off.
    max_retries = NULL,

    #' @field cache An environment holding the persistent connection state and timestamps.
    cache = NULL,

    #' @field dots A list of additional arguments to be passed to [DBI::dbConnect()].
    dots = NULL,

    #' @description
    #' Create a new Lease object.
    #' @param drv A [DBI Driver][DBI::DBIDriver-class], e.g., `duckdb::duckdb()`.
    #' @param ... Additional arguments passed to [DBI::dbConnect()].
    #' @param timeout Seconds to hold the connection open for reuse. Default is `60`.
    #' @param max_retries Number of times to retry connecting using exponential back-off. Default is `3`.
    #' @param cache An optional environment to use for caching. If `NULL`, a new environment is created.
    #' @return A new `Lease` object.
    initialize = function(drv = character(), ...,
                          timeout = 60, max_retries = 10, cache = NULL) {
      self$drv <- drv
      self$timeout <- timeout
      self$max_retries <- max_retries
      self$dots <- list(...)

      # Environments are reference objects. They must be initialized inside
      # the constructor so instances don't share the same cache environment.
      self$cache <- cache %||% new.env(parent = emptyenv())
    }
  )
)
