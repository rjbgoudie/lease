library(testthat)
library(DBI)
library(later)

skip_if_not_installed("duckdb")

test_that("with_leased_connection establishes and reuses the connection", {
  lease <- dbLease(duckdb::duckdb(), dbdir = ":memory:", timeout = 5)

  # First call establishes connection
  con1 <- with_leased_connection(lease)
  expect_true(dbIsValid(con1))

  # Second call should return the EXACT same connection object/pointer
  con2 <- with_leased_connection(lease)
  expect_identical(con1, con2)

  dbDisconnect(con1, shutdown = TRUE)
})

test_that("background disconnect works after timeout expires", {
  # Set a very short timeout for the test
  lease <- dbLease(duckdb::duckdb(), dbdir = ":memory:", timeout = 1)

  con <- with_leased_connection(lease)
  expect_true(dbIsValid(con))

  # Sleep just past the timeout threshold
  Sys.sleep(1.2)

  # Since later tasks run in the R event loop when idle,
  # manually force execution of the queue during a busy test script.
  later::run_now()

  # The background task should have triggered and closed the connection
  expect_false(dbIsValid(con))
  expect_null(lease$cache$con)
})

test_that("background disconnect reschedules if connection is used", {
  lease <- dbLease(duckdb::duckdb(), dbdir = ":memory:", timeout = 1.5)

  con <- with_leased_connection(lease)

  Sys.sleep(1.0)

  # Touch the connection again, which updates cache$last_used
  con_reused <- with_leased_connection(lease)

  Sys.sleep(0.6)
  later::run_now()

  expect_true(dbIsValid(con_reused))
  expect_identical(con, con_reused)

  dbDisconnect(con, shutdown = TRUE)
})

test_that("DBI wrapper methods work seamlessly through the lease", {
  lease <- dbLease(duckdb::duckdb(), dbdir = ":memory:", timeout = 5)

  expect_no_error({
    dbWriteTable(lease, "iris_test", iris)
  })

  res <- dbReadTable(lease, "iris_test")
  expect_equal(nrow(res), nrow(iris))
  expect_equal(colnames(res), colnames(iris))

  expect_true(dbIsValid(lease$cache$con))

  dbDisconnect(lease$cache$con, shutdown = TRUE)
})
