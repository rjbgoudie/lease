
# lease

<!-- badges: start -->
<!-- badges: end -->

A `lease` object acts as a proxy for a `DBI` connection. It automatically opens the connection when needed, keeps it open for a specified timeout, and retries with exponential back-off if the database is locked.

## Installation

You can install the development version of lease from [GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("rjbgoudie/lease")
```

## Example


```r
library(lease)
library(DBI)

# Create a lease for an in-memory DuckDB database
db_lease <- dbLease(
  duckdb::duckdb(dbdir = "mydb.duckdb"),
  timeout = 60, # how many seconds to keep the connection open
  max_retries = 10 # how many (exponentially increasing) tries to make if db is locked
)
```

Use the `db_lease` object directly where you would normally use a `conn`.

```r
# Write data
dbWriteTable(db_lease, "mtcars", mtcars)

# Query data
res <- dbGetQuery(db_lease, "SELECT * FROM mtcars LIMIT 5")
head(res)

# List tables
dbListTables(db_lease)
```
