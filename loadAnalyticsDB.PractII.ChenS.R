# loadAnalyticsDB.PractII.ChenS.R
# CS5200 Practicum II – Load Data into Analytics Datamart
# Author: Shaobo Chen
# Date: Spring 2025

# ----------------------------
# 1) Clear Environment & Load Libraries
rm(list = ls())
if (!require("RSQLite")) install.packages("RSQLite")
library(RSQLite)
if (!require("RMySQL")) install.packages("RMySQL")
library(RMySQL)

# ----------------------------
# 2) Connect to Databases
## 2.1 Cloud MySQL Datamart
mydbcon <- dbConnect(
  RMySQL::MySQL(),
  dbname   = Sys.getenv("DB_NAME"),
  host     = Sys.getenv("DB_HOST"),
  port     = as.integer(Sys.getenv("DB_PORT")),
  user     = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD"),
  sslmode  = "REQUIRED"
)


## 2.2 Operational SQLite
film_con  <- dbConnect(SQLite(), dbname = "film-sales.db")
music_con <- dbConnect(SQLite(), dbname = "music-sales.db")

# ----------------------------
# 3) Extraction
#
# 3.1) Store-Based Sales (fact_sales)
# (a) Film
film_store_query <- "
SELECT
  DATE(p.payment_date)       AS trans_date,
  s_ctry.country             AS store_country,
  SUM(p.amount)              AS total_revenue,
  AVG(p.amount)              AS average_revenue,
  COUNT(*)                   AS total_units,
  AVG(p.amount)              AS average_units,
  COUNT(DISTINCT p.customer_id) AS num_customers,
  1                          AS min_units,
  1                          AS max_units,
  'film'                     AS channel
FROM payment p
  JOIN staff st         ON p.staff_id = st.staff_id
  JOIN store s          ON st.store_id = s.store_id
  JOIN address a_store  ON s.address_id = a_store.address_id
  JOIN city ci_store    ON a_store.city_id = ci_store.city_id
  JOIN country s_ctry   ON ci_store.country_id = s_ctry.country_id
GROUP BY DATE(p.payment_date), s_ctry.country;
"
film_sales <- dbGetQuery(film_con, film_store_query)

# (b) Music
music_store_query <- "
SELECT 
  DATE(i.InvoiceDate)           AS trans_date,
  i.BillingCountry              AS store_country,
  SUM(i.Total)                  AS total_revenue,
  AVG(i.Total)                  AS average_revenue,
  SUM(ii.Quantity)              AS total_units,
  AVG(ii.Quantity)              AS average_units,
  COUNT(DISTINCT i.CustomerId)  AS num_customers,
  MIN(ii.Quantity)              AS min_units,
  MAX(ii.Quantity)              AS max_units,
  'music'                       AS channel
FROM invoices i
JOIN (
  SELECT InvoiceId, SUM(Quantity) AS Quantity
  FROM invoice_items
  GROUP BY InvoiceId
) ii ON i.InvoiceId = ii.InvoiceId
GROUP BY DATE(i.InvoiceDate), i.BillingCountry;
"
music_sales <- dbGetQuery(music_con, music_store_query)

# Combine store-based film & music
sales_agg <- rbind(film_sales, music_sales)

# 3.2) Customer-Based (fact_customer)
# (a) Film customers
film_cust_query <- "
SELECT
  cust_ctry.country AS customer_country,
  COUNT(DISTINCT p.customer_id) AS num_customers,
  'film' AS channel
FROM payment p
  JOIN customer cust ON p.customer_id = cust.customer_id
  JOIN address a_cust ON cust.address_id = a_cust.address_id
  JOIN city ci_cust   ON a_cust.city_id = ci_cust.city_id
  JOIN country cust_ctry ON ci_cust.country_id = cust_ctry.country_id
GROUP BY cust_ctry.country;
"
film_cust <- dbGetQuery(film_con, film_cust_query)

# (b) Music customers
music_cust_query <- "
SELECT
  c.Country AS customer_country,
  COUNT(DISTINCT i.CustomerId) AS num_customers,
  'music' AS channel
FROM invoices i
  JOIN customers c ON i.CustomerId = c.CustomerId
GROUP BY c.Country;
"
music_cust <- dbGetQuery(music_con, music_cust_query)

cust_agg <- rbind(film_cust, music_cust)

# ----------------------------
# 4) Load Dimensions

#
# 4.1) dim_country
#
# Combine store_country from sales_agg and customer_country from cust_agg
store_countries    <- unique(sales_agg$store_country)
customer_countries <- unique(cust_agg$customer_country)
all_countries      <- unique(c(store_countries, customer_countries))
all_countries      <- all_countries[!is.na(all_countries)]
all_countries      <- all_countries[nchar(all_countries) > 0]

dbExecute(mydbcon, "SET FOREIGN_KEY_CHECKS=0;")
dbExecute(mydbcon, "TRUNCATE TABLE dim_country;")
dbExecute(mydbcon, "SET FOREIGN_KEY_CHECKS=1;")

for(ct in all_countries) {
  sql <- sprintf("INSERT INTO dim_country (country_name) VALUES ('%s')", ct)
  dbExecute(mydbcon, sql)
}

#
# 4.2) dim_date
#
dbExecute(mydbcon, "SET FOREIGN_KEY_CHECKS=0;")
dbExecute(mydbcon, "TRUNCATE TABLE dim_date;")
dbExecute(mydbcon, "SET FOREIGN_KEY_CHECKS=1;")

all_dates <- unique(sales_agg$trans_date)
for(d in all_dates) {
  dv <- as.Date(d)
  day_val <- as.integer(format(dv, "%d"))
  mon_val <- as.integer(format(dv, "%m"))
  y_val   <- as.integer(format(dv, "%Y"))
  q_val   <- ceiling(mon_val/3)
  m_name  <- format(dv, "%B")
  
  insert_date <- sprintf("
    INSERT INTO dim_date (date_val, day, month, quarter, year, month_name)
    VALUES ('%s', %d, %d, %d, %d, '%s')",
                         dv, day_val, mon_val, q_val, y_val, m_name
  )
  dbExecute(mydbcon, insert_date)
}

# (dim_channel presumably seeded by schema creation or re-insert if needed)

# ----------------------------
# 5) Load fact_sales
dbExecute(mydbcon, "SET FOREIGN_KEY_CHECKS=0;")
dbExecute(mydbcon, "TRUNCATE TABLE fact_sales;")
dbExecute(mydbcon, "SET FOREIGN_KEY_CHECKS=1;")

dim_date_df    <- dbGetQuery(mydbcon, "SELECT * FROM dim_date;")
dim_country_df <- dbGetQuery(mydbcon, "SELECT * FROM dim_country;")
dim_channel_df <- dbGetQuery(mydbcon, "SELECT * FROM dim_channel;")

fact_sales_df <- merge(sales_agg, dim_date_df,    by.x="trans_date",  by.y="date_val")
fact_sales_df$store_country_id <- dim_country_df$country_id[
  match(fact_sales_df$store_country, dim_country_df$country_name)
]
fact_sales_df$channel_id <- dim_channel_df$channel_id[
  match(fact_sales_df$channel, dim_channel_df$channel)
]

# Batch insert
batch_size  <- 1000
num_rows    <- nrow(fact_sales_df)
num_batches <- ceiling(num_rows / batch_size)

cat(sprintf("Inserting %d rows into fact_sales in %d batches...\n", num_rows, num_batches))
for(b in seq_len(num_batches)) {
  st <- (b - 1)*batch_size + 1
  en <- min(b*batch_size, num_rows)
  chunk <- fact_sales_df[st:en, ]
  
  vals <- apply(chunk, 1, function(r) {
    sprintf("(%d,%d,%.4f,%.2f,%d,%.2f,%d,%d,%d,%d)",
            as.integer(r["date_id"]),
            as.integer(r["store_country_id"]),
            as.numeric(r["total_revenue"]),
            as.numeric(r["average_revenue"]),
            as.integer(r["total_units"]),
            as.numeric(r["average_units"]),
            as.integer(r["num_customers"]),
            as.integer(r["min_units"]),
            as.integer(r["max_units"]),
            as.integer(r["channel_id"])
    )
  })
  
  insert_sql <- paste0(
    "INSERT INTO fact_sales (date_id, store_country_id, total_revenue, average_revenue, ",
    "total_units, average_units, num_customers, min_units, max_units, channel_id) VALUES\n",
    paste(vals, collapse = ",\n"), ";"
  )
  
  dbExecute(mydbcon, insert_sql)
  cat(sprintf("  Batch %d / %d inserted (%d rows).\n", b, num_batches, nrow(chunk)))
}

# ----------------------------
# 6) Load fact_customer
dbExecute(mydbcon, "SET FOREIGN_KEY_CHECKS=0;")
dbExecute(mydbcon, "TRUNCATE TABLE fact_customer;")
dbExecute(mydbcon, "SET FOREIGN_KEY_CHECKS=1;")

# Re-pull dims (in case changed)
dim_country_df <- dbGetQuery(mydbcon, "SELECT * FROM dim_country;")
dim_channel_df <- dbGetQuery(mydbcon, "SELECT * FROM dim_channel;")

cust_agg$customer_country_id <- dim_country_df$country_id[
  match(cust_agg$customer_country, dim_country_df$country_name)
]
cust_agg$channel_id <- dim_channel_df$channel_id[
  match(cust_agg$channel, dim_channel_df$channel)
]

cust_batch_size <- 1000
nr_cust         <- nrow(cust_agg)
num_batches_c   <- ceiling(nr_cust / cust_batch_size)

cat(sprintf("Inserting %d rows into fact_customer in %d batches...\n", nr_cust, num_batches_c))

for(b in seq_len(num_batches_c)) {
  st <- (b - 1)*cust_batch_size + 1
  en <- min(b*cust_batch_size, nr_cust)
  chunk <- cust_agg[st:en, ]
  
  # Example: if you want to skip rows that have NA in country ID:
  # chunk <- subset(chunk, !is.na(customer_country_id))
  
  vals <- apply(chunk, 1, function(r) {
    sprintf("(%d,%d,%d)",
            as.integer(r["customer_country_id"]),
            as.integer(r["channel_id"]),
            as.integer(r["num_customers"])
    )
  })
  
  insert_sql <- paste0(
    "INSERT INTO fact_customer (customer_country_id, channel_id, num_customers) VALUES\n",
    paste(vals, collapse = ",\n"), ";"
  )
  
  dbExecute(mydbcon, insert_sql)
  cat(sprintf("  Batch %d / %d inserted (%d rows).\n", b, num_batches_c, nrow(chunk)))
}

# ----------------------------
# Done: Cleanup
dbDisconnect(film_con)
dbDisconnect(music_con)
dbDisconnect(mydbcon)

cat("ETL process complete; connection closed.\n")

