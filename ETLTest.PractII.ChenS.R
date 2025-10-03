# ETLTest.PractII.ChenS.R
# CS5200 Practicum II – Test the result of the ETL process
# Author: Shaobo Chen
# Date: Spring 2025


# ----------------------------
# Load libraries
if (!require("RMySQL")) install.packages("RMySQL")
library(RMySQL)
if (!require("RSQLite")) install.packages("RSQLite")
library(RSQLite)

# ----------------------------
# 1) Establish connections for testing
mydbcon <- dbConnect(
  RMySQL::MySQL(),
  dbname   = Sys.getenv("DB_NAME"),
  host     = Sys.getenv("DB_HOST"),
  port     = as.integer(Sys.getenv("DB_PORT")),
  user     = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD"),
  sslmode  = "REQUIRED"
)

film_con_test  <- dbConnect(SQLite(), dbname = "film-sales.db")
music_con_test <- dbConnect(SQLite(), dbname = "music-sales.db")

# ----------------------------
# 2) Test 1: US Customer Count
# --------------------------------
# 2.1) Operational US customer count
# (a) Film
op_film_query <- "
  SELECT COUNT(DISTINCT cust.customer_id) AS op_film_customers
  FROM customer cust
  JOIN address a ON cust.address_id = a.address_id
  JOIN city ci ON a.city_id = ci.city_id
  JOIN country c ON ci.country_id = c.country_id
  WHERE c.country = 'USA';
"
op_film_res <- dbGetQuery(film_con_test, op_film_query)

# (b) Music
op_music_query <- "
  SELECT COUNT(DISTINCT CustomerId) AS op_music_customers
  FROM customers
  WHERE Country = 'USA';
"
op_music_res <- dbGetQuery(music_con_test, op_music_query)

# Combined operational US customers
op_us_customers <- op_film_res$op_film_customers + op_music_res$op_music_customers

# 2.2) Star schema US customer count (fact_customer)
#    We sum num_customers from fact_customer where country_name = 'USA'.
star_us_cust_q <- "
  SELECT SUM(fc.num_customers) AS schema_us_customers
  FROM fact_customer fc
  JOIN dim_country dc ON fc.customer_country_id = dc.country_id
  WHERE dc.country_name = 'USA';
"
star_res <- dbGetQuery(mydbcon_test, star_us_cust_q)
schema_us_customers <- star_res$schema_us_customers

# 2.3) Compare with tolerance
tolerance <- 0.10  # 10%
difference <- abs(schema_us_customers - op_us_customers)
percent_diff <- difference / op_us_customers
test_result <- if (percent_diff <= tolerance) "PASS" else "FAIL"

cat("=== Test 1: Consistency of US Customer Count ===\n")
cat("Operational US Customer Count:", op_us_customers, "\n")
cat("Star Schema US Customer Count:", schema_us_customers, "\n")
cat("Difference:", difference, "(", round(percent_diff * 100, 1), "% )\n")
cat("Test Result:", test_result, "\n\n")
cat("=== End of Test 1 ===\n")
# ----------------------------
# 3) Test 2: Total Revenue
# --------------------------------
# 3.1) Operational total revenue
#     (a) Film – exclude rows with NULL rental_id
op_film_rev <- dbGetQuery(film_con_test, "
  SELECT SUM(amount) AS rev
  FROM payment
  WHERE staff_id IS NOT NULL;
")$rev

#     (b) Music
op_music_rev <- dbGetQuery(music_con_test, "
  SELECT SUM(Total) AS rev
  FROM invoices;
")$rev

op_total_rev <- op_film_rev + op_music_rev

# 3.2) Star schema total revenue (fact_sales)
star_total_rev_q <- "
  SELECT SUM(total_revenue) AS rev
  FROM fact_sales;
"
schema_total_rev <- dbGetQuery(mydbcon_test, star_total_rev_q)$rev

# 3.3) Compare with small absolute tolerance
tol <- 0.01  # e.g. 1 cent tolerance
diff_rev <- abs(schema_total_rev - op_total_rev)
test2 <- if (diff_rev <= tol) "PASS" else "FAIL"

cat("=== Test 2: Total Revenue Match ===\n")
cat("Operational Total Revenue:", op_total_rev, "\n")
cat("Star Schema Total Revenue:", schema_total_rev, "\n")
cat("Difference:", diff_rev, "\n")
cat("Test Result:", test2, "\n\n")

# ----------------------------
# 4) Optional: Film-Only Revenue Check
#    (Sometimes helpful for debugging.)
schema_film_rev <- dbGetQuery(mydbcon_test, "
  SELECT SUM(total_revenue) AS rev
  FROM fact_sales fs
  JOIN dim_channel dc ON fs.channel_id = dc.channel_id
  WHERE dc.channel = 'film';
")$rev

cat("Film Rev – Operational:", op_film_rev,
    "vs Schema (film only):", schema_film_rev, "\n\n")
cat("=== End of Test 2 ===\n")

# ----------------------------
# 4) Test 3: Find the single (year, store_country) pair with the highest total sales
#             across both film + music, compare operational vs. star schema.
# --------------------------------

cat("\n=== Test 3: Single Country-Year with Highest Total Sales ===\n")

# (1) Film: year, store_country
op_film_year_country <- dbGetQuery(film_con_test, "
  SELECT
    CAST(strftime('%Y', p.payment_date) AS INT) AS year,
    s_ctry.country AS store_country,
    SUM(p.amount)  AS total_revenue
  FROM payment p
    JOIN rental r        ON p.rental_id = r.rental_id
    JOIN inventory i     ON r.inventory_id = i.inventory_id
    JOIN store s         ON i.store_id = s.store_id
    JOIN address a_store ON s.address_id = a_store.address_id
    JOIN city ci_store   ON a_store.city_id = ci_store.city_id
    JOIN country s_ctry  ON ci_store.country_id = s_ctry.country_id
  GROUP BY year, s_ctry.country
")

# (2) Music: year, store_country
op_music_year_country <- dbGetQuery(music_con_test, "
  SELECT
    CAST(strftime('%Y', i.InvoiceDate) AS INT) AS year,
    i.BillingCountry AS store_country,
    SUM(i.Total)     AS total_revenue
  FROM invoices i
  GROUP BY year, store_country
")

# (3) Combine & re-aggregate just in case
op_combined <- rbind(op_film_year_country, op_music_year_country)
library(stats)  # for aggregate()
op_agg <- aggregate(total_revenue ~ year + store_country, data = op_combined, sum)

# (4) Identify the single country-year with the highest total
#     We'll store that as (op_bestYear, op_bestCountry, op_bestRevenue).
idx_op <- which.max(op_agg$total_revenue)
op_bestYear    <- op_agg$year[idx_op]
op_bestCountry <- op_agg$store_country[idx_op]
op_bestRevenue <- op_agg$total_revenue[idx_op]

cat("OPERATIONAL:\n")
cat("  Highest (year, country):", op_bestYear, ",", op_bestCountry,
    " => total_revenue =", op_bestRevenue, "\n")


#### 3B) Star Schema ####
# We'll do a similar grouping: (year, store_country), sum total_revenue from fact_sales
# joined with dim_date & dim_country. Then find the max row.

star_year_country_q <- "
SELECT
  dd.year            AS year,
  dc.country_name    AS store_country,
  SUM(fs.total_revenue) AS total_revenue
FROM fact_sales fs
JOIN dim_date    dd ON fs.date_id          = dd.date_id
JOIN dim_country dc ON fs.store_country_id = dc.country_id
GROUP BY dd.year, dc.country_name
"
star_df <- dbGetQuery(mydbcon_test, star_year_country_q)

star_agg <- aggregate(total_revenue ~ year + store_country, data=star_df, sum)

idx_star <- which.max(star_agg$total_revenue)
star_bestYear    <- star_agg$year[idx_star]
star_bestCountry <- star_agg$store_country[idx_star]
star_bestRevenue <- star_agg$total_revenue[idx_star]

cat("\nSTAR SCHEMA:\n")
cat("  Highest (year, country):", star_bestYear, ",", star_bestCountry,
    " => total_revenue =", star_bestRevenue, "\n")


#### 3C) Compare ####
# We'll check if year & country match, plus a small tolerance on revenue.
yearMatch    <- (op_bestYear == star_bestYear)
countryMatch <- (op_bestCountry == star_bestCountry)

# e.g. 5% tolerance:
tolPct <- 0.05
revDiffAbs <- abs(op_bestRevenue - star_bestRevenue)
revDiffPct <- revDiffAbs / op_bestRevenue

if (yearMatch && countryMatch && (revDiffPct <= tolPct)) {
  cat("\nTest 3 Result: PASS\n")
} else {
  cat("\nTest 3 Result: FAIL\n")
  cat("  year match? ", yearMatch, "\n")
  cat("  country match? ", countryMatch, "\n")
  cat("  revenue diff abs =", revDiffAbs, "(% diff =", round(revDiffPct*100,1), ")\n")
}

cat("=== End of Test 3 ===\n")

# ----------------------------
# 4) Test 4: Top Country by Unit Sales in Q1 of Most Recent Year
# --------------------------------

# 4.1) Get most recent year
latest_year_q <- "SELECT MAX(year) AS latest_year FROM dim_date;"
latest_year <- dbGetQuery(mydbcon_test, latest_year_q)$latest_year

# 4.2) Top country by unit sales in star schema (Q1)
star_top_country_q <- sprintf("
  SELECT dc.country_name, SUM(fs.total_units) AS total_units
  FROM fact_sales fs
  JOIN dim_country dc ON fs.store_country_id = dc.country_id
  JOIN dim_date dd ON fs.date_id = dd.date_id
  WHERE dd.year = %d AND dd.quarter = 1
  GROUP BY dc.country_name
  ORDER BY total_units DESC
  LIMIT 1;", latest_year)
star_top <- dbGetQuery(mydbcon_test, star_top_country_q)

# 4.3) Operational Film units sold in Q1
op_film_units_q <- sprintf("
  SELECT s_ctry.country AS country, COUNT(*) AS total_units
  FROM payment p
  JOIN rental r ON p.rental_id = r.rental_id
  JOIN inventory i ON r.inventory_id = i.inventory_id
  JOIN store s ON i.store_id = s.store_id
  JOIN address a ON s.address_id = a.address_id
  JOIN city ci ON a.city_id = ci.city_id
  JOIN country s_ctry ON ci.country_id = s_ctry.country_id
  WHERE strftime('%%Y', p.payment_date) = '%d' AND CAST(strftime('%%m', p.payment_date) AS INTEGER) BETWEEN 1 AND 3
  GROUP BY s_ctry.country;", latest_year)
film_units <- dbGetQuery(film_con_test, op_film_units_q)

# 4.4) Operational Music units sold in Q1
op_music_units_q <- sprintf("
  SELECT i.BillingCountry AS country, SUM(ii.Quantity) AS total_units
  FROM invoices i
  JOIN invoice_items ii ON i.InvoiceId = ii.InvoiceId
  WHERE strftime('%%Y', i.InvoiceDate) = '%d'
    AND CAST(strftime('%%m', i.InvoiceDate) AS INTEGER) BETWEEN 1 AND 3
  GROUP BY i.BillingCountry;", latest_year)

music_units <- dbGetQuery(music_con_test, op_music_units_q)

# 4.5) Combine and aggregate operational film + music units
op_units <- rbind(film_units, music_units)
op_units_agg <- aggregate(total_units ~ country, data = op_units, sum)
op_units_agg <- op_units_agg[order(-op_units_agg$total_units), ]
op_top <- op_units_agg[1, ]

# 4.6) Compare top country name and total unit counts with tolerance
unit_diff <- abs(op_top$total_units - star_top$total_units)
tolerance <- 1  # allow 1 unit difference
test4_result <- if (toupper(op_top$country) == toupper(star_top$country_name) &&
                    unit_diff <= tolerance) "PASS" else "FAIL"

# 4.7) Output
cat("=== Test 4: Top Country by Unit Sales (Q1 of", latest_year, ") ===\n")
cat("Operational Top Country:", op_top$country, "with", op_top$total_units, "units\n")
cat("Star Schema Top Country:", star_top$country_name, "with", star_top$total_units, "units\n")
cat("Difference:", unit_diff, "\n")
cat("Test Result:", test4_result, "\n\n")



# # --- Total Revenue by Channel: Film ---
# 
# # Operational Film Revenue
# op_film_rev <- dbGetQuery(film_con_test, "
#   SELECT SUM(amount) AS rev
#   FROM payment;
# ")$rev
# 
# # Star Schema Film Revenue
# schema_film_rev <- dbGetQuery(mydbcon_test, "
#   SELECT SUM(fs.total_revenue) AS rev
#   FROM fact_sales fs
#   JOIN dim_channel dc ON fs.channel_id = dc.channel_id
#   WHERE dc.channel = 'film';
# ")$rev
# 
# diff_film <- abs(op_film_rev - schema_film_rev)
# film_test <- if (diff_film <= 0.01) "PASS" else "FAIL"
# 
# cat("Test: Film Revenue Match\n")
# cat("Operational Film Revenue:", op_film_rev, "\n")
# cat("Star Schema Film Revenue:", schema_film_rev, "\n")
# cat("Difference:", diff_film, "\n")
# cat("Result:", film_test, "\n\n")
# 
# 
# # --- Total Revenue by Channel: Music ---
# 
# # Operational Music Revenue
# op_music_rev <- dbGetQuery(music_con_test, "
#   SELECT SUM(Total) AS rev
#   FROM invoices;
# ")$rev
# 
# # Star Schema Music Revenue
# schema_music_rev <- dbGetQuery(mydbcon_test, "
#   SELECT SUM(fs.total_revenue) AS rev
#   FROM fact_sales fs
#   JOIN dim_channel dc ON fs.channel_id = dc.channel_id
#   WHERE dc.channel = 'music';
# ")$rev
# 
# diff_music <- abs(op_music_rev - schema_music_rev)
# music_test <- if (diff_music <= 0.01) "PASS" else "FAIL"
# 
# cat("Test: Music Revenue Match\n")
# cat("Operational Music Revenue:", op_music_rev, "\n")
# cat("Star Schema Music Revenue:", schema_music_rev, "\n")
# cat("Difference:", diff_music, "\n")
# cat("Result:", music_test, "\n\n")

# op_film_daily <- dbGetQuery(film_con_test, "
#   SELECT DATE(payment_date) AS date, SUM(amount) AS op_revenue
#   FROM payment
#   GROUP BY DATE(payment_date)
#   ORDER BY date;
# ")
# 
# schema_film_daily <- dbGetQuery(mydbcon_test, "
#   SELECT dd.date_val AS date, SUM(fs.total_revenue) AS schema_revenue
#   FROM fact_sales fs
#   JOIN dim_date dd ON fs.date_id = dd.date_id
#   JOIN dim_channel dc ON fs.channel_id = dc.channel_id
#   WHERE dc.channel = 'film'
#   GROUP BY dd.date_val
#   ORDER BY dd.date_val;
# ")
# daily_compare <- merge(op_film_daily, schema_film_daily, by = "date", all = TRUE)
# daily_compare$diff <- round(daily_compare$op_revenue - daily_compare$schema_revenue, 2)
# daily_compare[abs(daily_compare$diff) > 0.01, ]

# ----------------------------
# 5) Close test connections
dbDisconnect(film_con_test)
dbDisconnect(music_con_test)
dbDisconnect(mydbcon_test)

cat("Testing complete. Connections closed.\n")

