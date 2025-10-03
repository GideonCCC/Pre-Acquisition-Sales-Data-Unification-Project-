# createStarSchema.PractII.ChenS.R
# CS5200 Practicum II - Create Star Schema for Analytics Datamart
# Author: Shaobo Chen
# Date: Spring 2025

# ----------------------------
# 1) Clear environment & load library
rm(list = ls())
if (!require("RMySQL")) install.packages("RMySQL")
library(RMySQL)

# ----------------------------
# 2) Connect to cloud MySQL (Aiven)
mydbcon <- dbConnect(
  RMySQL::MySQL(),
  dbname   = Sys.getenv("DB_NAME"),
  host     = Sys.getenv("DB_HOST"),
  port     = as.integer(Sys.getenv("DB_PORT")),
  user     = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD"),
  sslmode  = "REQUIRED"
)


# ----------------------------
# 3) Drop existing tables (fact first to avoid FKs)
dbExecute(dbcon, "DROP TABLE IF EXISTS fact_customer;")
dbExecute(dbcon, "DROP TABLE IF EXISTS fact_sales;")
dbExecute(dbcon, "DROP TABLE IF EXISTS dim_channel;")
dbExecute(dbcon, "DROP TABLE IF EXISTS dim_country;")
dbExecute(dbcon, "DROP TABLE IF EXISTS dim_date;")

# ----------------------------
# 4) Create dimension tables

# 4.1 dim_date
dbExecute(dbcon, "
CREATE TABLE dim_date (
  date_id     INT AUTO_INCREMENT PRIMARY KEY,
  date_val    DATE NOT NULL,
  day         INT,
  month       INT,
  quarter     INT,
  year        INT,
  month_name  VARCHAR(15)
);
")

# 4.2 dim_country
dbExecute(dbcon, "
CREATE TABLE dim_country (
  country_id   INT AUTO_INCREMENT PRIMARY KEY,
  country_name VARCHAR(100) NOT NULL
);
")

# 4.3 dim_channel
dbExecute(dbcon, "
CREATE TABLE dim_channel (
  channel_id INT AUTO_INCREMENT PRIMARY KEY,
  channel    VARCHAR(20) NOT NULL
);
")

# Seed dim_channel
dbExecute(dbcon, "INSERT INTO dim_channel (channel) VALUES ('film'),('music');")

# ----------------------------
# 5) Create fact tables

# 5.1 fact_sales: revenue & units by store country
dbExecute(dbcon, "
CREATE TABLE fact_sales (
  fact_sales_id    INT AUTO_INCREMENT PRIMARY KEY,
  date_id          INT NOT NULL,
  store_country_id INT NOT NULL,
  channel_id       INT NOT NULL,
  total_revenue    DECIMAL(12,4),
  average_revenue  DECIMAL(12,2),
  total_units      INT,
  average_units    DECIMAL(12,2),
  num_customers    INT,
  min_units        INT,
  max_units        INT,
  CONSTRAINT fk_sales_date    FOREIGN KEY (date_id)          REFERENCES dim_date(date_id),
  CONSTRAINT fk_sales_store   FOREIGN KEY (store_country_id) REFERENCES dim_country(country_id),
  CONSTRAINT fk_sales_channel FOREIGN KEY (channel_id)       REFERENCES dim_channel(channel_id)
);
")

# 5.2 fact_customer: distinct customers by customer country
dbExecute(dbcon, "
CREATE TABLE fact_customer (
  customer_country_id  INT NOT NULL,
  channel_id           INT NOT NULL,
  num_customers        INT NOT NULL,
  PRIMARY KEY (customer_country_id, channel_id),
  CONSTRAINT fk_cust_country FOREIGN KEY (customer_country_id) REFERENCES dim_country(country_id),
  CONSTRAINT fk_cust_channel FOREIGN KEY (channel_id)          REFERENCES dim_channel(channel_id)
);
")

# ----------------------------
# 6) Finalize
dbDisconnect(dbcon)
cat("Star schema created: dim_date, dim_country, dim_channel, fact_sales, fact_customer\n")

