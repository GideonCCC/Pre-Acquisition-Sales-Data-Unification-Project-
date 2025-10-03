# sandbox.PracticumII.ChenS.R
# CS5200 Practicum II Pre-Work Sandbox
# Author: Shaobo Chen
# Date: Spring 2025

# 1) Clear the environment and load required libraries
rm(list = ls())

# Load packages for SQLite and MySQL
if (!require("RSQLite")) install.packages("RSQLite")
library(RSQLite)

if (!require("RMySQL")) install.packages("RMySQL")
library(RMySQL)

# ----------------------------
# 2) Connect to the Film Sales SQLite Database and inspect key tables
cat("********** Section 2: Film Sales SQLite Database **********\n")
cat("Connecting to film-sales.db...\n")
film_con <- dbConnect(SQLite(), dbname = "film-sales.db")

# List all tables in the film database
film_tables <- dbListTables(film_con)
cat("Film database tables:\n")
print(film_tables)

# Inspect a key table - using the 'payment' table as an example (commonly used for sales)
if ("payment" %in% film_tables) {
  cat("\nDisplaying a sample of the 'payment' table:\n")
  film_payment_sample <- dbGetQuery(film_con, "SELECT * FROM payment LIMIT 5;")
  print(film_payment_sample)
  
  cat("\nTable schema for 'payment':\n")
  film_payment_info <- dbGetQuery(film_con, "PRAGMA table_info(payment);")
  print(film_payment_info)
} else {
  cat("\nThe table 'payment' was not found in the film-sales database.\n")
}

# Disconnect from the film database
dbDisconnect(film_con)
cat("Disconnected from film-sales.db.\n\n")

# ----------------------------
# 3) Connect to the Music Sales SQLite Database and inspect key tables
cat("********** Section 3: Music Sales SQLite Database **********\n")
cat("Connecting to music-sales.db...\n")
music_con <- dbConnect(SQLite(), dbname = "music-sales.db")

# List all tables in the music database
music_tables <- dbListTables(music_con)
cat("Music database tables:\n")
print(music_tables)

# Inspect a key table - using the 'invoices' table
if ("invoices" %in% music_tables) {
  cat("\nDisplaying a sample of the 'invoices' table:\n")
  music_invoices_sample <- dbGetQuery(music_con, "SELECT * FROM invoices LIMIT 5;")
  print(music_invoices_sample)
  
  cat("\nTable schema for 'invoices':\n")
  music_invoices_info <- dbGetQuery(music_con, "PRAGMA table_info(invoices);")
  print(music_invoices_info)
} else {
  cat("\nThe table 'invoices' was not found in the music-sales database.\n")
}

# Disconnect from the music database
dbDisconnect(music_con)
cat("Disconnected from music-sales.db.\n\n")

# ----------------------------
# 4) Connect to the Cloud MySQL Database on Aiven, create a test table, insert sample data, and verify
cat("********** Section 4: Cloud MySQL Database on Aiven **********\n")
cat("Connecting to cloud MySQL database on Aiven...\n")
mydbcon <- dbConnect(
  RMySQL::MySQL(),
  dbname   = Sys.getenv("DB_NAME"),
  host     = Sys.getenv("DB_HOST"),
  port     = as.integer(Sys.getenv("DB_PORT")),
  user     = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD"),
  sslmode  = "REQUIRED"
)


cat("Connected to MySQL database successfully.\n")

# Create a test table 'test_sandbox'
cat("Creating test table 'test_sandbox'...\n")
dbExecute(dbcon, "DROP TABLE IF EXISTS test_sandbox;")
dbExecute(dbcon, "
  CREATE TABLE test_sandbox (
    id INT PRIMARY KEY,
    description VARCHAR(100)
  );
")
cat("Test table 'test_sandbox' created.\n")

# Insert sample data into the test table
cat("Inserting sample data into 'test_sandbox'...\n")
dbExecute(dbcon, "
  INSERT INTO test_sandbox (id, description)
  VALUES
    (1, 'Sample record 1'),
    (2, 'Sample record 2'),
    (3, 'Sample record 3');
")
cat("Sample data inserted into 'test_sandbox'.\n")

# Query the test table to verify the inserted data
cat("Querying data from 'test_sandbox'...\n")
mysql_result <- dbGetQuery(dbcon, "SELECT * FROM test_sandbox;")
print(mysql_result)

# Disconnect from the MySQL database
dbDisconnect(dbcon)
cat("Disconnected from MySQL database.\n")
cat("********** Combined Sandbox Execution Complete **********\n")