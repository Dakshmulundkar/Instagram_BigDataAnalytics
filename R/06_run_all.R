# 06_run_all.R
# Run the complete InstaPulse pipeline from the project root in R/RStudio:
#   source("R/06_run_all.R")
# No absolute paths: everything resolves relative to the project folder,
# so the project works wherever it is unzipped.

# If this file is sourced from anywhere, jump to the project root first
# (the folder that contains R/00_common.R). Never fails: falls back to getwd().
try({
  ofile <- sys.frame(1)$ofile
  if (!is.null(ofile)) {
    root <- normalizePath(dirname(ofile), winslash = "/", mustWork = FALSE)
    root <- normalizePath(file.path(root, ".."), winslash = "/", mustWork = FALSE)
    if (file.exists(file.path(root, "R", "00_common.R"))) setwd(root)
  }
}, silent = TRUE)

if (!file.exists(file.path("R", "00_common.R")))
  stop("Run from the InstaPulse project root (the folder containing R/ and dashboard/).")

cat("=== InstaPulse BDA Mini Project ===\n")
cat("Working directory:", normalizePath(getwd(), winslash = "/"), "\n")
cat("Step 1: ingestion/schema inspection\n")
source("R/01_ingestion.R")

cat("\nStep 2: preprocessing\n")
source("R/02_preprocessing.R")

cat("\nStep 3: Spark analytics\n")
source("R/03_analytics.R")

cat("\nStep 4: Bloom Filter\n")
source("R/04_bloom_filter.R")

cat("\nStep 5: visualizations\n")
source("R/05_visualizations.R")

cat("\n=== PIPELINE COMPLETE ===\n")
cat("Run shiny::runApp('dashboard') to open the dashboard.\n")
