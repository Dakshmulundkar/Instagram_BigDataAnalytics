# 02_preprocessing.R
# Cleans the Instagram dataset (PRIMARY 605K Parquet, FALLBACK 1K CSV).

options(stringsAsFactors = FALSE)

spark_home <- Sys.getenv("SPARK_HOME")
if (spark_home != "") {
  .libPaths(c(file.path(spark_home, "R", "lib"), .libPaths()))
}
library(SparkR)

source(file.path(normalizePath(getwd(), winslash = "/", mustWork = FALSE),
                 "R", "00_common.R"))

project_root <- resolve_project_root()
src <- detect_data_source(project_root)
cat("DATA SOURCE:", src$label, "\n")

start_spark("InstaPulse_Preprocessing")

processed_dir <- file.path(project_root, "data", "processed")
dir.create(processed_dir, showWarnings = FALSE, recursive = TRUE)

if (src$source == "primary") {
  posts <- spark_parquet_read_main(src$path)

  cleaned <- clean_primary_frame(posts)
  posts <- cleaned$df
  print_clean_stats(cleaned$stats)

  # Full-frame Parquet first (preferred). On Windows without winutils this
  # fails safely; the bounded sample below keeps the dashboard working.
  parquet_ok <- spark_write_parquet_best_effort(
    posts, file.path(processed_dir, "posts_clean_parquet"))

  if (!parquet_ok) {
    # Visualization-only sample: 10K rows, documented as sample-only.
    # Full-frame analytics run in Spark (03) without collecting 605K rows.
    sample_df <- limit(posts, 10000)
    spark_write_csv_robust(sample_df,
      file.path(processed_dir, "posts_clean_sample"))
    cat("Wrote visualization sample (10000 rows, sample-only):",
        file.path(processed_dir, "posts_clean_sample"), "\n")
  }

  cat("\nPreprocessing complete. Processed rows:", cleaned$stats$rows_after, "\n")
  bounded_preview(posts, 5)

} else {
  # ---- FALLBACK 1K CSV path (preserved P0 behavior) ----
  if (!file.exists(src$path)) stop("Missing dataset: ", src$path)
  posts <- spark_csv_read_raw(src$path)
  posts <- cast_ids_to_string(posts)

  cols <- columns(posts)
  rows_before <- count(posts)

  if ("user_posted" %in% cols) {
    posts <- withColumn(posts, "user_posted", lower(trim(column("user_posted"))))
  }
  posts <- cast_numerics_to_double(posts)

  if (all(c("likes", "num_comments") %in% columns(posts))) {
    posts <- withColumn(posts, "engagement",
                        coalesce(column("likes"), lit(0)) + coalesce(column("num_comments"), lit(0)))
    posts <- withColumn(posts, "engagement_total", column("engagement"))
  }

  if ("date_posted" %in% columns(posts)) {
    ts_iso <- to_timestamp(column("date_posted"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    ts_alt <- to_timestamp(column("date_posted"), "yyyy-MM-dd HH:mm:ss z")
    ts_fallback <- to_timestamp(column("date_posted"))
    posts <- withColumn(posts, "_ts", coalesce(ts_iso, ts_alt, ts_fallback))
    posts <- withColumn(posts, "post_date", to_date(column("_ts")))
    posts <- withColumn(posts, "post_year", year(column("post_date")))
    posts <- withColumn(posts, "post_month", month(column("post_date")))
    posts <- withColumn(posts, "day_of_week", dayofweek(column("post_date")))
    posts <- drop(posts, "_ts")
  }

  if ("location" %in% columns(posts)) {
    loc <- trim(column("location"))
    posts <- withColumn(posts, "location",
                        otherwise(when(isNull(loc) | (loc == ""), "Unknown"), loc))
  }

  null_user_before <- 0
  if ("user_posted" %in% columns(posts)) {
    null_user_before <- count(filter(posts,
      isNull(column("user_posted")) | (column("user_posted") == "")))
    posts <- filter(posts, !isNull(column("user_posted")) & (column("user_posted") != ""))
  }

  dup_removed <- 0
  if ("post_id" %in% columns(posts)) {
    n_pre_dedup <- count(posts)
    posts <- dropDuplicates(posts, "post_id")
    dup_removed <- n_pre_dedup - count(posts)
  }

  posts <- cache(posts)
  cnt <- count(posts)

  cat("\n===== CLEAN VALIDATION (fallback 1K) =====\n")
  cat("ROWS_BEFORE_CLEAN:", rows_before, "\n")
  cat("ROW COUNT:", cnt, "\n")
  cat("NULL/EMPTY USERNAME REMOVED:", null_user_before, "\n")
  cat("DUPLICATE post_id REMOVED:", dup_removed, "\n")
  cat("COLUMN COUNT:", length(columns(posts)), "\n")

  spark_write_csv_robust(posts, file.path(processed_dir, "posts_clean_csv"))
  cat("Wrote fallback CSV:", file.path(processed_dir, "posts_clean_csv"), "\n")
  cat("\nPreprocessing complete. Processed rows:", cnt, "\n")
  bounded_preview(posts, 5)
}

sparkR.session.stop()
