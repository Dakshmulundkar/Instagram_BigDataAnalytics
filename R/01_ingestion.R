# 01_ingestion.R
# InstaPulse - Big Data Analytics (605K Instagram records + 1K fallback).
# PRIMARY: data/raw/main_instagram/main_instagram.parquet (605,868 rows x 20 cols)
# FALLBACK: data/raw/Instagram - Posts.csv (1,000 rows, demo only).

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

start_spark("InstaPulse_Ingestion")

if (src$source == "primary") {
  # Source schema preserved; sid/profile_id stay long, never float.
  posts <- spark_parquet_read_main(src$path)
} else {
  if (!file.exists(src$path)) stop("Missing dataset: ", src$path)
  posts <- spark_csv_read_raw(src$path)
  posts <- cast_ids_to_string(posts)
}

createOrReplaceTempView(posts, "instagram_posts")

cat("\n===== DATASET SCHEMA (source preserved; IDs type-safe) =====\n")
printSchema(posts)

cat("\n===== ROW COUNT =====\n")
rc <- count(posts)
print(rc)

cat("\n===== COLUMN COUNT =====\n")
print(length(columns(posts)))

user_col <- if ("username" %in% columns(posts)) "username" else "user_posted"
if (user_col %in% columns(posts)) {
  cat("\n===== DISTINCT USERNAMES =====\n")
  print(count(distinct(select(posts, user_col))))
  cat("\n===== NULL USERNAMES =====\n")
  print(count(filter(posts, isNull(column(user_col)))))
  for (nm in intersect(c("likes", "num_comments", "comments", "followers"), columns(posts))) {
    cat("NULL", nm, ":", count(filter(posts, isNull(column(nm)))), "\n")
  }
}

cat("\n===== SAMPLE (max 5 rows) =====\n")
bounded_preview(posts, 5)

# Keep session available for the next script if running interactively.
