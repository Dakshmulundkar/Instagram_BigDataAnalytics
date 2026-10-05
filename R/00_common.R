# 00_common.R
# Shared helpers for the InstaPulse SparkR pipeline.
# Supports PRIMARY (605K Parquet) + FALLBACK (1K CSV) datasets.
# Keeps Spark session/CSV options + identifier handling in one place.

options(stringsAsFactors = FALSE)

# ---- Dataset locations ----
MAIN_PARQUET <- file.path("data", "raw", "main_instagram", "main_instagram.parquet")
FALLBACK_CSV <- file.path("data", "raw", "Instagram - Posts.csv")

# Legacy 1K-schema identifiers (kept as strings to preserve 19-digit precision).
ID_STRING_COLS <- c("post_id", "pk", "user_posted_id", "content_id", "shortcode")
# Legacy 1K-schema metric columns.
NUMERIC_COLS <- c("likes", "num_comments", "followers", "posts_count",
                  "video_view_count", "video_play_count")
# Primary 605K-schema metric columns (long in source; cast to double for rates).
MAIN_NUMERIC_COLS <- c("likes", "comments", "followers", "following", "num_posts")

resolve_project_root <- function() {
  candidates <- unique(c(
    normalizePath(getwd(), winslash = "/", mustWork = FALSE),
    normalizePath(file.path(getwd(), ".."), winslash = "/", mustWork = FALSE)
  ))
  for (cand in candidates) {
    if (file.exists(file.path(cand, MAIN_PARQUET))) return(cand)
    if (file.exists(file.path(cand, FALLBACK_CSV))) return(cand)
    if (file.exists(file.path(cand, "R", "01_ingestion.R"))) return(cand)
  }
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

# Returns list(source="primary"|"fallback", label, path).
detect_data_source <- function(project_root) {
  main <- file.path(project_root, MAIN_PARQUET)
  if (file.exists(main)) {
    return(list(source = "primary", label = "PRIMARY: main_instagram.parquet (605K)", path = main))
  }
  fb <- file.path(project_root, FALLBACK_CSV)
  return(list(source = "fallback", label = "FALLBACK: Instagram - Posts.csv (1K)", path = fb))
}

# Single-session starter: keeps shuffle partitions small for a Windows laptop.
start_spark <- function(appName) {
  sparkR.session(appName = appName,
                 sparkConfig = list(spark.sql.shuffle.partitions = "4"))
}

spark_csv_read_raw <- function(path) {
  # Multiline + no numeric inference (IDs stay strings).
  read.df(
    path, source = "csv", header = "true", inferSchema = "false",
    multiLine = "true", quote = '"', escape = '"', mode = "PERMISSIVE"
  )
}

spark_parquet_read_main <- function(path) {
  # Source schema preserved; sid/profile_id stay long, never float.
  read.df(path, source = "parquet")
}

cast_ids_to_string <- function(df) {
  for (nm in intersect(ID_STRING_COLS, columns(df))) {
    df <- withColumn(df, nm, cast(column(nm), "string"))
  }
  df
}

cast_numerics_to_double <- function(df) {
  for (nm in intersect(NUMERIC_COLS, columns(df))) {
    df <- withColumn(df, nm, cast(column(nm), "double"))
  }
  df
}

cast_main_numerics_to_double <- function(df) {
  for (nm in intersect(MAIN_NUMERIC_COLS, columns(df))) {
    df <- withColumn(df, nm, cast(column(nm), "double"))
  }
  df
}

# Canonical username normalization used by pipeline + Bloom + dashboard docs.
normalize_username_col <- function(df, col = "username") {
  withColumn(df, col, lower(trim(column(col))))
}

# Explicit timestamp parse for the primary date format "yyyy-MM-dd HH:mm:ss".
# Keeps the original `date` column and adds post_date + temporal parts.
parse_main_timestamp <- function(df, col = "date") {
  ts <- to_timestamp(column(col), "yyyy-MM-dd HH:mm:ss")
  df <- withColumn(df, "_ts", ts)
  df <- withColumn(df, "post_date", to_date(column("_ts")))
  df <- withColumn(df, "post_year", year(column("post_date")))
  df <- withColumn(df, "post_month", month(column("post_date")))
  df <- withColumn(df, "post_day", dayofmonth(column("post_date")))
  df <- withColumn(df, "day_of_week", dayofweek(column("post_date")))
  df <- withColumn(df, "post_hour", hour(column("_ts")))
  drop(df, "_ts")
}

# Engagement core + safe per-follower rates. Each derived column defined once.
# engagement = likes + comments (null-safe); rates NULL when followers <= 0.
add_engagement_cols <- function(df, likes_col = "likes", comments_col = "comments") {
  lk <- coalesce(column(likes_col), lit(0))
  cm <- coalesce(column(comments_col), lit(0))
  df <- withColumn(df, "engagement", lk + cm)
  if ("followers" %in% columns(df)) {
    fl <- column("followers")
    # when() without otherwise yields NULL (no divide-by-zero rows invented).
    df <- withColumn(df, "engagement_rate", when(fl > 0, column("engagement") / fl))
    df <- withColumn(df, "likes_per_follower", when(fl > 0, column(likes_col) / fl))
    df <- withColumn(df, "comments_per_follower", when(fl > 0, column(comments_col) / fl))
  }
  df
}

# Safe presentation label for numeric post_type (semantics NOT invented).
add_post_type_label <- function(df) {
  if (!"post_type" %in% columns(df)) return(df)
  withColumn(df, "post_type_label",
             otherwise(when(column("post_type") == 1, "Post Type 1"),
                       "Post Type 2"))
}

# Full cleaning pipeline for the PRIMARY 605K Parquet frame.
# Returns list(df=cleaned (cached), stats=list(...)).
# Shared by 02 (materialize) and 03 (rebuild when processed output missing).
clean_primary_frame <- function(posts) {
  stats <- list(rows_before = count(posts))

  # Username: trim + lowercase, string preserved.
  posts <- normalize_username_col(posts, "username")

  # Metric columns to double (rates need fractional values).
  posts <- cast_main_numerics_to_double(posts)

  # engagement = likes + comments (+ safe per-follower rates).
  posts <- add_engagement_cols(posts, "likes", "comments")

  # Explicit timestamp parse; original `date` column kept.
  posts <- parse_main_timestamp(posts, "date")

  # Neutral post-type presentation labels (no invented semantics).
  posts <- add_post_type_label(posts)

  # Null descriptions/bios -> "" so downstream text handling never breaks.
  for (nm in intersect(c("description", "bio"), columns(posts))) {
    posts <- fillna(posts, "", cols = nm)
  }

  # Remove unusable usernames (documented; validation shows 0 on this source).
  stats$null_users_removed <- count(filter(posts,
    isNull(column("username")) | (column("username") == "")))
  posts <- filter(posts, !isNull(column("username")) & (column("username") != ""))

  # Deduplicate ONLY on true post id and ONLY if duplicates actually exist.
  stats$dup_sid_removed <- 0
  if ("sid" %in% columns(posts)) {
    n_distinct_sid <- count(distinct(select(posts, "sid")))
    n_rows <- count(posts)
    if (n_distinct_sid < n_rows) {
      posts <- dropDuplicates(posts, "sid")
      stats$dup_sid_removed <- n_rows - count(posts)
    }
  }

  posts <- cache(posts)
  stats$rows_after <- count(posts)  # materialize cache
  if ("sid" %in% columns(posts)) {
    stats$distinct_sid <- count(distinct(select(posts, "sid")))
  }
  if ("post_date" %in% columns(posts)) {
    stats$null_dates <- count(filter(posts, isNull(column("post_date"))))
  }
  stats$ncols <- length(columns(posts))
  list(df = posts, stats = stats)
}

# Bounded preview helper: never dumps full frames.
bounded_preview <- function(df, n = 5) {
  showDF(df, numRows = min(n, 10), truncate = 40)
}

print_clean_stats <- function(stats) {
  cat("\n===== CLEAN VALIDATION =====\n")
  cat("ROWS_BEFORE_CLEAN:", stats$rows_before, "\n")
  cat("ROW COUNT:", stats$rows_after, "\n")
  if (!is.null(stats$distinct_sid)) cat("DISTINCT SID COUNT:", stats$distinct_sid, "\n")
  cat("NULL/EMPTY USERNAME REMOVED:", stats$null_users_removed, "\n")
  cat("DUPLICATE sid REMOVED:", stats$dup_sid_removed, "\n")
  if (!is.null(stats$null_dates)) cat("NULL DATE COUNT:", stats$null_dates, "\n")
  cat("COLUMN COUNT:", stats$ncols, "\n")
}

# Windows-safe writer for SMALL frames (aggregates, samples).
# write.df hits Hadoop permission handling without winutils; fall back to
# collect()+write.csv (safe here because callers only pass small results).
spark_write_csv_robust <- function(df, path) {
  ok <- tryCatch({
    write.df(df, path, source = "csv", header = "true", mode = "overwrite")
    TRUE
  }, error = function(e) FALSE, warning = function(w) FALSE)
  if (!ok) {
    dir.create(path, showWarnings = FALSE, recursive = TRUE)
    for (f in list.files(path, full.names = TRUE, all.files = TRUE, no.. = TRUE))
      unlink(f, recursive = TRUE, force = TRUE)
    local <- collect(df)
    write.csv(local, file.path(path, "part-00000.csv"), row.names = FALSE)
    cat("NOTE: Spark write.df unavailable (Windows Hadoop/winutils missing); used collect() fallback for", path, "\n")
  }
  invisible(TRUE)
}

# Full-frame Parquet writer: returns TRUE on success, FALSE (one-line note)
# when Windows Hadoop blocks it. Never stops the pipeline.
spark_write_parquet_best_effort <- function(df, path) {
  out <- tryCatch({
    write.df(df, path, source = "parquet", mode = "overwrite")
    cat("Wrote parquet:", path, "\n")
    TRUE
  }, error = function(e) {
    cat("NOTE: Parquet write skipped (Windows Hadoop/winutils not configured).\n")
    FALSE
  }, warning = function(w) {
    cat("NOTE: Parquet write skipped (Windows Hadoop/winutils not configured).\n")
    FALSE
  })
  invisible(out)
}

# Read a result directory written by spark_write_csv_robust (base R side).
read_result_dir <- function(path) {
  if (!dir.exists(path)) return(NULL)
  parts <- list.files(path, pattern = "\\.csv$", full.names = TRUE)
  if (length(parts) == 0) return(NULL)
  do.call(rbind, lapply(parts, function(f)
    read.csv(f, stringsAsFactors = FALSE, check.names = FALSE)))
}
