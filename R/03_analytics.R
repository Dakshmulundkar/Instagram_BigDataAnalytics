# 03_analytics.R
# SparkR / Spark SQL analytics (PRIMARY 605K Parquet, FALLBACK 1K CSV).
#
# DIAGNOSTIC FINDINGS (diag03, 605K frame):
#   read/count/select/SQL/group-by/avg/percentile/count-distinct: PASS (1.5-8.7s)
#   full clean + cache + materialize: PASS but slow (47.3s, memory pressure)
#   combined heavy query on the CACHED frame: STALLED (>5 min, no output)
#   write.df/save to directory: FAILS (Windows Hadoop NativeIO) - never used here
# DESIGN (from evidence):
#   - NO cache() of the 605K frame (the big cached frame stalls follow-up jobs)
#   - NO write.df (NativeIO); results via collect() of SMALL aggregates + base-R write
#   - Heavy queries SPLIT (counts/avgs separate from medians; 4 user tops separate)
#   - Each stage timed; watch for any STAGE exceeding ~60s.

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

start_spark("InstaPulse_Analytics")

# ---- Build the working frame WITHOUT cache and WITHOUT validation counts ----
# (02_preprocessing already validated counts/dedup; recounting here only adds
# full scans. Transforms are lazy; each stage below runs one bounded scan.)
if (src$source == "primary") {
  posts <- spark_parquet_read_main(src$path)
  posts <- normalize_username_col(posts, "username")
  posts <- cast_main_numerics_to_double(posts)
  posts <- add_engagement_cols(posts, "likes", "comments")
  posts <- parse_main_timestamp(posts, "date")
  posts <- add_post_type_label(posts)
  for (nm in intersect(c("description", "bio"), columns(posts))) {
    posts <- fillna(posts, "", cols = nm)
  }
  # NOTE: username null-filter + sid dedup validated dup-free in 02
  # (0 removed). Re-applied cheaply: filter only (no count checks here).
  posts <- filter(posts, !isNull(column("username")) & (column("username") != ""))
  origin <- "main parquet, transforms only (no cache)"
} else {
  # ---- Fallback 1K CSV chain (preserved behavior, tiny data) ----
  clean_csv <- file.path(project_root, "data", "processed", "posts_clean_csv")
  if (dir.exists(clean_csv)) {
    posts <- tryCatch({
      d <- read.df(clean_csv, source = "csv", header = "true",
                    inferSchema = "true", multiLine = "true",
                    quote = '"', escape = '"', mode = "PERMISSIVE")
      cast_ids_to_string(d)
    }, error = function(e) {
      parts <- list.files(clean_csv, pattern = "\\.csv$", full.names = TRUE)
      hdr <- names(read.csv(parts[1], nrows = 0, check.names = FALSE))
      cc <- stats::setNames(rep(NA_character_, length(hdr)), hdr)
      for (id in intersect(ID_STRING_COLS, hdr)) cc[[id]] <- "character"
      local <- do.call(rbind, lapply(parts, function(f)
        read.csv(f, stringsAsFactors = FALSE, check.names = FALSE, colClasses = cc)))
      for (nm in intersect(NUMERIC_COLS, names(local)))
        local[[nm]] <- suppressWarnings(as.numeric(local[[nm]]))
      createDataFrame(local)
    })
    origin <- "fallback cleaned CSV"
  } else {
    if (!file.exists(src$path)) stop("Missing dataset: ", src$path)
    posts <- spark_csv_read_raw(src$path)
    posts <- cast_ids_to_string(posts)
    posts <- cast_numerics_to_double(posts)
    origin <- "fallback raw CSV"
  }
}
cat("Frame origin:", origin, "\n")
cat("Dataset loaded. Rows:", count(posts), "\n")

result_dir <- file.path(project_root, "results")
for (d in c("overall_summary", "top_users", "content_analysis", "location_analysis",
            "category_analysis", "business_analysis",
            "top_users_by_posts", "top_users_by_followers",
            "top_users_by_likes", "top_users_by_engagement",
            "engagement_sample", "temporal_year", "temporal_month",
            "temporal_weekday", "language_analysis", "quality_summary",
            "filter_cube", "top_users_cube")) {
  p <- file.path(result_dir, d)
  if (dir.exists(p)) unlink(p, recursive = TRUE, force = TRUE)
}
dir.create(result_dir, showWarnings = FALSE, recursive = TRUE)

has <- function(cols) all(cols %in% columns(posts))
eng_col <- if ("engagement" %in% columns(posts)) "engagement" else if ("engagement_total" %in% columns(posts)) "engagement_total" else NULL
user_col <- if ("username" %in% columns(posts)) "username" else "user_posted"

createOrReplaceTempView(posts, "posts")

# One analytics stage: collect a SMALL aggregate, persist with base R, time it.
run_stage <- function(name, sdf, note = NULL) {
  t0 <- proc.time()[["elapsed"]]
  local <- collect(sdf)
  dt <- round(proc.time()[["elapsed"]] - t0, 1)
  outdir <- file.path(result_dir, name)
  dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
  write.csv(local, file.path(outdir, "part-00000.csv"), row.names = FALSE)
  msg <- paste0("STAGE ", name, ": PASS (", dt, "s) rows=", nrow(local))
  if (!is.null(note)) msg <- paste0(msg, " ", note)
  cat(msg, "\n")
  invisible(local)
}

# ---- A1. Overall counts + averages (split from medians: see header note) ----
if (!is.null(eng_col) && has(c("likes", "comments"))) {
  overall <- run_stage("overall_counts", sql(paste0(
    "SELECT COUNT(*) AS total_posts, ",
    "COUNT(DISTINCT ", user_col, ") AS unique_users, ",
    "AVG(likes) AS avg_likes, AVG(comments) AS avg_comments, ",
    "AVG(followers) AS avg_followers, AVG(", eng_col, ") AS avg_engagement, ",
    "AVG(engagement_rate) AS avg_engagement_rate, ",
    "SUM(CASE WHEN is_business_account = true THEN 1 ELSE 0 END) AS business_post_count, ",
    "SUM(CASE WHEN is_business_account = false THEN 1 ELSE 0 END) AS non_business_post_count ",
    "FROM posts")))
  cat("  total_posts=", overall$total_posts, " unique_users=", overall$unique_users, "\n")
}

# ---- A2. Medians (separate stage: percentile_approx is sort-heavy) ----
if (has(c("likes", "comments"))) {
  med <- run_stage("overall_medians", sql(
    paste0("SELECT percentile_approx(likes, 0.5) AS median_likes, ",
           "percentile_approx(comments, 0.5) AS median_comments FROM posts")))
  cat("  median_likes=", med$median_likes, " median_comments=", med$median_comments, "\n")
}

# ---- Merge A1+A2 into the canonical overall_summary output ----
oc <- read_result_dir(file.path(result_dir, "overall_counts"))
om <- read_result_dir(file.path(result_dir, "overall_medians"))
if (!is.null(oc)) {
  if (!is.null(om)) oc <- cbind(oc, om)
  unlink(file.path(result_dir, "overall_counts"), recursive = TRUE, force = TRUE)
  unlink(file.path(result_dir, "overall_medians"), recursive = TRUE, force = TRUE)
  dir.create(file.path(result_dir, "overall_summary"), showWarnings = FALSE)
  write.csv(oc, file.path(result_dir, "overall_summary", "part-00000.csv"), row.names = FALSE)
  cat("STAGE overall_summary: PASS (merge)\n")
}

# ---- B. Content analysis by post_type (neutral labels) ----
if (has(c("post_type")) && !is.null(eng_col)) {
  run_stage("content_analysis", sql(paste0(
    "SELECT post_type, post_type_label, COUNT(*) AS post_count, ",
    "AVG(likes) AS avg_likes, AVG(comments) AS avg_comments, ",
    "AVG(", eng_col, ") AS avg_engagement, AVG(followers) AS avg_followers, ",
    "AVG(image_grade) AS avg_image_grade, AVG(description_grade) AS avg_description_grade ",
    "FROM posts GROUP BY post_type, post_type_label ORDER BY post_count DESC")))
}

# ---- C. Description category analysis ----
if (has(c("description_category")) && !is.null(eng_col)) {
  run_stage("category_analysis", sql(paste0(
    "SELECT description_category, COUNT(*) AS post_count, ",
    "AVG(likes) AS avg_likes, AVG(comments) AS avg_comments, ",
    "AVG(", eng_col, ") AS avg_engagement, AVG(followers) AS avg_followers, ",
    "AVG(image_grade) AS avg_image_grade, AVG(description_grade) AS avg_description_grade ",
    "FROM posts GROUP BY description_category ORDER BY post_count DESC")))
}

# ---- D. Business vs non-business ----
if (has(c("is_business_account")) && !is.null(eng_col)) {
  run_stage("business_analysis", sql(paste0(
    "SELECT is_business_account, COUNT(*) AS post_count, ",
    "AVG(likes) AS avg_likes, AVG(comments) AS avg_comments, ",
    "AVG(", eng_col, ") AS avg_engagement, AVG(followers) AS avg_followers, ",
    "AVG(engagement_rate) AS avg_engagement_rate ",
    "FROM posts GROUP BY is_business_account")))
}

# ---- E. User activity: four separate TopK stages (never one giant sort) ----
if (user_col %in% columns(posts)) {
  run_stage("top_users_by_posts",
    sql(paste0("SELECT ", user_col, " AS username, COUNT(*) AS post_count FROM posts ",
               "GROUP BY ", user_col, " ORDER BY post_count DESC LIMIT 15")))
  if (has(c("followers"))) run_stage("top_users_by_followers",
    sql(paste0("SELECT ", user_col, " AS username, MAX(followers) AS max_followers, COUNT(*) AS post_count FROM posts ",
               "GROUP BY ", user_col, " ORDER BY max_followers DESC LIMIT 15")))
  if (has(c("likes"))) run_stage("top_users_by_likes",
    sql(paste0("SELECT ", user_col, " AS username, SUM(likes) AS total_likes, COUNT(*) AS post_count FROM posts ",
               "GROUP BY ", user_col, " ORDER BY total_likes DESC LIMIT 15")))
  if (!is.null(eng_col)) {
    top_eng <- collect(sql(paste0("SELECT ", user_col, " AS username, SUM(", eng_col, ") AS total_engagement, ",
                          "AVG(", eng_col, ") AS avg_engagement, COUNT(*) AS post_count FROM posts ",
                          "GROUP BY ", user_col, " ORDER BY total_engagement DESC LIMIT 15")))
    for (nm in c("top_users_by_engagement", "top_users")) {  # top_users = legacy name
      outdir <- file.path(result_dir, nm)
      dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
      write.csv(top_eng, file.path(outdir, "part-00000.csv"), row.names = FALSE)
    }
    cat("STAGE top_users_by_engagement (+ legacy top_users): PASS rows=15\n")
  }
}

# ---- F. Engagement relationships: bounded pseudo-random sample (viz only) ----
if (has(c("followers", "likes", "comments")) && !is.null(eng_col)) {
  samp_cols <- paste(c("followers", "likes", "comments", eng_col,
                       intersect(c("image_grade", "description_grade",
                                   "post_type_label", "post_type", "is_business_account",
                                   "description_category", "post_year"), columns(posts))),
                     collapse = ", ")
  # ~1% pseudo-random sample, capped: documents sampling, never alters source.
  run_stage("engagement_sample",
    sql(paste0("SELECT ", samp_cols, " FROM posts WHERE rand(42) < 0.01 LIMIT 5000")),
    "(visualization-only sample)")
}

# ---- I. Filter cube: full-data GROUP BY over the dashboard filter dims ----
# Tiny (<=76 rows): lets EVERY tab react to filters with exact full-data
# numbers, without re-running Spark per click.
if (has(c("post_type", "is_business_account", "description_category")) && !is.null(eng_col)) {
  run_stage("filter_cube", sql(paste0(
    "SELECT post_type, post_type_label, is_business_account, description_category, ",
    "COUNT(*) AS post_count, SUM(likes) AS sum_likes, SUM(comments) AS sum_comments, ",
    "SUM(", eng_col, ") AS sum_engagement, SUM(followers) AS sum_followers, ",
    "SUM(engagement_rate) AS sum_rate, COUNT(engagement_rate) AS cnt_rate ",
    "FROM posts GROUP BY post_type, post_type_label, ",
    "is_business_account, description_category")),
    "(powers dashboard-wide filters)")
}

# ---- J. Per-slice top users: TopK per (type x business x category) via window ----
# One scan; <=1140 rows. Exact within any filter slice (All = use static overall file).
if (has(c("is_business_account", "description_category")) && !is.null(eng_col)) {
  ucol_q <- user_col
  run_stage("top_users_cube", sql(paste0(
    "SELECT * FROM (SELECT post_type_label, is_business_account, description_category, ",
    ucol_q, " AS username, SUM(", eng_col, ") AS total_engagement, COUNT(*) AS post_count, ",
    "ROW_NUMBER() OVER (PARTITION BY post_type_label, is_business_account, description_category ",
    "ORDER BY SUM(", eng_col, ") DESC) AS rn FROM posts ",
    "GROUP BY post_type_label, is_business_account, description_category, ", ucol_q,
    ") WHERE rn <= 15")),
    "(per-slice top users)")
}

# ---- G. Temporal analytics ----
if (has(c("post_year")) && !is.null(eng_col)) {
  run_stage("temporal_year",
    sql(paste0("SELECT post_year AS year, COUNT(*) AS post_count, AVG(", eng_col, ") AS avg_engagement, ",
               "AVG(likes) AS avg_likes FROM posts GROUP BY post_year ORDER BY post_year")))
  if (has(c("post_month"))) run_stage("temporal_month",
    sql(paste0("SELECT post_month AS month, COUNT(*) AS post_count, AVG(", eng_col, ") AS avg_engagement ",
               "FROM posts GROUP BY post_month ORDER BY post_month")))
  if ("day_of_week" %in% columns(posts)) run_stage("temporal_weekday",
    sql(paste0("SELECT day_of_week AS weekday, COUNT(*) AS post_count, AVG(", eng_col, ") AS avg_engagement ",
               "FROM posts GROUP BY day_of_week ORDER BY weekday")))
}

# ---- H. Language (retained even though currently all English) ----
if ("lang" %in% columns(posts)) {
  run_stage("language_analysis",
    sql("SELECT lang, COUNT(*) AS post_count FROM posts GROUP BY lang ORDER BY post_count DESC"))
}

# ---- Data-quality summary (powers dashboard quality tab) ----
qcols <- intersect(c("username", "user_posted", "likes", "comments", "num_comments",
                     "followers", "post_date", "date_posted"),
                   columns(posts))
if (length(qcols) > 0) {
  qexpr <- paste0("SUM(CASE WHEN ", qcols, " IS NULL THEN 1 ELSE 0 END) AS null_", qcols,
                  collapse = ", ")
  run_stage("quality_summary",
    sql(paste0("SELECT COUNT(*) AS total_posts, ", qexpr, " FROM posts")))
}

# ---- Legacy fallback outputs (only when legacy columns exist) ----
if ("content_type" %in% columns(posts) && !is.null(eng_col)) {
  run_stage("content_analysis",
    agg(groupBy(posts, "content_type"),
        posts_count = count(column(eng_col)),
        average_engagement = avg(column(eng_col)),
        total_engagement = sum(column(eng_col))))
}
if ("location" %in% columns(posts) && !is.null(eng_col)) {
  loc_a <- agg(groupBy(posts, "location"),
               posts_count = count(column(eng_col)),
               average_engagement = avg(column(eng_col)),
               total_engagement = sum(column(eng_col)))
  loc_a <- arrange(loc_a, desc(loc_a$posts_count))
  run_stage("location_analysis", loc_a)
}

cat("\n====================================\n")
cat("InstaPulse Analytics completed!\n")
cat("Results saved in:", result_dir, "\n")
cat("====================================\n")

sparkR.session.stop()
