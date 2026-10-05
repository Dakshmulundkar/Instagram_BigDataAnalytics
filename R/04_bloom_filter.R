# 04_bloom_filter.R
# Bloom Filter over normalized usernames (PRIMARY 605K -> ~485K users).
# Standard semantics: any zero bit -> DEFINITELY ABSENT;
# all bits one -> PROBABLY PRESENT (false positives possible, no false negatives).
# Only the DISTINCT username column is collected (one short string column);
# the full 605K frame never leaves Spark.

options(stringsAsFactors = FALSE)

spark_home <- Sys.getenv("SPARK_HOME")
if (spark_home != "") {
  .libPaths(c(file.path(spark_home, "R", "lib"), .libPaths()))
}

BLOOM_VERSION <- "v2-doublehash-605k"
ID_COLS_CHAR <- c("post_id", "pk", "user_posted_id", "content_id", "shortcode")

has_sparkr <- requireNamespace("SparkR", quietly = TRUE)

resolve_root_04 <- function() {
  cands <- unique(c(normalizePath(getwd(), winslash = "/", mustWork = FALSE),
                    normalizePath(file.path(getwd(), ".."), winslash = "/", mustWork = FALSE)))
  for (cand in cands) {
    if (file.exists(file.path(cand, "data", "raw", "main_instagram", "main_instagram.parquet"))) return(cand)
    if (file.exists(file.path(cand, "data", "raw", "Instagram - Posts.csv"))) return(cand)
  }
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

project_root <- resolve_root_04()
main_pq <- file.path(project_root, "data", "raw", "main_instagram", "main_instagram.parquet")
clean_sample_dir <- file.path(project_root, "data", "processed", "posts_clean_sample")
clean_csv_dir <- file.path(project_root, "data", "processed", "posts_clean_csv")
raw_file <- file.path(project_root, "data", "raw", "Instagram - Posts.csv")

normalize_user <- function(x) tolower(trimws(x))

# Two independent base hashes (djb2 + sdbm-like); combined via double hashing.
# Must stay in sync with dashboard/app.R (same BLOOM_VERSION).
hash1_djb2 <- function(ints) {
  h <- 5381
  for (v in ints) h <- (h * 33 + v) %% 2147483647
  h
}
hash2_sdbm <- function(ints) {
  h <- 0
  for (v in ints) h <- (v + (h * 65599)) %% 2147483647
  if (h == 0) h <- 1
  h
}
bloom_indices <- function(x, k, m) {
  ints <- utf8ToInt(enc2utf8(normalize_user(x)))
  h1 <- hash1_djb2(ints); h2 <- hash2_sdbm(ints)
  as.integer(((h1 + (seq_len(k) - 1) * h2) %% m) + 1)
}

# ---- PRIMARY path: Spark DISTINCT username (single column collect) ----
users <- NULL
data_source <- NULL
if (file.exists(main_pq) && has_sparkr) {
  suppressMessages(library(SparkR))
  sparkR.session(appName = "InstaPulse_Bloom",
                 sparkConfig = list(spark.sql.shuffle.partitions = "4"))
  df <- read.df(main_pq, source = "parquet")
  df <- withColumn(df, "username", lower(trim(column("username"))))
  udf <- collect(distinct(select(df, "username")))
  sparkR.session.stop()
  users <- unique(tolower(trimws(udf$username)))
  users <- users[!is.na(users) & users != ""]
  data_source <- "PRIMARY main_instagram.parquet"
} else {
  # ---- FALLBACK: cleaned sample / cleaned CSV / raw 1K CSV ----
  parts <- if (dir.exists(clean_sample_dir))
    list.files(clean_sample_dir, pattern = "\\.csv$", full.names = TRUE) else character(0)
  if (length(parts) == 0 && dir.exists(clean_csv_dir))
    parts <- list.files(clean_csv_dir, pattern = "\\.csv$", full.names = TRUE)
  if (length(parts) > 0) {
    dat <- do.call(rbind, lapply(parts, function(f)
      read.csv(f, stringsAsFactors = FALSE, check.names = FALSE)))
  } else {
    if (!file.exists(raw_file)) stop("Missing dataset: ", raw_file)
    hdr <- names(read.csv(raw_file, nrows = 0, check.names = FALSE))
    cc <- stats::setNames(rep(NA_character_, length(hdr)), hdr)
    for (id in intersect(ID_COLS_CHAR, hdr)) cc[[id]] <- "character"
    dat <- read.csv(raw_file, stringsAsFactors = FALSE, check.names = FALSE, colClasses = cc)
  }
  ucol <- if ("username" %in% names(dat)) "username" else "user_posted"
  if (!ucol %in% names(dat)) stop("username column not found.")
  users <- unique(tolower(trimws(dat[[ucol]])))
  users <- users[!is.na(users) & users != ""]
  data_source <- "FALLBACK 1K CSV/sample"
}
cat("DATA SOURCE:", data_source, "\n")

n <- length(users)
cat("Distinct normalized usernames:", n, "\n")

# Standard formulas: m = -(n ln p)/(ln2)^2, k = (m/n) ln2.
false_positive_target <- 0.01
m <- max(128, ceiling(-(n * log(false_positive_target)) / (log(2)^2)))
k <- min(20, max(2, round((m / n) * log(2))))
theoretical_fpr <- (1 - exp(-k * n / m))^k
cat("Bloom params: n=", n, " m=", m, " k=", k,
    " theory_fpr=", round(theoretical_fpr, 5), "\n", sep = "")

bits <- rep(FALSE, m)
for (i in seq_along(users)) {
  bits[bloom_indices(users[i], k, m)] <- TRUE
  if (i %% 100000 == 0) cat("  inserted", i, "of", n, "\n")
}
cat("Insertion complete.\n")

bloom_query <- function(x) {
  all(bits[bloom_indices(x, k, m)])
}

# Bounded tests: known inserts + far synthetic + near-miss mutations.
set.seed(42)
positive_queries <- base::sample(users, min(200, length(users)))
far_neg <- paste0("not_a_real_user_", seq_len(500))
mutate_one <- function(u) {
  chars <- strsplit(u, "")[[1]]
  if (length(chars) == 0) return(paste0(u, "x"))
  pos <- base::sample(seq_along(chars), 1)
  chars[pos] <- base::sample(c(letters, 0:9), 1)
  paste(chars, collapse = "")
}
near_neg <- unique(vapply(base::sample(users, min(300, length(users))), mutate_one, character(1)))
near_neg <- near_neg[!near_neg %in% users]
negative_queries <- head(unique(c(far_neg, near_neg)), 500)

positive_result <- vapply(positive_queries, bloom_query, logical(1))
negative_result <- vapply(negative_queries, bloom_query, logical(1))

# Inserted usernames must have no false negatives.
stopifnot(all(positive_result))

false_positives <- sum(negative_result)
false_positive_rate <- false_positives / length(negative_queries)

results <- data.frame(
  data_source = data_source,
  inserted_users = n,
  bit_array_size = m,
  hash_functions = k,
  hash_version = BLOOM_VERSION,
  false_positive_target = false_positive_target,
  theoretical_fpr = theoretical_fpr,
  positive_queries = length(positive_queries),
  positive_false_negatives = sum(!positive_result),
  negative_queries = length(negative_queries),
  false_positives = false_positives,
  false_positive_rate = false_positive_rate
)

dir.create(file.path(project_root, "results"), showWarnings = FALSE, recursive = TRUE)
write.csv(results, file.path(project_root, "results", "bloom_filter_results.csv"),
          row.names = FALSE)
saveRDS(list(bits = bits, m = m, k = k, version = BLOOM_VERSION, n = n,
             fp_target = false_positive_target),
        file.path(project_root, "results", "bloom_filter.rds"))
# Exact username set for the dashboard's ground-truth check (one short column).
saveRDS(users, file.path(project_root, "results", "bloom_usernames.rds"))

cat("\n===== BLOOM FILTER RESULT (target vs theoretical vs empirical) =====\n")
print(results)
cat("\nExample positive query:", positive_queries[1], "=>", bloom_query(positive_queries[1]), "\n")
cat("Example negative query:", negative_queries[1], "=>", bloom_query(negative_queries[1]), "\n")
