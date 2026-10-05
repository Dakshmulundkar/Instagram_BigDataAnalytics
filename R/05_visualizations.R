# 05_visualizations.R
# ggplot2 charts from SMALL aggregate/sample results (never the 605K frame).
# Scatters use the bounded <=5000-row engagement_sample with log10 axes;
# sampling is documented on-chart and never alters source analytics.

options(stringsAsFactors = FALSE)
library(ggplot2)

source(file.path(normalizePath(getwd(), winslash = "/", mustWork = FALSE),
                 "R", "00_common.R"))

project_root <- resolve_project_root()
res <- function(nm) read_result_dir(file.path(project_root, "results", nm))
fig_dir <- file.path(project_root, "results", "figures")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

save_fig <- function(p, nm) {
  ggsave(file.path(fig_dir, nm), p, width = 8, height = 5)
  cat("chart:", nm, "\n")
}

overall <- res("overall_summary")
content <- res("content_analysis")
categ <- res("category_analysis")
biz <- res("business_analysis")
samp <- res("engagement_sample")
ty <- res("temporal_year")
tm <- res("temporal_month")
top_posts <- res("top_users_by_posts")

# 1. Posts by post type (neutral labels; semantics not invented).
if (!is.null(content) && all(c("post_type_label", "post_count") %in% names(content))) {
  save_fig(ggplot(content, aes(reorder(post_type_label, post_count), post_count)) +
    geom_col() + coord_flip() +
    labs(title = "Posts by Post Type", x = "Post Type", y = "Posts (full data)") +
    theme_minimal(), "posts_by_post_type.png")
}

# 2. Posts by description category (top 15).
if (!is.null(categ) && all(c("description_category", "post_count") %in% names(categ))) {
  top15 <- head(categ[order(-categ$post_count), ], 15)
  save_fig(ggplot(top15, aes(reorder(description_category, post_count), post_count)) +
    geom_col() + coord_flip() +
    labs(title = "Top 15 Description Categories by Post Count",
         x = "Category", y = "Posts (full data)") + theme_minimal(),
    "posts_by_category.png")
}

# 3. Business vs non-business engagement.
if (!is.null(biz) && all(c("is_business_account", "avg_engagement") %in% names(biz))) {
  biz$account <- ifelse(biz$is_business_account, "Business", "Non-business")
  save_fig(ggplot(biz, aes(account, avg_engagement)) + geom_col() +
    labs(title = "Average Engagement: Business vs Non-Business",
         x = "Account Type", y = "Avg Engagement (full data)") + theme_minimal(),
    "business_engagement.png")
}

# 4-5. Followers vs likes / engagement (bounded sample, log-log for skew).
if (!is.null(samp) && all(c("followers", "likes") %in% names(samp))) {
  s <- samp[samp$followers > 0 & samp$likes >= 0, ]
  save_fig(ggplot(s, aes(followers, likes + 1)) + geom_point(alpha = 0.25, size = 0.7) +
    scale_x_log10() + scale_y_log10() +
    labs(title = "Followers vs Likes (bounded ~1% sample, log-log)",
         x = "Followers (log10)", y = "Likes + 1 (log10)") + theme_minimal(),
    "followers_vs_likes.png")
}
ecol <- if (!is.null(samp) && "engagement" %in% names(samp)) "engagement" else NULL
if (!is.null(ecol)) {
  s <- samp[samp$followers > 0, ]
  save_fig(ggplot(s, aes(followers, .data[[ecol]] + 1)) + geom_point(alpha = 0.25, size = 0.7) +
    scale_x_log10() + scale_y_log10() +
    labs(title = "Followers vs Engagement (bounded ~1% sample, log-log)",
         x = "Followers (log10)", y = "Engagement + 1 (log10)") + theme_minimal(),
    "followers_vs_engagement.png")
}

# 6. Engagement by description category (top 12).
if (!is.null(categ) && all(c("description_category", "avg_engagement") %in% names(categ))) {
  top12 <- head(categ[order(-categ$avg_engagement), ], 12)
  save_fig(ggplot(top12, aes(reorder(description_category, avg_engagement), avg_engagement)) +
    geom_col() + coord_flip() +
    labs(title = "Average Engagement by Description Category (Top 12)",
         x = "Category", y = "Avg Engagement (full data)") + theme_minimal(),
    "engagement_by_category.png")
}

# 7. Engagement/posts by year + posts by month.
if (!is.null(ty) && all(c("year", "avg_engagement") %in% names(ty))) {
  save_fig(ggplot(ty, aes(year, avg_engagement)) + geom_line() + geom_point() +
    labs(title = "Average Engagement by Year", x = "Year", y = "Avg Engagement (full data)") +
    theme_minimal(), "engagement_by_year.png")
}
if (!is.null(tm) && all(c("month", "post_count") %in% names(tm))) {
  save_fig(ggplot(tm, aes(factor(month, levels = 1:12), post_count)) + geom_col() +
    labs(title = "Posts by Month (Seasonality)", x = "Month", y = "Posts (full data)") +
    theme_minimal(), "posts_by_month.png")
}

# 8-9. Grade vs likes: binned means from the bounded sample (no 605K scatter).
binned_grade_plot <- function(s, grade_col, title, fname) {
  s <- s[!is.na(s[[grade_col]]) & s$likes >= 0, ]
  if (nrow(s) < 50) return(NULL)
  s$bin <- cut(s[[grade_col]], breaks = 10)
  agg <- aggregate(s$likes, by = list(bin = s$bin), FUN = mean)
  names(agg) <- c("bin", "mean_likes")
  save_fig(ggplot(agg, aes(bin, mean_likes, group = 1)) + geom_line() + geom_point() +
    labs(title = paste0(title, " (bounded ~1% sample, binned)"),
         x = paste0(grade_col, " bin"), y = "Mean Likes") +
    theme_minimal() + theme(axis.text.x = element_text(angle = 30, hjust = 1)),
    fname)
}
if (!is.null(samp) && all(c("image_grade", "likes") %in% names(samp)))
  binned_grade_plot(samp, "image_grade", "Image Grade vs Likes", "image_grade_vs_likes.png")
if (!is.null(samp) && all(c("description_grade", "likes") %in% names(samp)))
  binned_grade_plot(samp, "description_grade", "Description Grade vs Likes", "description_grade_vs_likes.png")

# 10. Top users by post count.
if (!is.null(top_posts) && all(c("username", "post_count") %in% names(top_posts))) {
  save_fig(ggplot(top_posts, aes(reorder(username, post_count), post_count)) +
    geom_col() + coord_flip() +
    labs(title = "Top Users by Post Count", x = "User", y = "Posts (full data)") +
    theme_minimal(), "top_users_by_activity.png")
}

# ---- Fallback-mode legacy charts (1K schema only) ----
dat_try <- NULL
clean_dir <- file.path(project_root, "data", "processed", "posts_clean_csv")
if (dir.exists(clean_dir) && is.null(content)) {
  parts <- list.files(clean_dir, pattern = "\\.csv$", full.names = TRUE)
  if (length(parts) > 0)
    dat_try <- do.call(rbind, lapply(parts, function(f)
      read.csv(f, stringsAsFactors = FALSE, check.names = FALSE)))
  if (!is.null(dat_try) && "content_type" %in% names(dat_try) && "engagement" %in% names(dat_try)) {
    agg_ct <- aggregate(engagement ~ content_type, data = dat_try, FUN = mean)
    save_fig(ggplot(agg_ct, aes(reorder(content_type, engagement), engagement)) +
      geom_col() + coord_flip() +
      labs(title = "Average Engagement by Content Type (fallback 1K)",
           x = "Content Type", y = "Average Engagement") + theme_minimal(),
      "engagement_by_content_type.png")
  }
}

cat("Charts created in results/figures.\n")
