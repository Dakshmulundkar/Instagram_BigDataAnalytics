# dashboard/app.R
# InstaPulse Dashboard: 605K-record Spark analytics + username Bloom Filter.
# NEVER loads the full 605K frame: KPIs/charts come from small Spark aggregate
# files; row-level exploring uses a bounded <=10K sample (clearly labeled).

library(shiny)
library(dplyr)
library(ggplot2)
library(DT)

# Robust root resolution (works from project root or dashboard/ dir).
resolve_project_root <- function() {
  cands <- unique(c(
    normalizePath(getwd(), winslash = "/", mustWork = FALSE),
    normalizePath(file.path(getwd(), ".."), winslash = "/", mustWork = FALSE)
  ))
  for (cand in cands) {
    if (file.exists(file.path(cand, "data", "raw", "main_instagram", "main_instagram.parquet"))) return(cand)
    if (file.exists(file.path(cand, "data", "raw", "Instagram - Posts.csv"))) return(cand)
    if (file.exists(file.path(cand, "dashboard", "app.R"))) return(cand)
  }
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

project_root <- resolve_project_root()
res_dir <- file.path(project_root, "results")
sample_dir <- file.path(project_root, "data", "processed", "posts_clean_sample")
bloom_rds <- file.path(res_dir, "bloom_filter.rds")
bloom_users_rds <- file.path(res_dir, "bloom_usernames.rds")

read_res <- function(nm) {
  p <- file.path(res_dir, nm)
  if (!dir.exists(p)) return(NULL)
  parts <- list.files(p, pattern = "\\.csv$", full.names = TRUE)
  if (length(parts) == 0) return(NULL)
  do.call(rbind, lapply(parts, function(f)
    read.csv(f, stringsAsFactors = FALSE, check.names = FALSE)))
}

overall <- read_res("overall_summary")
content <- read_res("content_analysis")
categ <- read_res("category_analysis")
biz <- read_res("business_analysis")
ty <- read_res("temporal_year")
tm <- read_res("temporal_month")
tw <- read_res("temporal_weekday")
lang <- read_res("language_analysis")
quality <- read_res("quality_summary")
top_posts <- read_res("top_users_by_posts")
top_fol <- read_res("top_users_by_followers")
top_likes <- read_res("top_users_by_likes")
top_eng <- read_res("top_users_by_engagement")
samp <- read_res("engagement_sample")
cube <- read_res("filter_cube")
ucube <- read_res("top_users_cube")

# Bounded explorer sample (<=10K rows, labeled as sample-only).
explorer <- NULL
if (dir.exists(sample_dir)) {
  parts <- list.files(sample_dir, pattern = "\\.csv$", full.names = TRUE)
  if (length(parts) > 0)
    explorer <- do.call(rbind, lapply(parts, function(f)
      read.csv(f, stringsAsFactors = FALSE, check.names = FALSE)))
}
if (is.null(explorer)) explorer <- samp
# Defensive: filter comparisons need real logicals, not "TRUE" strings.
for (nm in c("explorer", "samp")) {
  d <- get(nm)
  if (!is.null(d) && "is_business_account" %in% names(d))
    assign(nm, within(d, is_business_account <- as.logical(is_business_account)))
}

data_label <- if (!is.null(overall) && !is.na(overall$total_posts[1]) &&
                   overall$total_posts[1] > 5000) {
  paste0("Main 605K Parquet (", format(overall$total_posts[1], big.mark = ","), " posts)")
} else "1K fallback CSV (demo)"

fmt <- function(x, d = 2) format(round(x, d), big.mark = ",", nsmall = d)

# ---- Bloom Filter (same double-hash as R/04_bloom_filter.R) ----
BLOOM_VERSION_APP <- "v2-doublehash-605k"
bloom <- if (file.exists(bloom_rds)) readRDS(bloom_rds) else NULL
bloom_users <- if (file.exists(bloom_users_rds)) readRDS(bloom_users_rds) else NULL
bloom_ready <- !is.null(bloom) && identical(bloom$version, BLOOM_VERSION_APP)

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
bloom_query_app <- function(x) {
  ints <- utf8ToInt(enc2utf8(tolower(trimws(x))))
  h1 <- hash1_djb2(ints); h2 <- hash2_sdbm(ints)
  idx <- vapply(seq_len(bloom$k), function(i) ((h1 + (i - 1) * h2) %% bloom$m) + 1, numeric(1))
  all(bloom$bits[idx])
}

ui <- fluidPage(
  titlePanel(paste0("InstaPulse — Instagram Big Data Analytics (", data_label, ")")),
  sidebarLayout(
    sidebarPanel(
      h4("Dashboard Filters"),
      p("Filters apply instantly to every tab with exact full-data numbers.",
        "Temporal & Quality tabs show full-data views."),
      if (!is.null(explorer) && "post_type_label" %in% names(explorer))
        selectInput("f_type", "Post Type", c("All", sort(unique(explorer$post_type_label)))),
      if (!is.null(explorer) && "is_business_account" %in% names(explorer))
        selectInput("f_biz", "Business Account", c("All", "Business", "Non-business")),
      if (!is.null(explorer) && "description_category" %in% names(explorer))
        selectInput("f_cat", "Description Category",
                    c("All", head(sort(unique(explorer$description_category)), 30))),
      hr(),
      helpText("KPIs and main charts react to filters (exact Spark-powered numbers).")
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Overview",
          fluidRow(
            column(4, wellPanel(h4("Posts"), textOutput("kpi_posts"))),
            column(4, wellPanel(h4("Users"), textOutput("kpi_users"))),
            column(4, wellPanel(h4("Avg Likes"), textOutput("kpi_likes")))
          ),
          fluidRow(
            column(4, wellPanel(h4("Avg Comments"), textOutput("kpi_comments"))),
            column(4, wellPanel(h4("Avg Followers"), textOutput("kpi_fol"))),
            column(4, wellPanel(h4("Avg Engagement"), textOutput("kpi_eng")))
          ),
          plotOutput("posts_year"),
          plotOutput("posts_month")
        ),
        tabPanel("Content",
          plotOutput("posts_type"),
          plotOutput("posts_cat"),
          plotOutput("eng_cat")
        ),
        tabPanel("Engagement",
          plotOutput("biz_eng"),
          plotOutput("fol_likes"),
          plotOutput("fol_eng")
        ),
        tabPanel("Users",
          plotOutput("u_posts"),
          plotOutput("u_eng"),
          DTOutput("top_table")
        ),
        tabPanel("Data Quality",
          h4("Null-value summary (full data)"),
          tableOutput("q_table"),
          h4("Language distribution (full data)"),
          tableOutput("lang_table"),
          p("The 605K dataset has no location field; quality metrics are shown instead of location analytics.")
        ),
        tabPanel("Bloom Filter",
          h3("Username Membership Test (real Bloom Filter)"),
          textInput("username", "Enter username", "darude"),
          verbatimTextOutput("bloom_result"),
          verbatimTextOutput("bloom_exact"),
          p("Bloom semantics: any zero bit = DEFINITELY ABSENT; all bits one = POSSIBLY PRESENT (false positives possible, never false negatives). Exact check is ground truth shown separately.")
        ),
        tabPanel("Data Explorer",
          h4(textOutput("match_count")),
          plotOutput("explorer_plot", height = 260),
          p("Bounded row-level sample (<=10K rows) with search + pagination. Not the full dataset."),
          DTOutput("table")
        )
      )
    )
  )
)

server <- function(input, output, session) {

  # ---- Global filters: exact full-data reactivity via the Spark filter cube ----
  # Each chart respects all filters EXCEPT its own dimension (BI-slicer behavior),
  # so toggling any filter visibly updates every tab.
  f_all <- reactive({
    isTRUE(input$f_type == "All") && isTRUE(input$f_biz == "All") && isTRUE(input$f_cat == "All")
  })
  biz_flag <- reactive({
    if (is.null(input$f_biz) || input$f_biz == "All") return(NULL)
    input$f_biz == "Business"
  })
  cube_except <- function(except = "") {
    req(!is.null(cube))
    x <- cube
    if (except != "type" && !is.null(input$f_type) && input$f_type != "All")
      x <- x[x$post_type_label == input$f_type, , drop = FALSE]
    bf <- biz_flag()
    if (except != "biz" && !is.null(bf)) x <- x[x$is_business_account == bf, , drop = FALSE]
    if (except != "cat" && !is.null(input$f_cat) && input$f_cat != "All")
      x <- x[x$description_category == input$f_cat, , drop = FALSE]
    x
  }
  cube_sum <- function(x) {
    p <- sum(x$post_count)
    if (length(p) == 0 || is.na(p) || p == 0) return(NULL)
    list(posts = p,
         avg_likes = sum(x$sum_likes) / p,
         avg_comments = sum(x$sum_comments) / p,
         avg_eng = sum(x$sum_engagement) / p,
         avg_fol = sum(x$sum_followers) / p,
         avg_rate = sum(x$sum_rate) / sum(x$cnt_rate))
  }
  # Same three filters applied to row-level samples (explorer + scatters).
  apply_filters <- function(x) {
    if ("post_type_label" %in% names(x) && !is.null(input$f_type) && input$f_type != "All")
      x <- x[x$post_type_label == input$f_type, , drop = FALSE]
    bf <- biz_flag()
    if ("is_business_account" %in% names(x) && !is.null(bf))
      x <- x[x$is_business_account == bf, , drop = FALSE]
    if ("description_category" %in% names(x) && !is.null(input$f_cat) && input$f_cat != "All")
      x <- x[x$description_category == input$f_cat, , drop = FALSE]
    x
  }

  # Overview KPIs: exact full-data numbers for the current filter selection.
  output$kpi_posts <- renderText({
    if (!is.null(cube)) {
      s <- cube_sum(cube_except())
      if (is.null(s)) return("0 (no matching posts)")
      format(s$posts, big.mark = ",")
    } else if (!is.null(overall)) format(overall$total_posts[1], big.mark = ",") else "N/A"
  })
  output$kpi_users <- renderText({
    # Unique users are not sliceable from sums: show the full-data total.
    if (!is.null(overall)) paste0(format(overall$unique_users[1], big.mark = ","), " (all data)") else "N/A"
  })
  output$kpi_likes <- renderText({
    if (!is.null(cube)) {
      s <- cube_sum(cube_except())
      if (is.null(s)) return("—")
      fmt(s$avg_likes)
    } else if (!is.null(overall)) fmt(overall$avg_likes[1]) else "N/A"
  })
  output$kpi_comments <- renderText({
    if (!is.null(cube)) {
      s <- cube_sum(cube_except())
      if (is.null(s)) return("—")
      fmt(s$avg_comments)
    } else if (!is.null(overall)) fmt(overall$avg_comments[1]) else "N/A"
  })
  output$kpi_fol <- renderText({
    if (!is.null(cube)) {
      s <- cube_sum(cube_except())
      if (is.null(s)) return("—")
      fmt(s$avg_fol, 1)
    } else if (!is.null(overall)) fmt(overall$avg_followers[1], 1) else "N/A"
  })
  output$kpi_eng <- renderText({
    if (!is.null(cube)) {
      s <- cube_sum(cube_except())
      if (is.null(s)) return("—")
      fmt(s$avg_eng)
    } else if (!is.null(overall)) fmt(overall$avg_engagement[1]) else "N/A"
  })

  output$posts_year <- renderPlot({
    req(!is.null(ty))
    ggplot(ty, aes(year, post_count)) + geom_col() +
      labs(title = "Posts by Year (full data)", x = "Year", y = "Posts") + theme_minimal()
  })
  output$posts_month <- renderPlot({
    req(!is.null(tm))
    ggplot(tm, aes(factor(month, levels = 1:12), post_count)) + geom_col() +
      labs(title = "Posts by Month (full data)", x = "Month", y = "Posts") + theme_minimal()
  })
  # Content tab: each chart ignores its own dimension, respects the other two.
  output$posts_type <- renderPlot({
    if (!is.null(cube)) {
      x <- cube_except("type")
      validate(need(nrow(x) > 0, "No posts match the current filters."))
      g <- aggregate(post_count ~ post_type_label, data = x, FUN = sum)
      ggplot(g, aes(post_type_label, post_count)) + geom_col() +
        labs(title = "Posts by Post Type (current filters)", x = "Post Type", y = "Posts") + theme_minimal()
    } else {
      req(!is.null(content))
      ggplot(content, aes(post_type_label, post_count)) + geom_col() +
        labs(title = "Posts by Post Type (full data)", x = "Post Type", y = "Posts") + theme_minimal()
    }
  })
  output$posts_cat <- renderPlot({
    if (!is.null(cube)) {
      x <- cube_except("cat")
      validate(need(nrow(x) > 0, "No posts match the current filters."))
      g <- aggregate(post_count ~ description_category, data = x, FUN = sum)
      top15 <- head(g[order(-g$post_count), ], 15)
      ggplot(top15, aes(reorder(description_category, post_count), post_count)) +
        geom_col() + coord_flip() +
        labs(title = "Top 15 Categories by Posts (current filters)", x = "Category", y = "Posts") + theme_minimal()
    } else {
      req(!is.null(categ))
      top15 <- head(categ[order(-categ$post_count), ], 15)
      ggplot(top15, aes(reorder(description_category, post_count), post_count)) +
        geom_col() + coord_flip() +
        labs(title = "Top 15 Categories by Posts (full data)", x = "Category", y = "Posts") + theme_minimal()
    }
  })
  output$eng_cat <- renderPlot({
    if (!is.null(cube)) {
      x <- cube_except("cat")
      validate(need(nrow(x) > 0, "No posts match the current filters."))
      g <- aggregate(cbind(sum_engagement, post_count) ~ description_category, data = x, FUN = sum)
      g$avg_engagement <- g$sum_engagement / g$post_count
      top12 <- head(g[order(-g$avg_engagement), ], 12)
      ggplot(top12, aes(reorder(description_category, avg_engagement), avg_engagement)) +
        geom_col() + coord_flip() +
        labs(title = "Avg Engagement by Category, Top 12 (current filters)",
             x = "Category", y = "Avg Engagement") + theme_minimal()
    } else {
      req(!is.null(categ))
      top12 <- head(categ[order(-categ$avg_engagement), ], 12)
      ggplot(top12, aes(reorder(description_category, avg_engagement), avg_engagement)) +
        geom_col() + coord_flip() +
        labs(title = "Avg Engagement by Category, Top 12 (full data)", x = "Category", y = "Avg Engagement") + theme_minimal()
    }
  })
  output$biz_eng <- renderPlot({
    if (!is.null(cube)) {
      x <- cube_except("biz")
      validate(need(nrow(x) > 0, "No posts match the current filters."))
      g <- aggregate(cbind(sum_engagement, post_count) ~ is_business_account, data = x, FUN = sum)
      g$account <- ifelse(g$is_business_account, "Business", "Non-business")
      g$avg_engagement <- g$sum_engagement / g$post_count
      ggplot(g, aes(account, avg_engagement)) + geom_col() +
        labs(title = "Avg Engagement: Business vs Non-Business (current filters)",
             x = "Account Type", y = "Avg Engagement") + theme_minimal()
    } else {
      req(!is.null(biz))
      biz$account <- ifelse(biz$is_business_account, "Business", "Non-business")
      ggplot(biz, aes(account, avg_engagement)) + geom_col() +
        labs(title = "Avg Engagement: Business vs Non-Business (full data)",
             x = "Account Type", y = "Avg Engagement") + theme_minimal()
    }
  })
  output$fol_likes <- renderPlot({
    req(!is.null(samp))
    s <- apply_filters(samp)
    s <- s[s$followers > 0 & s$likes >= 0, ]
    validate(need(nrow(s) > 0, "No sample points match the current filters."))
    ggplot(s, aes(followers, likes + 1)) + geom_point(alpha = 0.25, size = 0.7) +
      scale_x_log10() + scale_y_log10() +
      labs(title = "Followers vs Likes (current filters, bounded sample, log-log)",
           x = "Followers (log10)", y = "Likes + 1 (log10)") + theme_minimal()
  })
  output$fol_eng <- renderPlot({
    req(!is.null(samp) && "engagement" %in% names(samp))
    s <- apply_filters(samp)
    s <- s[s$followers > 0, ]
    validate(need(nrow(s) > 0, "No sample points match the current filters."))
    ggplot(s, aes(followers, engagement + 1)) + geom_point(alpha = 0.25, size = 0.7) +
      scale_x_log10() + scale_y_log10() +
      labs(title = "Followers vs Engagement (current filters, bounded sample, log-log)",
           x = "Followers (log10)", y = "Engagement + 1 (log10)") + theme_minimal()
  })
  # Users tab: exact per-slice tops when filtered, exact global file when All.
  output$u_posts <- renderPlot({
    req(!is.null(top_posts))
    ggplot(top_posts, aes(reorder(username, post_count), post_count)) +
      geom_col() + coord_flip() +
      labs(title = "Top Users by Post Count (full data)", x = "User", y = "Posts") + theme_minimal()
  })
  output$u_eng <- renderPlot({
    if (!is.null(ucube) && !f_all()) {
      x <- ucube
      if (input$f_type != "All") x <- x[x$post_type_label == input$f_type, , drop = FALSE]
      bf <- biz_flag()
      if (!is.null(bf)) x <- x[x$is_business_account == bf, , drop = FALSE]
      if (input$f_cat != "All") x <- x[x$description_category == input$f_cat, , drop = FALSE]
      validate(need(nrow(x) > 0, "No posts match the current filters."))
      g <- aggregate(cbind(total_engagement, post_count) ~ username, data = x, FUN = sum)
      top15 <- head(g[order(-g$total_engagement), ], 15)
      ggplot(top15, aes(reorder(username, total_engagement), total_engagement)) +
        geom_col() + coord_flip() +
        labs(title = "Top Users by Total Engagement (current filters)",
             x = "User", y = "Engagement") + theme_minimal()
    } else {
      req(!is.null(top_eng))
      ggplot(top_eng, aes(reorder(username, total_engagement), total_engagement)) +
        geom_col() + coord_flip() +
        labs(title = "Top Users by Total Engagement (full data)", x = "User", y = "Engagement") + theme_minimal()
    }
  })
  output$top_table <- renderDT({
    if (!is.null(ucube) && !f_all()) {
      x <- ucube
      if (input$f_type != "All") x <- x[x$post_type_label == input$f_type, , drop = FALSE]
      bf <- biz_flag()
      if (!is.null(bf)) x <- x[x$is_business_account == bf, , drop = FALSE]
      if (input$f_cat != "All") x <- x[x$description_category == input$f_cat, , drop = FALSE]
      g <- aggregate(cbind(total_engagement, post_count) ~ username, data = x, FUN = sum)
      datatable(head(g[order(-g$total_engagement), ], 15),
                options = list(pageLength = 15, scrollX = TRUE))
    } else {
      req(!is.null(top_eng))
      datatable(top_eng, options = list(pageLength = 15, scrollX = TRUE))
    }
  })
  output$q_table <- renderTable({
    req(!is.null(quality))
    as.data.frame(quality)
  })
  output$lang_table <- renderTable({
    req(!is.null(lang))
    as.data.frame(lang)
  })

  output$bloom_result <- renderText({
    if (!bloom_ready) return("Bloom Filter: NOT LOADED (run R/04_bloom_filter.R to generate results/bloom_filter.rds).")
    hit <- bloom_query_app(input$username)
    if (hit) "Bloom Filter: POSSIBLY PRESENT (all required bits are 1)."
    else "Bloom Filter: DEFINITELY NOT PRESENT (a required bit is 0)."
  })
  output$bloom_exact <- renderText({
    if (is.null(bloom_users)) return("Exact dataset check: username set not loaded.")
    u <- tolower(trimws(input$username))
    if (u %in% bloom_users) "Exact dataset check: PRESENT." else "Exact dataset check: ABSENT."
  })

  filtered_sample <- reactive({
    x <- explorer
    req(!is.null(x))
    apply_filters(x)
  })

  output$match_count <- renderText({
    x <- filtered_sample()
    paste0(format(nrow(x), big.mark = ","), " rows match (from ",
           format(nrow(explorer), big.mark = ","), "-row sample)")
  })

  # Lightweight reactive view of the CURRENT filter selection: aggregates
  # <=10K in-memory rows, so it redraws in milliseconds on every change.
  output$explorer_plot <- renderPlot({
    x <- filtered_sample()
    req(nrow(x) > 0)
    if ("post_type_label" %in% names(x)) {
      g <- x %>% group_by(post_type_label) %>%
        summarise(posts = dplyr::n(), .groups = "drop")
      ggplot(g, aes(post_type_label, posts)) + geom_col() +
        labs(title = "Current selection: posts by type (sample)",
             x = "Post Type", y = "Posts") + theme_minimal()
    } else {
      ggplot(data.frame(n = nrow(x)), aes("", n)) + geom_col() +
        labs(title = "Current selection size (sample)", x = "", y = "Posts") + theme_minimal()
    }
  })

  output$table <- renderDT({
    x <- filtered_sample()
    keep <- intersect(c("username", "shortcode", "post_type_label", "likes", "comments",
                        "followers", "num_posts", "is_business_account",
                        "description_category", "description_grade", "image_grade",
                        "post_date", "date"), names(x))
    if (length(keep) == 0) keep <- names(x)
    datatable(head(x[, keep, drop = FALSE], 10000),
              options = list(scrollX = TRUE, pageLength = 10,
                             deferRender = TRUE, serverSide = TRUE),
              filter = "top")
  })
}

shinyApp(ui, server)
