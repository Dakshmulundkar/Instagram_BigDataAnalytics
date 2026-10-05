# InstaPulse — BDA Mini Project Report

## 1. Title
**InstaPulse: Instagram Post Analytics using Apache Spark and R**

## 2. Introduction
Social media platforms generate large volumes of heterogeneous data. This mini project analyzes Instagram post data using Big Data Analytics techniques. Apache Spark is used for scalable data processing, while R is used for analysis and visualization.

## 3. Problem Statement
Instagram posts contain information such as likes, comments, users, dates, locations and content types. Manually analyzing such data is difficult. The proposed system processes Instagram-derived historical data and extracts useful patterns in engagement, posting activity and user behavior.

## 4. Objectives
- Process Instagram post data using Apache Spark.
- Clean and transform large/semi-large datasets.
- Analyze likes, comments, engagement and posting trends.
- Identify active users and locations.
- Implement a Bloom Filter for username membership testing.
- Visualize the results using R.
- Provide an interactive Shiny dashboard.

## 5. Dataset
Primary: `data/raw/main_instagram/main_instagram.parquet` — 605,868 records,
20 source columns, 485,125 unique usernames, date range 2012-02-07 to 2019-08-14.
Business accounts: 218,285 posts; non-business: 387,583. Averages: likes 205.84,
comments 5.55, followers 7300.31. `post_type` is numeric (1 = 584,061 posts,
2 = 21,807 posts) with no documented Image/Reel semantics, so neutral labels
"Post Type 1/2" are used. Fallback/demo: `data/raw/Instagram - Posts.csv`
(1,000 rows), used only when Parquet is unavailable, never merged.

## 6. Technologies
- R
- Apache Spark / SparkR
- R Shiny
- ggplot2
- dplyr
- Bloom Filter

## 7. System Architecture
Dataset → Spark Ingestion → Cleaning → Spark Analytics → Bloom Filter → Processed Results → R Visualization → Shiny Dashboard

## 8. Methodology
### 8.1 Data ingestion
The Parquet dataset is read using SparkR (`read.df`, source preserved, IDs kept
type-safe, `spark.sql.shuffle.partitions = 4`, temp view `instagram_posts`).
The 1K CSV remains as explicit fallback.

### 8.2 Preprocessing
Usernames are normalized, numeric fields are converted, the `yyyy-MM-dd HH:mm:ss`
date is parsed explicitly (original kept, year/month/day/weekday/hour derived),
engagement = likes + comments with safe per-follower rates (NULL when
followers = 0), and `sid` uniqueness is verified (605,868 distinct, no dedup needed).

### 8.3 Analytics (all in Spark/Spark SQL; only small aggregates collected)
The project calculates:
- total posts (605,868), unique users (485,125)
- average/median likes (205.84 / 42) and comments (5.55 / 1)
- engagement = likes + comments, plus safe engagement_rate
- business (218,285) vs non-business (387,583) comparison
- post-type, description-category, user-activity (posts/followers/likes/engagement),
  temporal (year/month/weekday), language, and data-quality summaries
- bounded <=5000-row sample for engagement-relationship visualization only

### 8.4 Bloom Filter
A Bloom Filter over ~485K normalized usernames (m = 4,649,952 bits, k = 7,
target FPR 0.01, theoretical ~0.0100, empirical ~0.008, zero false negatives).
It returns "possibly present" or "definitely absent"; the dashboard shows the
Bloom verdict alongside an exact ground-truth check.

### 8.5 Visualization
R/ggplot2 charts from small aggregates/bounded samples with log10 axes for
skewed metrics: post-type/category volumes, business engagement, followers vs
likes/engagement, engagement by category/year, grade-vs-likes, top users.

### 8.6 Dashboard
R Shiny presents KPI cards from full-data aggregates, charts, sample-explorer
filters (post type/business/category), a data-quality section (no location
field exists in the 605K data), Bloom Filter search, and a paginated sample table.

## 9. Results
Insert actual screenshots/charts generated from the project.

Do NOT invent benchmark values or row counts.

## 10. Limitations
- Historical/public dataset rather than live Instagram data.
- Results depend on the fields available in the selected dataset.
- Bloom Filter can have false positives.
- The mini project does not claim bot/fake-account detection.
- Live API/streaming is outside the mini-project scope.

## 11. Future Scope
- Authorized Instagram/Meta API integration.
- Streaming analytics with Spark Structured Streaming.
- Image feature extraction.
- Sentiment analysis.
- Advanced ML models.

## 12. Conclusion
InstaPulse demonstrates how Big Data Analytics techniques can be applied to Instagram-derived data. Apache Spark provides scalable processing, R provides analysis and visualization, and the Bloom Filter demonstrates memory-efficient membership testing. The Shiny dashboard presents the resulting insights interactively.
