# InstaPulse — BDA Mini Project

## Topic
**Instagram Post Analytics using Apache Spark and R**

This is a **subject mini project**, so the scope is intentionally smaller than the original major-project blueprint.

### Implemented core
1. Public Instagram post dataset
2. Apache Spark / SparkR ingestion and preprocessing
3. Spark aggregations
4. Username Bloom Filter
5. Engagement and temporal analytics
6. R visualizations
7. R Shiny dashboard

### Optional
Image analysis and machine learning are NOT required for the mini project. They can be mentioned as future scope.

## Dataset (primary)

`data/raw/main_instagram/main_instagram.parquet` — 605,868 records, 20 source columns,
485,125 unique usernames, date range 2012-02-07 to 2019-08-14.

Key fields: `sid`, `sid_profile`, `shortcode`, `profile_id`, `date`, `post_type`
(neutral labels "Post Type 1/2" — source gives no Image/Reel semantics),
`likes`, `comments`, `username`, `followers`, `following`, `num_posts`,
`is_business_account` (218,285 true / 387,583 false), `lang` (all `en`),
`description_category`, `description_grade`, `image_grade`.

Validation: 0 null usernames, 0 null likes/comments/followers, `sid` unique.

Fallback/demo: `data/raw/Instagram - Posts.csv` (1,000 rows, 40 columns) is kept
intact and used only when the Parquet file is unavailable. Never merged.

## Project flow

Parquet (primary) / CSV (fallback)
   ↓
SparkR ingestion (`read.df`, `instagram_posts` temp view, shuffle partitions = 4)
   ↓
Cleaning + type conversion (username normalize, IDs type-safe, explicit
timestamp parse, engagement + safe per-follower rates)
   ↓
Spark SQL analytics (overall, post-type, category, business, users, temporal,
language, quality; bounded <=5000-row viz sample)
   ↓
Username Bloom Filter (double hashing, persisted bit array + exact username set)
   ↓
Small aggregate CSVs + ggplot2 charts
   ↓
Shiny dashboard (KPIs from aggregates, bounded sample explorer, Bloom tab)

## Files

R/01_ingestion.R
R/02_preprocessing.R
R/03_analytics.R
R/04_bloom_filter.R
R/05_visualizations.R
R/06_run_all.R

dashboard/app.R

report/mini_project_report_outline.md
ppt/mini_project_ppt_outline.md

## Clone from GitHub (teammates)

Prerequisites (once per machine): R, Java 11 or 17, Spark 3.5.x or 4.x,
**Git LFS** (the 151 MB Parquet exceeds GitHub's 100 MB plain-file limit, so it
is stored with LFS — plain `git clone` without LFS gives you a 1 KB pointer
instead of the data):
```powershell
git lfs install
git clone https://github.com/Dakshmulundkar/Instagram_BigDataAnalytics.git
cd Instagram_BigDataAnalytics
$env:JAVA_HOME  = "<path-to-your-java-folder>"
$env:SPARK_HOME = "<path-to-your-spark-folder>"
$env:R_HOME     = "<path-to-your-R-folder>"
```
(Replace the `<...>` paths with your own install locations, e.g. wherever you
installed R, Java, and Spark — any drive or folder works.)
Then continue from step 4 of "How to run on Windows" below (install R packages,
run the pipeline, launch the dashboard). Generated outputs (`results/`,
`data/processed/`) are git-ignored and recreated by the pipeline.

## How to run on Windows (PowerShell)

Steps to run InstaPulse:
1. Get the project: either clone it (see above) or extract the ZIP anywhere.
2. Install R, Java (11 or 17), and Spark (3.5.x or 4.x).
3. Open PowerShell, go to the project folder, and point to your installs
(replace the `<...>` paths with your own locations — any drive works):
```powershell
cd "<path-to-the-project-folder>"
$env:JAVA_HOME  = "<path-to-your-java-folder>"
$env:SPARK_HOME = "<path-to-your-spark-folder>"
$env:R_HOME     = "<path-to-your-R-folder>"
```
4. Install R packages (first time only):
```powershell
& "$env:R_HOME\bin\Rscript.exe" -e "install.packages(c('dplyr','ggplot2','shiny','DT'), repos='https://cloud.r-project.org')"
```
SparkR comes from your Spark install — never install it from CRAN.
5. Run the full pipeline (from the project root):
```powershell
& "$env:R_HOME\bin\Rscript.exe" "R\06_run_all.R"
```
This runs ingestion → preprocessing → Spark analytics → Bloom Filter →
visualizations. Expected at the end: `total_posts=605868`,
`unique_users=485125`, Bloom false negatives 0.
6. Launch the dashboard:
```powershell
& "$env:R_HOME\bin\Rscript.exe" -e "shiny::runApp('dashboard', launch.browser=TRUE)"
```
7. The InstaPulse dashboard opens in the browser. Use the left-pane filters
(Post Type / Business Account / Description Category) — every tab updates instantly.

Or, in R/RStudio instead of PowerShell: set working directory to the project
root, then `source("R/06_run_all.R")` followed by `shiny::runApp("dashboard")`.

The dataset is already included (`data/raw/main_instagram/main_instagram.parquet`,
605,868 rows; plus the 1K fallback CSV), so no separate download is needed.

Notes: `winutils`/NativeIO warnings during Spark steps are expected and harmless
(small results use the built-in fallback automatically). Keep Java 11/17 —
Spark 3.5.x/4.x do not need a separate Hadoop install for this project.

## Viva one-line explanation

"InstaPulse is a large-scale data analytics project on ~606K Instagram records using Apache Spark/SparkR for distributed processing, Spark SQL for analytics, a Bloom Filter for memory-efficient username membership testing, R/ggplot2 for visualization, and Shiny for dashboarding."

## Scope decision

This is large-scale (~606K records) but NOT petabyte/internet-scale Big Data,
and we deliberately do NOT claim:

Because this is a subject mini project, we deliberately do NOT claim:
- live Instagram monitoring
- unrestricted Instagram scraping
- complete image-archive processing
- real bot/fake-account detection

These are future/advanced extensions.
