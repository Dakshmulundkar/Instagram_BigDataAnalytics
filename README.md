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

## Quick Start

The primary Parquet dataset is stored with Git LFS. Install Git LFS before
cloning, then run `git lfs pull` to fetch the actual dataset. Downloading the
GitHub ZIP instead of cloning may leave you with an LFS pointer rather than the
data file. The 1,000-row CSV is a fallback/demo dataset; it is not merged with
the primary dataset.

SparkR is included in the Apache Spark distribution. The commands below install
Spark locally inside the project and set `SPARK_HOME` from that known location;
you do not need to edit installation paths or set `JAVA_HOME` or `R_HOME`.
Use Java 17 and Spark 3.5.6 for the tested setup. Keep the same terminal open
while running the pipeline and dashboard.

### Windows (PowerShell)

Install R, Java, Git, and Git LFS once. These commands use Windows Package
Manager (`winget`):

```powershell
winget install --id RProject.R --exact
winget install --id EclipseAdoptium.Temurin.17.JDK --exact
winget install --id Git.Git --exact
winget install --id GitHub.GitLFS --exact
```

Close and reopen PowerShell so the installed commands are available, then get
the project and its dataset:

```powershell
git lfs install
git clone https://github.com/Dakshmulundkar/Instagram_BigDataAnalytics.git
cd Instagram_BigDataAnalytics
git lfs pull
```

Download and unpack the Spark distribution into the project. `SPARK_HOME` is
derived from the extracted folder automatically:

```powershell
New-Item -ItemType Directory -Force .tools | Out-Null
Invoke-WebRequest -Uri https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz -OutFile .tools\spark.tgz
tar -xzf .tools\spark.tgz -C .tools
$env:SPARK_HOME = (Resolve-Path .tools\spark-3.5.6-bin-hadoop3).Path
```

Install the R packages, run the pipeline, then launch the dashboard:

```powershell
Rscript -e "install.packages(c('dplyr','ggplot2','shiny','DT'), repos='https://cloud.r-project.org')"
Rscript .\R\06_run_all.R
Rscript -e "shiny::runApp('dashboard', launch.browser=TRUE)"
```

### Ubuntu

Install R, Java 17, Git LFS, and the system libraries used to install R packages:

```bash
sudo apt update
sudo apt install -y r-base openjdk-17-jdk git git-lfs curl tar build-essential libcurl4-openssl-dev libssl-dev libxml2-dev
```

Clone the project and fetch its LFS dataset:

```bash
git lfs install
git clone https://github.com/Dakshmulundkar/Instagram_BigDataAnalytics.git
cd Instagram_BigDataAnalytics
git lfs pull
```

Download and unpack Spark locally, then set `SPARK_HOME` from the extracted
folder:

```bash
mkdir -p .tools
curl -fL https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz -o .tools/spark.tgz
tar -xzf .tools/spark.tgz -C .tools
export SPARK_HOME="$PWD/.tools/spark-3.5.6-bin-hadoop3"
```

Install the R packages, run the pipeline, then launch the dashboard:

```bash
Rscript -e "install.packages(c('dplyr','ggplot2','shiny','DT'), repos='https://cloud.r-project.org')"
Rscript R/06_run_all.R
Rscript -e "shiny::runApp('dashboard', launch.browser=TRUE)"
```

For both systems, the pipeline runs ingestion → preprocessing → Spark analytics
→ Bloom Filter → visualizations. On the primary dataset, expect
`total_posts=605868`, `unique_users=485125`, and zero Bloom false negatives.
The dashboard starts in the browser after the final command. To run it later,
open a terminal in the project folder, set `SPARK_HOME` to the local `.tools`
Spark folder as above, and run the dashboard command again.

SparkR comes from Spark; do not install it from CRAN. Windows `winutils` or
NativeIO warnings can occur during Spark writes; the pipeline has a fallback
for its small output files. No separate Hadoop installation is required.

## Viva one-line explanation

"InstaPulse is a large-scale data analytics project on ~606K Instagram records using Apache Spark/SparkR for distributed processing, Spark SQL for analytics, a Bloom Filter for memory-efficient username membership testing, R/ggplot2 for visualization, and Shiny for dashboarding."

## Scope decision

This is large-scale (~606K records) but NOT petabyte/internet-scale Big Data.
We deliberately do NOT claim:
- live Instagram monitoring
- unrestricted Instagram scraping
- complete image-archive processing
- real bot/fake-account detection

These are future/advanced extensions.
