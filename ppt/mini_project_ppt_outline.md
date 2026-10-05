# InstaPulse — Mini Project PPT

## Slide 1 — Title
InstaPulse: Instagram Post Analytics using Apache Spark and R
Names / Roll No. / Subject: Big Data Analytics

## Slide 2 — Introduction
- Instagram generates large volumes of heterogeneous data.
- Post data includes users, likes, comments, dates, locations and content types.
- Big Data tools can process and analyze this information efficiently.

## Slide 3 — Problem Statement
Traditional analysis becomes difficult as social-media data grows.
We need a system that cleans, processes and analyzes Instagram post data and presents useful insights.

## Slide 4 — Objectives
- Spark-based processing
- Data preprocessing
- Engagement analytics
- User/location/content analysis
- Bloom Filter
- R visualization
- Shiny dashboard

## Slide 5 — Dataset
Primary: main_instagram.parquet — 605,868 records, 20 columns, 485,125 users,
2012-02-07 to 2019-08-14. Business 218,285 / non-business 387,583.
Avg likes 205.84, comments 5.55, followers 7300.31. 1K CSV kept as fallback.

## Slide 6 — Technologies
R, Apache Spark/SparkR, R Shiny, ggplot2, dplyr, Bloom Filter.

## Slide 7 — Architecture
Dataset → Spark → Cleaning → Analytics → Bloom Filter → R → Shiny Dashboard

## Slide 8 — Data Processing
SparkR ingestion (Parquet, type-safe IDs) → username normalization → explicit
timestamp parse → engagement + safe per-follower rates → sid uniqueness verified.

## Slide 9 — Analytics
Show actual charts (full-data aggregates + bounded log-scale samples):
- posts by post type (Post Type 1/2 — neutral labels, no invented semantics)
- engagement by description category
- business vs non-business engagement
- followers vs likes (log-log sample)

## Slide 10 — Bloom Filter
Explain (~485K usernames, m = 4,649,952, k = 7, empirical FPR ~0.008, 0 false negatives):
- bit array
- multiple hash functions (double hashing)
- possibly present
- definitely absent
- false positives + exact ground-truth check in dashboard

## Slide 11 — Dashboard
Insert screenshot of the Shiny dashboard.

## Slide 12 — Results
State actual findings from the charts.

## Slide 13 — Limitations
Historical dataset, schema dependency, false positives, no live API.

## Slide 14 — Future Scope
Authorized API, streaming, image analytics, sentiment analysis, advanced ML.

## Slide 15 — Conclusion
The project demonstrates a practical BDA pipeline using Spark and R.

## Slide 16 — Thank You
Questions?
