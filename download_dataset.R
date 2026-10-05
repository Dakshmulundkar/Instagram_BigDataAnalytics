# download_dataset.R
# Run once from the InstaPulse project root.
# Downloads the public sample CSV into data/raw.

url <- "https://raw.githubusercontent.com/luminati-io/Instagram-Posts-dataset-samples/main/Instagram%20-%20Posts.csv"
dest <- file.path("data","raw","Instagram - Posts.csv")
dir.create(dirname(dest), recursive=TRUE, showWarnings=FALSE)

download.file(url, dest, mode="wb", quiet=FALSE)
cat("Downloaded dataset to:", normalizePath(dest), "\n")
