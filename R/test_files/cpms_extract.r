library(openxlsx)
library(dplyr)

input_file <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/candy study.xlsx"

raw <- read.xlsx(input_file, sheet = 1, colNames = FALSE)
View(raw)

# Simple test extraction
study_row <- raw %>% filter(trimws(as.character(X1)) == "Study")
cpms_row  <- raw %>% filter(trimws(as.character(X1)) == "Study Id")

study_value <- if (nrow(study_row) > 0) trimws(as.character(study_row$X2[1])) else NA
cpms_id     <- if (nrow(cpms_row)  > 0) trimws(as.character(cpms_row$X2[1]))  else NA

cat("Study Name:", study_value, "\n")
cat("CPMS ID:", cpms_id, "\n")