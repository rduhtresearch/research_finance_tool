library(DBI)
library(duckdb)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(readr)
library(openxlsx)


DB_PATH <- "/Users/tategraham/Documents/NHS/research_finance_tool/data/finance_rules_AH.duckdb"

ICT_CSV_PATH <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/testing_data2.xlsx"   # <-- change this to your ICT export CSV


df <- read.xlsx(ICT_CSV_PATH)
View(df$SheetName)

df_long <- df %>%
  pivot_longer(
    cols = starts_with("VISIT"),
    names_to = "Visit",
    values_to = "is_activity"
  ) %>%
  filter(is_activity == 1) %>%
  select(Visit, Activity)

View(df_long)