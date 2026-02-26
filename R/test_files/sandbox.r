setwd("/Users/tategraham/Documents/NHS/research_finance_tool/R/utils")
source('pipeline_combined.r')

input_file  <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/test_study.xlsx"

processed_file <- process_workbook(
  input_path  = input_file,
  archive_dir = NULL,   # e.g. "/path/to/archive"
  export_path = NULL,   # e.g. "/path/to/output.xlsx"
  #db_dir      = dirname(input_file)
  db_dir      = '/Users/tategraham/Documents/NHS/research_finance_tool/data'
)
