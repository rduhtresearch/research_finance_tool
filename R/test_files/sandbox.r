setwd("/Users/tategraham/Documents/NHS/research_finance_tool/R/utils")
source('combined_pipeline.r')

result <- process_workbook(
  input_path = "/path/to/your/study.xlsx"
)