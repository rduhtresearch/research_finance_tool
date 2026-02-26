setwd("/Users/tategraham/Documents/NHS/research_finance_tool/R/test_files")
source('pipeline_fixed.r')

library(DBI)
library(duckdb)

#-------------------------------------------------------------------------------
input_file  <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/embrace.xlsx"

processed_file <- process_workbook(
  input_path  = input_file,
  archive_dir = NULL,   # e.g. "/path/to/archive"
  export_path = '/Users/tategraham/Documents/NHS',   # e.g. "/path/to/output.xlsx"
  #db_dir      = dirname(input_file)
  db_dir      = '/Users/tategraham/Documents/NHS/research_finance_tool/data'
)

View(processed_file$`Non-subset`)
#-------------------------------------------------------------------------------
posting_path <- '/Users/tategraham/Documents/NHS/posting_plan.csv'
posting_plan <- read.csv(posting_path)
View(posting_plan)
#-------------------------------------------------------------------------------
db_path = '/Users/tategraham/Documents/NHS/research_finance_tool/data/ict_local.duckdb'
con <- dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

ict_table <- dbGetQuery(con, "select * from ict_costing_tbl;")
View(ict_table)
#-------------------------------------------------------------------------------

posting_plan$cpms_id <- as.character(posting_plan$cpms_id)
ict_table$Study_Arm <- trimws(ict_table$Study_Arm)

names(posting_plan)[names(posting_plan) == 'Study_Arm']

# result <- posting_plan %>%
#   left_join(
#     ict_table,
#     by = c(
#       "Visit"      = "Visit_Number",
#       "Activity"   = "Visit_Name",
#       "Study_Arm"  = "Study_Arm",
#       "cpms_id"    = "CPMS_ID"
#     )
#   )



db_path = '/Users/tategraham/Documents/NHS/research_finance_tool/data/ict_local.duckdb'
con <- dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

ict_cost <- dbGetQuery(con, "
  SELECT CPMS_ID, Study_Arm, Visit_Number, Visit_Label, Activity_Name, ICT_Cost
  FROM ict_costing_tbl
  WHERE Activity_Name IS NOT NULL
")

View(ict_cost)

ict_cost_deduped <- ict_cost |>
  group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name) |>
  slice_min(ICT_Cost, n = 1, with_ties = FALSE) |>
  ungroup()

dbDisconnect(con, shutdown = TRUE)

# Join
posting_with_cost <- posting_plan |>
  left_join(
    ict_cost_deduped,
    by = c(
      "cpms_id"    = "CPMS_ID",
      "Study_Arm"  = "Study_Arm",
      "Visit"      = "Visit_Number",
      "Activity"   = "Activity_Name"
    )
  )
View(posting_with_cost)

ict_cost <- dbGetQuery(con, "
  SELECT CPMS_ID, Study_Arm, Visit_Number, Visit_Label, Activity_Name, ICT_Cost
  FROM ict_costing_tbl
  WHERE Activity_Name IS NULL
")

mff_costs <- ict_cost |>
  filter(is.na(Activity_Name)) |>
  select(CPMS_ID, Study_Arm, Visit_Number, Visit_Label, ICT_Cost) |>
  rename(visit_ict_cost = ICT_Cost)


posting_with_cost <- posting_with_cost |>
  left_join(
    mff_costs,
    by = c(
      "cpms_id"   = "CPMS_ID",
      "Study_Arm" = "Study_Arm",
      "Visit"     = "Visit_Number"
    ),
    relationship = "many-to-one"
  )

View(posting_with_cost)



