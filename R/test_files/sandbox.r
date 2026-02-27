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

View(processed_file$`Unscheduled Activities`)
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
View(posting_plan)

# ict_cost_deduped <- ict_cost |>
#   group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name, ICT_Cost) |>
#   slice_min(ICT_Cost, n = 1, with_ties = FALSE) |>
#   ungroup()

dbDisconnect(con, shutdown = TRUE)
# 
# View(ict_cost_deduped)

posting_plan$cpms_id <- as.character(posting_plan$cpms_id)
ict_cost$Study_Arm <- trimws(ict_table$Study_Arm)


posting_plan_c <- posting_plan  %>%
  filter(Study_Arm %in% c("SC", "UA"))

View(posting_plan_c)

# Join
posting_with_cost <- posting_plan_c |>
  left_join(
    ict_cost,
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

View(ict_cost)

# mff_costs <- ict_cost |>
#   filter(is.na(Activity_Name)) |>
#   select(CPMS_ID, Study_Arm, Visit_Number, Visit_Label, ICT_Cost) |>
#   rename(visit_ict_cost = ICT_Cost)


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




# # -----------------------------------------------------------------------------
# # test_pipeline.r
# # End-to-end test: process workbook → posting plan → join ICT costs
# # -----------------------------------------------------------------------------
# 
# library(DBI)
# library(duckdb)
# library(dplyr)
# library(readr)
# library(openxlsx)
# 
# source('/Users/tategraham/Documents/NHS/research_finance_tool/R/test_files/pipeline_fixed.r')
# 
# # -----------------------------------------------------------------------------
# # 1) Paths
# # -----------------------------------------------------------------------------
# 
# INPUT_FILE   <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/embrace.xlsx"
# POSTING_PATH <- "/Users/tategraham/Documents/NHS/posting_plan.csv"
# ICT_DB_PATH  <- "/Users/tategraham/Documents/NHS/research_finance_tool/data/ict_local.duckdb"
# RULES_DB_PATH <- "/Users/tategraham/Documents/NHS/research_finance_tool/data/finance_rules_AH.duckdb"
# 
# # -----------------------------------------------------------------------------
# # 2) Run pipeline — builds ict_local.duckdb and returns long-format sheets
# # -----------------------------------------------------------------------------
# 
# processed_file <- process_workbook(
#   input_path = INPUT_FILE,
#   db_dir     = dirname(ICT_DB_PATH)
# )
# 
# # -----------------------------------------------------------------------------
# # 3) Load posting plan (generated separately by posting_test.r / 02_generate)
# # -----------------------------------------------------------------------------
# 
# posting_plan <- read_csv(POSTING_PATH, show_col_types = FALSE) |>
#   mutate(cpms_id = as.character(cpms_id))
# 
# # -----------------------------------------------------------------------------
# # 4) Load ICT costs from DuckDB — single connection, all queries up front
# # -----------------------------------------------------------------------------
# 
# con <- dbConnect(duckdb::duckdb(), dbdir = ICT_DB_PATH, read_only = TRUE)
# 
# ict_activity <- dbGetQuery(con, "
#   SELECT CPMS_ID, Study_Arm, Visit_Number, Visit_Label, Activity_Name, ICT_Cost
#   FROM ict_costing_tbl
#   WHERE Activity_Name IS NOT NULL
# ")
# 
# ict_visit <- dbGetQuery(con, "
#   SELECT CPMS_ID, Study_Arm, Visit_Number, Visit_Label, ICT_Cost AS visit_ict_cost
#   FROM ict_costing_tbl
#   WHERE Activity_Name IS NULL
# ")
# 
# dbDisconnect(con, shutdown = TRUE)
# rm(con)   # prevent accidental reuse after disconnect
# 
# # -----------------------------------------------------------------------------
# # 5) Deduplicate activity-level costs
# # Duplicates arise from UA rows being picked up by both the dedicated UA sheet
# # and flagged rows on main arm sheets. Taking slice_min is a safe guard until
# # the double-ingestion is fixed at source in run_stage_a.
# # -----------------------------------------------------------------------------
# 
# ict_activity_deduped <- ict_activity |>
#   mutate(Study_Arm = trimws(Study_Arm)) |>
#   group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name) |>
#   slice_min(ICT_Cost, n = 1, with_ties = FALSE) |>
#   ungroup()
# 
# # -----------------------------------------------------------------------------
# # 6) Join 1: UA / SC / SSP — join on (cpms_id, Study_Arm, Visit, Activity)
# # -----------------------------------------------------------------------------
# 
# UA_ARMS <- c("UA", "SC", "SSP")
# 
# posting_ua_costed <- posting_plan |>
#   filter(Study_Arm %in% UA_ARMS) |>
#   left_join(
#     ict_activity_deduped,
#     by = c(
#       "cpms_id"   = "CPMS_ID",
#       "Study_Arm" = "Study_Arm",
#       "Visit"     = "Visit_Number",
#       "Activity"  = "Activity_Name"
#     ),
#     relationship = "many-to-one"
#   )
# 
# # -----------------------------------------------------------------------------
# # 7) Join 2: Main arm — join on (cpms_id, Study_Arm, Visit) only
# # Main arm ICT costs are at visit grain (MFF totals), not activity grain.
# # -----------------------------------------------------------------------------
# 
# posting_main_costed <- posting_plan |>
#   filter(!Study_Arm %in% UA_ARMS) |>
#   left_join(
#     ict_visit,
#     by = c(
#       "cpms_id"   = "CPMS_ID",
#       "Study_Arm" = "Study_Arm",
#       "Visit"     = "Visit_Number"
#     ),
#     relationship = "many-to-one"
#   )
# 
# # -----------------------------------------------------------------------------
# # 8) Union both halves
# # -----------------------------------------------------------------------------
# 
# posting_with_cost <- bind_rows(posting_ua_costed, posting_main_costed)
# 
# # -----------------------------------------------------------------------------
# # 9) Reconciliation guards
# # -----------------------------------------------------------------------------
# 
# stopifnot(
#   "Row count changed after join — fan-out or row loss detected" =
#     nrow(posting_with_cost) == nrow(posting_plan)
# )
# 
# stopifnot(
#   "NA costs detected — some posting lines could not be matched to ICT" =
#     !any(is.na(posting_with_cost$visit_ict_cost) & !posting_plan$Study_Arm %in% UA_ARMS) &&
#     !any(is.na(posting_with_cost$ICT_Cost)       &  posting_plan$Study_Arm %in% UA_ARMS)
# )
# 
# # -----------------------------------------------------------------------------
# # 10) Inspect
# # -----------------------------------------------------------------------------
# 
# View(posting_with_cost)
# 
# cat("\n✅ Join complete\n")
# cat("Posting lines:     ", nrow(posting_with_cost), "\n")
# cat("UA/SC/SSP lines:   ", nrow(posting_ua_costed), "\n")
# cat("Main arm lines:    ", nrow(posting_main_costed), "\n")
# cat("NA activity costs: ", sum(is.na(posting_with_cost$ICT_Cost)), "\n")
# cat("NA visit costs:    ", sum(is.na(posting_with_cost$visit_ict_cost)), "\n")


ict_cost_collapsed <- ict_cost %>%
  group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name) %>%
  summarise(ICT_Cost = sum(ICT_Cost, na.rm = TRUE), .groups = "drop")

View(ict_cost_collapsed)

posting_with_cost <- posting_plan_c %>%
  left_join(
    ict_cost_collapsed,
    by = c(
      "cpms_id"   = "CPMS_ID",
      "Study_Arm" = "Study_Arm",
      "Visit"     = "Visit_Number",
      "Activity"  = "Activity_Name"
    )
  )

View(posting_with_cost)

View(posting_plan_c)







t_cost_indexed <- ict_cost %>%
  group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name) %>%
  arrange(ICT_Cost, .by_group = TRUE) %>%  # choose a stable rule
  mutate(activity_instance = row_number()) %>%
  ungroup()

View(t_cost_indexed)

posting_indexed <- posting_plan_c %>%
  group_by(cpms_id, Study_Arm, Visit, Activity) %>%
  arrange(row_id, .by_group = TRUE) %>%
  mutate(activity_instance = row_number()) %>%
  ungroup()

View(posting_indexed)

posting_with_cost <- posting_indexed %>%
  left_join(
    ict_cost_indexed,
    by = c(
      "cpms_id" = "CPMS_ID",
      "Study_Arm" = "Study_Arm",
      "Visit" = "Visit_Number",
      "Activity" = "Activity_Name",
      "activity_instance" = "activity_instance"
    )
  )


















suppressPackageStartupMessages({
  library(dplyr)
})

# -----------------------------
# 1) Index ICT occurrences
# -----------------------------
# Use a stable ordering for occurrences within each activity key.
# If you have a source_row / import_row column from Excel, use that instead of ICT_Cost.
t_cost_indexed <- ict_cost %>%
  group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name) %>%
  arrange(ICT_Cost, .by_group = TRUE) %>%  # or arrange(source_row, .by_group = TRUE)
  mutate(activity_instance = row_number()) %>%
  ungroup()

# -----------------------------
# 2) Index POSTING occurrences at the base-row grain (row_id)
# -----------------------------
# First collapse explosion to one row per base activity occurrence (row_id).
posting_occ_map <- posting_plan_c %>%
  distinct(cpms_id, Study_Arm, Visit, Activity, row_id) %>%
  group_by(cpms_id, Study_Arm, Visit, Activity) %>%
  arrange(row_id, .by_group = TRUE) %>%
  mutate(activity_instance = row_number()) %>%
  ungroup()

# Re-attach the base occurrence index to all exploded posting lines
posting_indexed <- posting_plan_c %>%
  left_join(
    posting_occ_map,
    by = c("cpms_id", "Study_Arm", "Visit", "Activity", "row_id")
  )

# -----------------------------
# 3) Join posting -> ICT using activity_instance
# -----------------------------
posting_with_cost <- posting_indexed %>%
  left_join(
    t_cost_indexed,
    by = c(
      "cpms_id"           = "CPMS_ID",
      "Study_Arm"         = "Study_Arm",
      "Visit"             = "Visit_Number",
      "Activity"          = "Activity_Name",
      "activity_instance" = "activity_instance"
    )
  )
View(posting_with_cost)
# -----------------------------
# 4) Sanity checks
# -----------------------------
# A) Ensure activity_instance is constant within each row_id (it should be)
bad_rowid_instances <- posting_with_cost %>%
  group_by(row_id) %>%
  summarise(n_instances = n_distinct(activity_instance), .groups = "drop") %>%
  filter(n_instances > 1)

# B) Ensure row count did not inflate
row_count_before <- nrow(posting_plan_c)
row_count_after  <- nrow(posting_with_cost)

# C) Identify posting rows that still didn't match ICT (ICT_Cost is NA)
unmatched_posting <- posting_with_cost %>%
  filter(is.na(ICT_Cost))

# D) Identify ICT rows that never matched any posting row (optional)
ict_unmatched <- anti_join(
  t_cost_indexed,
  posting_with_cost %>%
    distinct(
      CPMS_ID = cpms_id,
      Study_Arm,
      Visit_Number = Visit,
      Activity_Name = Activity,
      activity_instance
    ),
  by = c("CPMS_ID", "Study_Arm", "Visit_Number", "Activity_Name", "activity_instance")
)

