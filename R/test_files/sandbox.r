setwd("/Users/tategraham/Documents/NHS/research_finance_tool/R/test_files")
source('pipeline_fixed.r')
source('posting_test.r')

library(DBI)
library(duckdb)

#-------------------------------------------------------------------------------
input_file  <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/embrace.xlsx"

processed_file <- process_workbook(
  input_path  = input_file,
  archive_dir = NULL,   # e.g. "/path/to/archive"
  export_path = '/Users/tategraham/Documents/NHS/embrace_processed.xlsx',   # e.g. "/path/to/output.xlsx"
  #db_dir      = dirname(input_file)
  db_dir      = '/Users/tategraham/Documents/NHS/research_finance_tool/data'
)

View(processed_file$`Unscheduled Activities`)
t <- processed_file$`Unscheduled Activities` |> filter(Activity == 'Recruitment Activities (per hour up to a maximum of £ 1,840)')
View(t)

out <- generate_posting_plan(
  ict           = processed_file,
  rules_db_path = "/Users/tategraham/Documents/NHS/research_finance_tool/data/finance_rules_AH.duckdb",
  scenario_id   = "A",
  ict_db_path   = '/Users/tategraham/Documents/NHS/research_finance_tool/data/ict_local.duckdb'
)

View(out)

# then do whatever you want with it
write_csv(out, "wherever/you/like.csv")




#-------------------------------------------------------------------------------
posting_path <- '/Users/tategraham/Documents/NHS/posting_plan.csv'
posting_plan <- read.csv(posting_path)
View(posting_plan)
View(processed_file$`Non-subset`)
View(ict_table)

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



# db_path = '/Users/tategraham/Documents/NHS/research_finance_tool/data/ict_local.duckdb'
# con <- dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
# on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
# 
# ict_cost <- dbGetQuery(con, "
#   SELECT CPMS_ID, Study_Arm, Visit_Number, Visit_Label, Activity_Name, ICT_Cost
#   FROM ict_costing_tbl
#   WHERE Activity_Name IS NOT NULL
# ")
# 
# View(ict_cost)
# View(posting_plan)



# ict_cost_deduped <- ict_cost |>
#   group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name, ICT_Cost) |>
#   slice_min(ICT_Cost, n = 1, with_ties = FALSE) |>
#   ungroup()
# 
# dbDisconnect(con, shutdown = TRUE)
# # 
# # View(ict_cost_deduped)
# 
# posting_plan$cpms_id <- as.character(posting_plan$cpms_id)
# ict_cost$Study_Arm <- trimws(ict_table$Study_Arm)
# 
# 
# posting_plan_c <- posting_plan  %>%
#   filter(Study_Arm %in% c("SC", "UA"))
# 
# View(posting_plan_c)
# 
# # Join
# posting_with_cost <- posting_plan_c |>
#   left_join(
#     ict_cost,
#     by = c(
#       "cpms_id"    = "CPMS_ID",
#       "Study_Arm"  = "Study_Arm",
#       "Visit"      = "Visit_Number",
#       "Activity"   = "Activity_Name"
#     )
#   )
# View(posting_with_cost)
# 
# ict_cost <- dbGetQuery(con, "
#   SELECT CPMS_ID, Study_Arm, Visit_Number, Visit_Label, Activity_Name, ICT_Cost
#   FROM ict_costing_tbl
#   WHERE Activity_Name IS NULL
# ")
# 
# View(ict_cost)

# mff_costs <- ict_cost |>
#   filter(is.na(Activity_Name)) |>
#   select(CPMS_ID, Study_Arm, Visit_Number, Visit_Label, ICT_Cost) |>
#   rename(visit_ict_cost = ICT_Cost)


# posting_with_cost <- posting_with_cost |>
#   left_join(
#     mff_costs,
#     by = c(
#       "cpms_id"   = "CPMS_ID",
#       "Study_Arm" = "Study_Arm",
#       "Visit"     = "Visit_Number"
#     ),
#     relationship = "many-to-one"
#   )
# 
# View(posting_with_cost)




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


# ict_cost_collapsed <- ict_cost %>%
#   group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name) %>%
#   summarise(ICT_Cost = sum(ICT_Cost, na.rm = TRUE), .groups = "drop")
# 
# View(ict_cost_collapsed)
# 
# posting_with_cost <- posting_plan_c %>%
#   left_join(
#     ict_cost_collapsed,
#     by = c(
#       "cpms_id"   = "CPMS_ID",
#       "Study_Arm" = "Study_Arm",
#       "Visit"     = "Visit_Number",
#       "Activity"  = "Activity_Name"
#     )
#   )
# 
# View(posting_with_cost)
# 
# View(posting_plan_c)
# 
# 
# 
# 
# 
# 
# 
# t_cost_indexed <- ict_cost %>%
#   group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name) %>%
#   arrange(ICT_Cost, .by_group = TRUE) %>%  # choose a stable rule
#   mutate(activity_instance = row_number()) %>%
#   ungroup()
# 
# View(t_cost_indexed)
# 
# posting_indexed <- posting_plan_c %>%
#   group_by(cpms_id, Study_Arm, Visit, Activity) %>%
#   arrange(row_id, .by_group = TRUE) %>%
#   mutate(activity_instance = row_number()) %>%
#   ungroup()
# 
# View(posting_indexed)
# 
# posting_with_cost <- posting_indexed %>%
#   left_join(
#     ict_cost_indexed,
#     by = c(
#       "cpms_id" = "CPMS_ID",
#       "Study_Arm" = "Study_Arm",
#       "Visit" = "Visit_Number",
#       "Activity" = "Activity_Name",
#       "activity_instance" = "activity_instance"
#     )
#   )











library(dplyr)


# 
# 
# 
# 
# suppressPackageStartupMessages({
#   library(dplyr)
# })
# 
# # -----------------------------
# # 1) Index ICT occurrences
# # -----------------------------
# # Use a stable ordering for occurrences within each activity key.
# # If you have a source_row / import_row column from Excel, use that instead of ICT_Cost.
# t_cost_indexed <- ict_cost %>%
#   group_by(CPMS_ID, Study_Arm, Visit_Number, Activity_Name) %>%
#   arrange(ICT_Cost, .by_group = TRUE) %>%  # or arrange(source_row, .by_group = TRUE)
#   mutate(activity_instance = row_number()) %>%
#   ungroup()
# 
# # -----------------------------
# # 2) Index POSTING occurrences at the base-row grain (row_id)
# # -----------------------------
# # First collapse explosion to one row per base activity occurrence (row_id).
# posting_occ_map <- posting_plan_c %>%
#   distinct(cpms_id, Study_Arm, Visit, Activity, row_id) %>%
#   group_by(cpms_id, Study_Arm, Visit, Activity) %>%
#   arrange(row_id, .by_group = TRUE) %>%
#   mutate(activity_instance = row_number()) %>%
#   ungroup()
# 
# # Re-attach the base occurrence index to all exploded posting lines
# posting_indexed <- posting_plan_c %>%
#   left_join(
#     posting_occ_map,
#     by = c("cpms_id", "Study_Arm", "Visit", "Activity", "row_id")
#   )
# 
# # -----------------------------
# # 3) Join posting -> ICT using activity_instance
# # -----------------------------
# posting_with_cost <- posting_indexed %>%
#   left_join(
#     t_cost_indexed,
#     by = c(
#       "cpms_id"           = "CPMS_ID",
#       "Study_Arm"         = "Study_Arm",
#       "Visit"             = "Visit_Number",
#       "Activity"          = "Activity_Name",
#       "activity_instance" = "activity_instance"
#     )
#   )
# View(posting_with_cost)
# # -----------------------------
# # 4) Sanity checks
# # -----------------------------
# # A) Ensure activity_instance is constant within each row_id (it should be)
# bad_rowid_instances <- posting_with_cost %>%
#   group_by(row_id) %>%
#   summarise(n_instances = n_distinct(activity_instance), .groups = "drop") %>%
#   filter(n_instances > 1)
# 
# # B) Ensure row count did not inflate
# row_count_before <- nrow(posting_plan_c)
# row_count_after  <- nrow(posting_with_cost)
# 
# # C) Identify posting rows that still didn't match ICT (ICT_Cost is NA)
# unmatched_posting <- posting_with_cost %>%
#   filter(is.na(ICT_Cost))
# 
# # D) Identify ICT rows that never matched any posting row (optional)
# ict_unmatched <- anti_join(
#   t_cost_indexed,
#   posting_with_cost %>%
#     distinct(
#       CPMS_ID = cpms_id,
#       Study_Arm,
#       Visit_Number = Visit,
#       Activity_Name = Activity,
#       activity_instance
#     ),
#   by = c("CPMS_ID", "Study_Arm", "Visit_Number", "Activity_Name", "activity_instance")
# )






# -----------------------------
# Join posting plan to ICT cost table
# -----------------------------

UA_ARMS <- c("UA", "SC", "SSP")

# Trim whitespace on join keys (both sides)
posting_plan <- posting_plan %>% mutate(Study_Arm = trimws(Study_Arm))
ict_table    <- ict_table    %>% mutate(Study_Arm = trimws(Study_Arm))

# MFF join — scheduled arms only, join on visit position
# ICT rows where Activity_Name IS NULL are MFF summary rows
result_mff <- posting_plan %>%
  filter(!Study_Arm %in% UA_ARMS) %>%
  left_join(
    ict_table %>% filter(is.na(Activity_Name)),
    by = c("cpms_id" = "CPMS_ID", "Study_Arm", "Visit" = "Visit_Number"),
    relationship = "many-to-one"
  )

# Activity join — UA/SC/SSP arms only, join on activity name + occurrence
# ICT rows where Activity_Name IS NOT NULL are per-activity rows
result_activity <- posting_plan %>%
  filter(Study_Arm %in% UA_ARMS) %>%
  left_join(
    ict_table %>% filter(!is.na(Activity_Name)),
    by = c("cpms_id" = "CPMS_ID", "Study_Arm",
           "Activity" = "Activity_Name",
           "activity_occurrence_id"),
    relationship = "many-to-one"
  )

# Combine and validate
result <- bind_rows(result_mff, result_activity)

# Check for unmatched rows
unmatched <- result %>% filter(is.na(ICT_Cost))
if (nrow(unmatched) > 0) {
  warning(nrow(unmatched), " rows did not match the ICT cost table.")
  print(unmatched %>% distinct(Study_Arm, Activity, Visit, activity_occurrence_id))
}






# Run these diagnostics before the join

UA_ARMS <- c("UA", "SC", "SSP")

# Check for duplicates in the MFF side of ict_table
ict_table %>%
  filter(is.na(activity_occurrence_id)) %>%
  group_by(CPMS_ID, Study_Arm, Visit_Number) %>%
  filter(n() > 1) %>%
  arrange(CPMS_ID, Study_Arm, Visit_Number)

# Check for duplicates in the activity side of ict_table
ict_table %>%
  filter(!is.na(activity_occurrence_id)) %>%
  group_by(CPMS_ID, Study_Arm, Activity_Name, activity_occurrence_id) %>%
  filter(n() > 1) %>%
  arrange(CPMS_ID, Study_Arm, Activity_Name, activity_occurrence_id)

# Check which row 296 of posting_plan is
posting_plan %>% slice(296) %>% glimpse()




ict_table %>%
  filter(is.na(activity_occurrence_id)) %>%
  group_by(CPMS_ID, Study_Arm, Visit_Number) %>%
  filter(n() > 1) %>%
  arrange(CPMS_ID, Study_Arm, Visit_Number)



# What does ict_table have for this exact combination?
ict_table %>%
  filter(CPMS_ID == "47452",
         Study_Arm == "Non-subset",
         Visit_Number == "VISIT - 004")

# And how many rows total match the join keys?
ict_table %>%
  filter(is.na(activity_occurrence_id),
         CPMS_ID == "47452",
         Study_Arm == "Non-subset",
         Visit_Number == "VISIT - 004")





# How many posting_plan rows match the same keys as row 296?
posting_plan %>%
  filter(!Study_Arm %in% UA_ARMS,
         cpms_id == "47452",
         Study_Arm == "Non-subset",
         Visit == "VISIT - 004") %>%
  select(row_id, Activity, Visit, Study_Arm, posting_line_type_id, activity_occurrence_id)






# Both joins now use activity-level ICT rows
# MFF summary rows (activity_occurrence_id IS NULL) are visit-level totals — 
# not used for per-activity posting lines

result <- posting_plan %>%
  left_join(
    ict_table %>% filter(!is.na(activity_occurrence_id)),
    by = c(
      "cpms_id"                = "CPMS_ID",
      "Study_Arm",
      "Activity"               = "Activity_Name",
      "activity_occurrence_id"
    )
  )

View(result)






UA_ARMS <- c("UA", "SC", "SSP")

posting_plan <- posting_plan %>% mutate(Study_Arm = trimws(Study_Arm))
ict_table    <- ict_table    %>% mutate(Study_Arm = trimws(Study_Arm))

# Scheduled arms — MFF visit-level join
result_mff <- posting_plan %>%
  filter(!Study_Arm %in% UA_ARMS) %>%
  left_join(
    ict_table %>%
      filter(is.na(activity_occurrence_id)) %>%
      select(CPMS_ID, Study_Arm, Visit_Number, ICT_Cost, Visit_Label),
    by = c("cpms_id" = "CPMS_ID", "Study_Arm", "Visit" = "Visit_Number")
  )

# UA/SC/SSP arms — per-activity join
result_activity <- posting_plan %>%
  filter(Study_Arm %in% UA_ARMS) %>%
  left_join(
    ict_table %>%
      filter(!is.na(activity_occurrence_id)) %>%
      select(CPMS_ID, Study_Arm, Activity_Name, activity_occurrence_id, ICT_Cost, Visit_Label),
    by = c("cpms_id" = "CPMS_ID", "Study_Arm",
           "Activity" = "Activity_Name",
           "activity_occurrence_id")
  )

result <- bind_rows(result_mff, result_activity) %>%
  mutate(Visit_Label = coalesce(Visit_Label.x, Visit_Label.y)) %>%
  select(-any_of(c("Visit_Label.x", "Visit_Label.y")))

# Validation — UA/SC/SSP rows should always match
unmatched_ua <- result %>% filter(Study_Arm %in% UA_ARMS, is.na(ICT_Cost))
if (nrow(unmatched_ua) > 0) {
  warning(nrow(unmatched_ua), " UA/SC/SSP rows did not match the ICT cost table.")
  print(unmatched_ua %>% distinct(Study_Arm, Activity, Visit, activity_occurrence_id))
}