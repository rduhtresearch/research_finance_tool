# -----------------------------
# 02_generate_posting_plan_AH.R
# Generate long posting plan from ICT + DuckDB rules (A–H)
# -----------------------------

library(DBI)
library(duckdb)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(readr)
library(openxlsx)

# -----------------------------
# 1) User settings
# -----------------------------

DB_PATH <- "/Users/tategraham/Documents/NHS/research_finance_tool/data/finance_rules_AH.duckdb"

#ICT_CSV_PATH <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/test.xlsx"   # <-- change this to your ICT export CSV
ICT_CSV_PATH <-'/Users/tategraham/Documents/NHS/file1788327344760.xlsx'

ruleset_id <- "COMM_AH_V1"

# Scenario selection (A..H)
scenario_id <- "A"

# MFF rate (fixed for MVP)
mff_rate <- 1.08

# -----------------------------
# 2) Read ICT
# -----------------------------
df <- read.xlsx(ICT_CSV_PATH)
View(df)

# Ensure stable row_id
if (!"row_id" %in% names(df)) df$row_id <- seq_len(nrow(df))

# -----------------------------
# 3) Required columns check
# -----------------------------
required <- c("Activity.Type", "Staff.Role", "Activity.Cost")
missing <- setdiff(required, names(df))
if (length(missing) > 0) stop(paste("Missing required columns:", paste(missing, collapse = ", ")))

# provider_org & pi_org are required for routing context (you can add later if not present)
# For MVP: if not present, create placeholders so pipeline runs.
if (!"provider_org" %in% names(df)) df$provider_org <- NA_character_
if (!"pi_org" %in% names(df)) df$pi_org <- NA_character_

# -----------------------------
# 4) Normalize + derive row context
# -----------------------------

# Optional manual override column (populated later by UI)
if (!"calc_tag" %in% names(df)) df$calc_tag <- NA_character_

df <- df %>%
  mutate(
    activity_type_norm = str_to_lower(str_trim(.data$`Activity.Type`)),
    staff_role_norm    = str_to_lower(str_trim(.data$`Staff.Role`)),
    
    # Auto classification (what the engine would do without overrides)
    row_category_auto = if_else(
      str_detect(activity_type_norm, "^investigation$|investigation"),
      "INVESTIGATION",
      "BASELINE"
    ),
    
    # Normalise calc_tag (blank -> NA)
    calc_tag = if_else(is.na(calc_tag), NA_character_, str_trim(as.character(calc_tag))),
    calc_tag = if_else(calc_tag == "", NA_character_, calc_tag),
    
    # Effective category used by rules engine
    row_category = if_else(!is.na(calc_tag), calc_tag, row_category_auto),
    
    # Medic detection: Staff Role equals "Medical Staff"
    is_medic = (str_trim(.data$`Staff.Role`) == "Medical Staff"),
    
    scenario_id = scenario_id,
    ruleset_id = ruleset_id
  )

test_id <- df |> filter(row_category_auto == "BASELINE") |> slice(1) |> pull(row_id)

df <- df %>%
  mutate(
    calc_tag = if_else(row_id == test_id, "TRAINING_FEE", calc_tag),
    row_category = if_else(!is.na(calc_tag), calc_tag, row_category_auto)
  )

# -----------------------------
# 5) Connect to DB and pull rules + mapping tables
# -----------------------------
con <- dbConnect(duckdb::duckdb(), dbdir = DB_PATH, read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

dist_rules <- dbGetQuery(con, "
  SELECT scenario_id, row_category, condition_field, condition_op, condition_value,
         posting_line_type_id, priority
  FROM dist_rules
  WHERE ruleset_id = ?
", params = list(ruleset_id))

amount_map <- dbGetQuery(con, "
  SELECT posting_line_type_id, base_mult, split_mult, applies_to_row_category
  FROM amount_map
")

routing_rules <- dbGetQuery(con, "
  SELECT scenario_id, condition_field, condition_op, condition_value,
         posting_line_type_id, destination_bucket, priority
  FROM routing_rules
  WHERE ruleset_id = ?
", params = list(ruleset_id))

# -----------------------------
# 6) Rule evaluator: conditions (only supports is_medic = TRUE/FALSE right now)
# -----------------------------
condition_passes <- function(condition_field, condition_op, condition_value, is_medic) {
  
  # No condition means "always applies"
  if (is.na(condition_field) || condition_field == "") return(TRUE)
  
  # Only support is_medic = TRUE/FALSE for MVP
  if (condition_field == "is_medic" && condition_op == "=" && condition_value %in% c("TRUE", "FALSE")) {
    return(is_medic == (condition_value == "TRUE"))
  }
  
  # Unknown condition => reject
  FALSE
}

# -----------------------------
# 7) Build posting lines per base row (dist_rules)
# -----------------------------
posting_plan <- df %>%
  select(Visit, Activity, cpms_id, study_name, row_id, scenario_id, row_category_auto, calc_tag, 
         row_category, is_medic, provider_org, pi_org, `Activity.Cost`) %>%
  mutate(
    # Ensure numeric Activity Cost (strip currency symbols/commas)
    activity_cost_num = as.numeric(gsub("[^0-9.]", "", .data$`Activity.Cost`))
  ) %>%
  rowwise() %>%
  mutate(
    posting_lines = list({
      # candidate rules for this row
      cand <- dist_rules %>%
        filter(.data$scenario_id == .env$scenario_id, .data$row_category == .env$row_category) %>%
        mutate(
          ok = purrr::pmap_lgl(
            list(condition_field, condition_op, condition_value),
            ~ condition_passes(..1, ..2, ..3, is_medic)
          )
        ) %>%
        filter(ok) %>%
        arrange(priority)
      
      unique(cand$posting_line_type_id)
    })
  ) %>%
  ungroup() %>%
  select(Visit, Activity, cpms_id, study_name, row_id, scenario_id, row_category_auto, calc_tag, 
         row_category, is_medic, provider_org, pi_org, activity_cost_num, posting_lines) %>%
  unnest(posting_lines) %>%
  rename(posting_line_type_id = posting_lines)

# -----------------------------
# 8) Attach amount parameters and calculate amounts
# amount = AC * mff_rate * base_mult * split_mult
# -----------------------------
posting_plan <- posting_plan %>%
  left_join(amount_map, by = "posting_line_type_id") %>%
  mutate(
    # Validate mapping exists
    missing_amount_map = is.na(base_mult) | is.na(split_mult),
    
    # Calculate amount
    posting_amount = activity_cost_num * mff_rate * base_mult * split_mult
  )

# Hard check: missing amount mappings are not allowed
if (any(posting_plan$missing_amount_map)) {
  bad <- posting_plan %>% filter(missing_amount_map) %>% distinct(posting_line_type_id)
  stop(paste("Missing amount_map for posting_line_type_id(s):", paste(bad$posting_line_type_id, collapse = ", ")))
}

# -----------------------------
# 9) Attach destination bucket (routing)
# -----------------------------
# Routing can be conditional too (we only support is_medic = ... like above)
resolve_destination <- function(scenario_id, posting_line_type_id, is_medic) {
  cand <- routing_rules %>%
    filter(.data$scenario_id == scenario_id, .data$posting_line_type_id == posting_line_type_id) %>%
    mutate(
      ok = purrr::pmap_lgl(
        list(condition_field, condition_op, condition_value),
        ~ condition_passes(..1, ..2, ..3, is_medic)
      )
    ) %>%
    filter(ok) %>%
    arrange(priority)
  
  if (nrow(cand) == 0) return(NA_character_)
  cand$destination_bucket[[1]]
}

posting_plan <- posting_plan %>%
  rowwise() %>%
  mutate(destination_bucket = resolve_destination(scenario_id, posting_line_type_id, is_medic)) %>%
  ungroup()


if (any(is.na(posting_plan$destination_bucket))) {
  bad <- posting_plan %>% filter(is.na(destination_bucket)) %>% distinct(scenario_id, posting_line_type_id)
  stop(paste("Missing routing for some posting lines. Example:", paste0(bad$scenario_id[1], " / ", bad$posting_line_type_id[1])))
}

# -----------------------------
# 10) Resolve destination "entity" (provider vs PI vs fixed buckets)
# This is NOT final cost centre code; it’s the logical destination.
# -----------------------------
posting_plan <- posting_plan %>%
  mutate(
    destination_entity = case_when(
      destination_bucket == "DEST_PROVIDER" ~ provider_org,
      destination_bucket == "DEST_PI_ORG" ~ pi_org,
      destination_bucket == "DEST_RD" ~ "R&D",
      destination_bucket == "DEST_TRUST_OH" ~ "TRUST_OVERHEAD",
      destination_bucket == "DEST_SUPPORT" ~ "SUPPORT_BUCKET",
      TRUE ~ "UNKNOWN"
    ),
    
    # cost_code is manual for MVP
    cost_code = NA_character_
  )

# -----------------------------
# 11) Output: long posting plan
# -----------------------------
out <- posting_plan %>%
  select(
    row_id, scenario_id, row_category_auto, calc_tag, row_category, is_medic,
    cpms_id, study_name, Activity, Visit, posting_line_type_id, posting_amount,
    destination_bucket, destination_entity,
    cost_code
  ) %>%
  arrange(row_id, posting_line_type_id)

View(out)

write_csv(out, "/Users/tategraham/Documents/NHS/posting_plan.csv")

cat("\n✅ Wrote posting_plan.csv\n")
cat("Rows:", nrow(out), "\n")
cat("Scenario:", scenario_id, "\n")
