#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(openxlsx)
  library(dplyr)
})

require_pkg <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop("Missing package: ", pkg, ". Install it with install.packages('", pkg, "').")
  }
}

# --- Column helpers (names vary; use position) ---

get_visit_cols <- function(df) {
  if (!("Activity.Cost" %in% names(df))) stop("Missing column: Activity.Cost")
  if (!("Total.Activity.Cost" %in% names(df))) stop("Missing column: Total.Activity.Cost")
  
  start_col <- which(names(df) == "Activity.Cost") + 1
  end_col   <- which(names(df) == "Total.Activity.Cost") - 1
  if (start_col > end_col) stop("No visit/occurrence columns found between Activity.Cost and Total.Activity.Cost")
  names(df)[start_col:end_col]
}

coerce_visit_cols_numeric_legacy <- function(df) {
  visit_cols <- get_visit_cols(df)
  sub <- df[, visit_cols, drop = FALSE]
  sub[is.na(sub)] <- 0
  sub[sub == "N/A"] <- 0
  sub <- data.frame(lapply(sub, function(x) as.numeric(as.character(x))))
  df[, visit_cols] <- sub
  df
}

# Helper fun to ensure column VISIT - 00.. naming format
rename_visit_cols <- function(df) {
  visit_cols <- get_visit_cols(df)
  new_names  <- paste("VISIT -", sprintf("%03d", seq_along(visit_cols)))
  names(df)[match(visit_cols, names(df))] <- new_names
  df
}

# --- Workbook extraction ---

find_section_starts <- function(raw_df, headers = c("Activity", "Activity Type", "Department")) {
  which(apply(raw_df, 1, function(row) all(headers %in% row)))
}

extract_sections <- function(raw_df, starts) {
  sections <- list()
  for (i in seq_along(starts)) {
    start_row <- starts[i]
    end_row <- ifelse(i < length(starts), starts[i + 1] - 1, nrow(raw_df))
    sections[[i]] <- raw_df[start_row:end_row, , drop = FALSE]
  }
  do.call(rbind, sections)
}

extract_sheet_table <- function(input_file, sheet_name) {
  raw <- read.xlsx(input_file, sheet = sheet_name, colNames = FALSE)
  
  get_sheet_kv <- function(raw, key) {
    idx <- which(trimws(as.character(raw$X1)) == key)
    if (length(idx) == 0) return(NA_character_)
    val <- trimws(as.character(raw$X2[idx[1]]))
    if (is.na(val) || val == "") NA_character_ else val
  }
  
  study_value <- get_sheet_kv(raw, "Study")
  cpms_id     <- get_sheet_kv(raw, "Study Id")
  
  starts <- find_section_starts(raw)
  if (length(starts) == 0) return(list(df = NULL, study = study_value, cpms_id = cpms_id))
  
  extracted <- extract_sections(raw, starts)
  colnames(extracted) <- make.names(as.character(extracted[1, ]), unique = TRUE)
  df <- extracted[-1, , drop = FALSE]
  
  list(df = df, study = study_value, cpms_id = cpms_id)
}

# --- Flags & cleaning (legacy behaviour preserved) ---

apply_flags_and_clean_legacy <- function(df, sheet_name, study_value, cpms_id) {
  value_list <- c(
    "Scheduled / Some Participants",
    "Scheduled / All Participants",
    "Unscheduled / Itemised Activities"
  )
  
  df$study_name <- study_value
  df$cpms_id    <- cpms_id
  df$Flag <- NA
  
  current_flag <- NA
  for (i in seq_len(nrow(df))) {
    if (df[i, 1] %in% value_list) current_flag <- df[i, 1]
    df$Flag[i] <- current_flag
  }
  
  df$Flag[is.na(df$Flag)] <- ifelse(sheet_name == "Setup & Closedown",
                                    "Setup & Closedown",
                                    "Scheduled / All Participants")
  
  df <- df[!(df$Activity %in% value_list | is.na(df$Activity)), ]
  df <- df[!apply(df[, 1, drop = FALSE], 1, function(row) row == colnames(df)[1]), ]
  
  df$SheetName <- sheet_name
  df
}

# --- Lookup: Scheduled visits from "Including MFF" ---

extract_mff_lookup <- function(df, sheet_name, study_value, cpms_id) {
  visit_cols <- get_visit_cols(df)
  
  mff_row <- df %>%
    filter(grepl("Including MFF", .data[["Activity.Cost"]])) %>%
    select(all_of(visit_cols))
  
  if (nrow(mff_row) == 0) return(NULL)
  
  tdf <- as.data.frame(t(mff_row), stringsAsFactors = FALSE)
  tdf$Visit_Name <- rownames(tdf)
  rownames(tdf) <- NULL
  
  tdf$Visit_Number <- paste("VISIT -", sprintf("%03d", seq_len(nrow(tdf))))
  tdf$Study_Arm <- sheet_name
  tdf$Study <- study_value
  tdf$CPMS_ID <- cpms_id
  
  names(tdf)[1] <- "ICT_Cost"
  
  tdf %>% select(CPMS_ID, Study, Visit_Number, Study_Arm, Visit_Name, ICT_Cost)
}

expand_to_visit_rows_legacy <- function(df, study_value, cpms_id, study_arm_value) {
  visit_cols <- get_visit_cols(df)
  
  flags <- df[, visit_cols, drop = FALSE]
  flags[is.na(flags)] <- 0
  flags[flags == "N/A"] <- 0
  flags <- data.frame(lapply(flags, function(x) as.numeric(as.character(x))))
  
  total_occ <- rowSums(flags)
  total_occ[is.na(total_occ) | total_occ == 0] <- 1
  total_occ <- round(total_occ)
  
  total_cost <- as.numeric(as.character(df[["Total.Activity.Cost"]]))
  cost_per_occ <- total_cost / total_occ
  
  out_list <- vector("list", nrow(df))
  for (i in seq_len(nrow(df))) {
    row_flags <- as.numeric(flags[i, ])
    row_flags[is.na(row_flags)] <- 0
    if (sum(row_flags) == 0) row_flags[1] <- 1  # legacy default occurrence
    
    rows <- list()
    for (j in seq_along(visit_cols)) {
      n <- row_flags[j]
      if (n <= 0) next
      rows[[length(rows) + 1]] <- data.frame(
        CPMS_ID     = rep(cpms_id, n),
        Study       = rep(study_value, n),
        Visit_Number= rep(paste("VISIT -", sprintf("%03d", j)), n),
        Study_Arm   = rep(study_arm_value, n),
        Visit_Name  = rep(visit_cols[j], n),
        ICT_Cost    = rep(cost_per_occ[i], n),
        stringsAsFactors = FALSE
      )
    }
    out_list[[i]] <- bind_rows(rows)
  }
  bind_rows(out_list)
}

# --- Lookup: UA/SSP expansion (legacy behaviour preserved) ---

build_ua_ssp_lookup_from_sheet <- function(df, study_value, cpms_id) {
  if (!("Flag" %in% names(df))) return(tibble())
  
  out <- list()
  
  df_ssp <- df %>% filter(Flag == "Scheduled / Some Participants")
  if (nrow(df_ssp) > 0) out[["SSP"]] <- expand_to_visit_rows_legacy(df_ssp, study_value, cpms_id, "SSP")
  
  df_ua <- df %>% filter(Flag == "Unscheduled / Itemised Activities")
  if (nrow(df_ua) > 0) out[["UA"]] <- expand_to_visit_rows_legacy(df_ua, study_value, cpms_id, "UA")
  
  bind_rows(out)
}

# --- Persistence: DuckDB auto-created ---

persist_ict_to_duckdb <- function(db_path, ict_cost_table) {
  require_pkg("DBI")
  require_pkg("duckdb")
  
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  DBI::dbExecute(con, "DROP TABLE IF EXISTS ict_costing_tbl")
  
  DBI::dbExecute(con, "
      CREATE TABLE IF NOT EXISTS ict_costing_tbl (
      CPMS_ID     VARCHAR NOT NULL,
      Study       VARCHAR,
      Visit_Number VARCHAR NOT NULL,
      Study_Arm    VARCHAR NOT NULL,
      Visit_Name   VARCHAR,
      ICT_Cost     DOUBLE  NOT NULL
    )
  ")
  
  DBI::dbWriteTable(con, "stg_ict_costing_tbl", ict_cost_table, overwrite = TRUE)
  
  DBI::dbExecute(con, "
      DELETE FROM ict_costing_tbl t
      USING stg_ict_costing_tbl s
      WHERE t.CPMS_ID = s.CPMS_ID
       AND t.Visit_Number = s.Visit_Number
       AND t.Study_Arm = s.Study_Arm
  ")
  
  DBI::dbExecute(con, "
    INSERT INTO ict_costing_tbl (CPMS_ID, Study, Visit_Number, Study_Arm, Visit_Name, ICT_Cost)
    SELECT CPMS_ID, Study, Visit_Number, Study_Arm, Visit_Name, ICT_Cost
    FROM stg_ict_costing_tbl
  ")
  
  DBI::dbExecute(con, "DROP TABLE stg_ict_costing_tbl")
  invisible(TRUE)
}

# --- Main (2 args only) ---

run_join_refactor <- function(input_file, output_file) {
  if (is.na(input_file) || is.na(output_file)) {
    stop("run_join_refactor(): input_file and output_file are required.")
  }
  if (!file.exists(input_file)) stop("Input file not found: ", input_file)
  
  sheet_names <- getSheetNames(input_file)
  
  processed_sheets <- list()
  ict_cost_table <- tibble()
  
  for (sheet_name in sheet_names) {
    parsed <- extract_sheet_table(input_file, sheet_name)
    df <- parsed$df
    study_value <- parsed$study
    cpms_id <- parsed$cpms_id
    
    message("Study: ", study_value, " | CPMS: ", cpms_id)
    
    if (is.null(df) || nrow(df) == 0) next
    
    # Build DuckDB table parts using ORIGINAL visit col names (keeps legacy behaviour)
    mff_part <- extract_mff_lookup(df, sheet_name, study_value, cpms_id)
    if (!is.null(mff_part) && nrow(mff_part) > 0) {
      ict_cost_table <- bind_rows(ict_cost_table, mff_part)
    }
    
    df <- apply_flags_and_clean_legacy(df, sheet_name, study_value, cpms_id)
    
    ua_ssp_part <- build_ua_ssp_lookup_from_sheet(df, study_value, cpms_id)
    if (nrow(ua_ssp_part) > 0) {
      ict_cost_table <- bind_rows(ict_cost_table, ua_ssp_part)
    }
    
    # Cleaned workbook output
    df <- coerce_visit_cols_numeric_legacy(df)
    
    # ✅ Rename visit columns for the final Excel output (persists to file)
    df <- rename_visit_cols(df)
    
    processed_sheets[[sheet_name]] <- df
  }
  
  write.xlsx(processed_sheets, file = output_file, rowNames = FALSE)
  
  db_path <- file.path(dirname(output_file), "ict_local.duckdb")
  if (nrow(ict_cost_table) > 0) persist_ict_to_duckdb(db_path, ict_cost_table)
  
  message("COMPLETE. Cleaned workbook written to: ", output_file)
  message("DuckDB created/updated at: ", db_path)
  
  invisible(list(
    output_file = output_file,
    db_path = db_path,
    n_rows_ict_cost = nrow(ict_cost_table)
  ))
}

# file paths
input_file  <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/candy study.xlsx"
output_file <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data.xlsx"

run_join_refactor(input_file, output_file)
