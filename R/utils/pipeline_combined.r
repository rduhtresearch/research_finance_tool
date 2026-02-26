#!/usr/bin/env Rscript
# pipeline_combined.R
#
# Merges refactor_1.R (Stage A: clean/standardise) and pipeline2.r (Stage B: reshape/normalise)
# into a single in-memory pipeline.
#
# What changed vs the originals:
#   - run_join_refactor() no longer writes the intermediate Excel file; instead it returns
#     processed_sheets (the named list of cleaned dataframes) alongside the other outputs.
#   - process_ict() gains an argument `df` so it can accept that in-memory list directly
#     instead of reading from disk. When `df` is supplied, `input_path` is not read again.
#   - A new entry function process_workbook() wires the two stages together.
#   - All other logic is identical to the originals — no renames, no rewrites.

source('/Users/tategraham/Documents/NHS/research_finance_tool/R/utils/add_study_arm.r')

suppressPackageStartupMessages({
  library(openxlsx)
  library(dplyr)
  library(tidyr)
  library(rlang)
  library(purrr)
})

require_pkg <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop("Missing package: ", pkg, ". Install it with install.packages('", pkg, "').")
  }
}

# ── Stage A helpers (from refactor_1.R — unchanged) ───────────────────────────

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
  
  tdf$ICT_Cost <- as.numeric(as.character(tdf$ICT_Cost))
  
  # ── Normalise Study_Arm labels to match pipeline conventions ──
  tdf$Study_Arm <- ifelse(tdf$Study_Arm == "Setup & Closedown", "SC", tdf$Study_Arm)
  
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
    if (sum(row_flags) == 0) row_flags[1] <- 1
    
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
        Visit_Name   = rep(df$Activity[i], n),  # ← activity name, repeated n times
        ICT_Cost    = rep(cost_per_occ[i], n),
        stringsAsFactors = FALSE
      )
    }
    out_list[[i]] <- bind_rows(rows)
  }
  bind_rows(out_list)
}

build_ua_ssp_lookup_from_sheet <- function(df, study_value, cpms_id) {
  if (!("Flag" %in% names(df))) return(tibble())
  
  out <- list()
  
  df_sc <- df %>% filter(Flag == "Setup & Closedown")
  if (nrow(df_sc) > 0) out[["SC"]] <- expand_to_visit_rows_legacy(df_sc, study_value, cpms_id, "SC")
  
  df_ssp <- df %>% filter(Flag == "Scheduled / Some Participants")
  if (nrow(df_ssp) > 0) out[["SSP"]] <- expand_to_visit_rows_legacy(df_ssp, study_value, cpms_id, "SSP")
  
  df_ua <- df %>% filter(Flag == "Unscheduled / Itemised Activities")
  if (nrow(df_ua) > 0) out[["UA"]] <- expand_to_visit_rows_legacy(df_ua, study_value, cpms_id, "UA")
  
  bind_rows(out)
}

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

# Stage A: reads the workbook, cleans/standardises, returns processed_sheets in memory.
# Previously this was run_join_refactor() but it wrote an intermediate Excel file at the end;
# that write has been removed. Everything else is identical.
run_stage_a <- function(input_file, db_dir = NULL) {
  if (is.na(input_file) || !file.exists(input_file)) {
    stop("run_stage_a(): input file not found: ", input_file)
  }
  
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
    
    mff_part <- extract_mff_lookup(df, sheet_name, study_value, cpms_id)
    if (!is.null(mff_part) && nrow(mff_part) > 0) {
      ict_cost_table <- bind_rows(ict_cost_table, mff_part)
    }
    
    df <- apply_flags_and_clean_legacy(df, sheet_name, study_value, cpms_id)
    
    ua_ssp_part <- build_ua_ssp_lookup_from_sheet(df, study_value, cpms_id)
    if (nrow(ua_ssp_part) > 0) {
      ict_cost_table <- bind_rows(ict_cost_table, ua_ssp_part)
    }
    
    df <- coerce_visit_cols_numeric_legacy(df)
    df <- rename_visit_cols(df)
    
    processed_sheets[[sheet_name]] <- df
  }
  
  # Write DuckDB if a directory was provided
  if (!is.null(db_dir)) {
    db_path <- file.path(db_dir, "ict_local.duckdb")
    if (nrow(ict_cost_table) > 0) persist_ict_to_duckdb(db_path, ict_cost_table)
    message("DuckDB created/updated at: ", db_path)
  }
  
  # Return the in-memory list — no intermediate Excel write
  list(
    processed_sheets = processed_sheets,
    ict_cost_table   = ict_cost_table
  )
}

# ── Stage B helpers (from pipeline2.r — unchanged) ────────────────────────────

.pivot_activity_data <- function(data) {
  if (is.null(data) || nrow(data) == 0) return(data)
  
  visit_cols <- grep("^VISIT", names(data), value = TRUE)
  if (length(visit_cols) == 0) {
    warning("No VISIT columns found; returning data unchanged.")
    return(data)
  }
  
  data %>%
    pivot_longer(cols = all_of(visit_cols), names_to = "Visit", values_to = "is_activity") %>%
    filter(is_activity == 1) %>%
    select(Visit, Activity, everything(), -is_activity)
}

.format_ua_columns <- function(data) {
  if (is.null(data) || nrow(data) == 0) return(data)
  
  ac_idx    <- which(names(data) == "Activity.Cost")
  total_idx <- which(names(data) == "Total.Activity.Cost")
  
  if (length(ac_idx) == 0 || length(total_idx) == 0) {
    warning("Columns 'Activity.Cost' / 'Total.Activity.Cost' not found; skipping strip.")
    return(data)
  }
  
  drop_start <- ac_idx + 2
  drop_end   <- total_idx - 1
  
  if (drop_start <= drop_end) data <- data[, -(drop_start:drop_end), drop = FALSE]
  #if ("VISIT.-.001" %in% names(data)) data[["VISIT.-.001"]] <- 1L
  if ("VISIT - 001" %in% names(data)) data[["VISIT - 001"]] <- 1L
  
  data
}

.extract_rows <- function(df_list, cond, exclude_sheets = character()) {
  df_list[!names(df_list) %in% exclude_sheets] %>%
    lapply(function(df) filter(df, !!cond)) %>%
    bind_rows()
}

.remove_rows <- function(df_list, cond, exclude_sheets = character()) {
  imap(df_list, function(df, nm) {
    if (nm %in% exclude_sheets) return(df)
    filter(df, !(!!cond) | is.na(!!cond))
  })
}

# Stage B: accepts the in-memory processed_sheets list from Stage A and reshapes to long format.
# Previously this was process_ict() and read from an Excel file; the read has been replaced
# with accepting `df` directly. Everything else is identical.
run_stage_b <- function(df) {
  UA_SHEET         <- "Unscheduled Activities"
  PHARM_SHEET      <- "Pharmacy"
  SETUP_SHEET      <- "Setup & Closedown"
  UA_FLAG          <- "Unscheduled / Itemised Activities"
  PHARM_DEPT       <- "Pharmacy"
  PROTECTED_SHEETS <- c(SETUP_SHEET, PHARM_SHEET, UA_SHEET)
  
  names(df) <- trimws(names(df))
  
  # Step 1: Unscheduled Activities
  ua_cond <- quo(Flag == UA_FLAG)
  if (UA_SHEET %in% names(df)) {
    df[[UA_SHEET]] <- .format_ua_columns(df[[UA_SHEET]])
  } else {
    df[[UA_SHEET]] <- .extract_rows(df, ua_cond, exclude_sheets = PROTECTED_SHEETS) %>%
      .format_ua_columns()
    df <- .remove_rows(df, ua_cond, exclude_sheets = PROTECTED_SHEETS)
  }
  
  # Step 2: Setup & Closedown
  if (SETUP_SHEET %in% names(df)) {
    df[[SETUP_SHEET]] <- .format_ua_columns(df[[SETUP_SHEET]])
  }
  
  # Step 3: Pharmacy
  pharm_cond <- quo(Department == PHARM_DEPT)
  pharm_new  <- .extract_rows(df, pharm_cond, exclude_sheets = PROTECTED_SHEETS)
  
  if (nrow(pharm_new) > 0) {
    df[[PHARM_SHEET]] <- if (PHARM_SHEET %in% names(df)) {
      bind_rows(df[[PHARM_SHEET]], pharm_new)
    } else {
      pharm_new
    }
    df <- .remove_rows(df, pharm_cond, exclude_sheets = PROTECTED_SHEETS)
  }
  
  # Step 4: Pivot to long format
  df_long <- lapply(df, .pivot_activity_data)
  
  invisible(df_long)
}

# ── Entry point ───────────────────────────────────────────────────────────────

#' Run the full pipeline in memory: read once → Stage A → Stage B → return.
#'
#' @param input_path   Path to the source .xlsx file (read once).
#' @param archive_dir  Optional. If provided, copies the original file here.
#' @param export_path  Optional. If provided, writes the final long-format output here.
#' @param db_dir       Optional. If provided, writes the DuckDB file here (same as before).
#' @return             Named list of long-format dataframes (one per sheet), invisibly.
process_workbook <- function(input_path, archive_dir = NULL, export_path = NULL, db_dir = NULL) {
  
  # Basic validation
  if (missing(input_path) || !nzchar(input_path)) stop("process_workbook(): input_path is required.")
  if (!file.exists(input_path)) stop("process_workbook(): file not found: ", input_path)
  
  # Optional: archive the original uploaded file
  if (!is.null(archive_dir)) {
    if (!dir.exists(archive_dir)) dir.create(archive_dir, recursive = TRUE)
    archive_dest <- file.path(archive_dir, basename(input_path))
    file.copy(input_path, archive_dest, overwrite = TRUE)
    message("Archived original to: ", archive_dest)
  }
  
  # Stage A: clean/standardise — returns in-memory list, no intermediate file written
  message("--- Stage A: cleaning/standardising ---")
  stage_a_result <- run_stage_a(input_file = input_path, db_dir = db_dir)
  
  # Stage B: reshape/normalise — accepts in-memory list directly, no file read
  message("--- Stage B: reshaping to long format ---")
  df_long <- run_stage_b(df = stage_a_result$processed_sheets)
  
  # Post-processing: append Study_Arm to each sheet
  message("--- Post-processing: adding Study_Arm ---")
  df_long <- add_study_arm(df_long)
  
  # Optional: write final export
  if (!is.null(export_path)) {
    write.xlsx(df_long, file = export_path, rowNames = FALSE)
    message("Final export written to: ", export_path)
  }
  
  message("--- Pipeline complete ---")
  invisible(df_long)
}
# ── Run ───────────────────────────────────────────────────────────────────────

# input_file  <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/test_study.xlsx"
# 
# process_workbook(
#   input_path  = input_file,
#   archive_dir = NULL,   # e.g. "/path/to/archive"
#   export_path = NULL,   # e.g. "/path/to/output.xlsx"
#   db_dir      = dirname(input_file)
# )
