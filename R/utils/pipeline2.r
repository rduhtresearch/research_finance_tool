# pipeline2.R

library(openxlsx)
library(dplyr)
library(tidyr)
library(rlang)
library(purrr)

# ── Private helpers (not intended for direct use) ─────────────────────────────

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
  if ("VISIT.-.001" %in% names(data)) data[["VISIT.-.001"]] <- 1L
  
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

# ── Public function ───────────────────────────────────────────────────────────

#' Load, process, and export an ICT activity workbook to long format.
#'
#' @param input_path  Path to the source .xlsx file.
#' @param export      If TRUE (default), writes a _Long_Format.xlsx alongside the source.
#' @return            Named list of long-format data frames (one per sheet), invisibly.
process_ict <- function(input_path, export = TRUE) {
  
  UA_SHEET         <- "Unscheduled Activities"
  PHARM_SHEET      <- "Pharmacy"
  SETUP_SHEET      <- "Setup & Closedown"
  UA_FLAG          <- "Unscheduled / Itemised Activities"
  PHARM_DEPT       <- "Pharmacy"
  PROTECTED_SHEETS <- c(SETUP_SHEET, PHARM_SHEET, UA_SHEET)
  
  # Load
  sheets <- getSheetNames(input_path)
  df     <- sapply(sheets, function(s) read.xlsx(input_path, sheet = s), simplify = FALSE)
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
  
  # Step 4: Pivot
  df_long <- lapply(df, .pivot_activity_data)
  
  # Step 5: Export - this is not needed for the pipelines 
  # if (export) {
  #   out_path <- sub("\\.xlsx$", "_Long_Format.xlsx", input_path)
  #   write.xlsx(df_long, file = out_path)
  #   message("Exported: ", out_path)
  # }
  
  invisible(df_long)
}