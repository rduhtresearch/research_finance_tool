library(openxlsx)
library(dplyr)
library(tidyr)
library(rlang)
library(purrr)

# ── Constants ─────────────────────────────────────────────────────────────────

ICT_CSV_PATH  <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/testing_data2.xlsx"
UA_SHEET      <- "Unscheduled Activities"
PHARM_SHEET   <- "Pharmacy"
SETUP_SHEET   <- "Setup & Closedown"
UA_FLAG       <- "Unscheduled / Itemised Activities"
PHARM_DEPT    <- "Pharmacy"

# Sheets never touched by row extraction/removal
PROTECTED_SHEETS <- c(SETUP_SHEET, PHARM_SHEET, UA_SHEET)

# ── Helpers ───────────────────────────────────────────────────────────────────

#' Pivot wide VISIT columns to long format, keeping only active visits
pivot_activity_data <- function(data) {
  if (is.null(data) || nrow(data) == 0) return(data)
  
  visit_cols <- grep("^VISIT", names(data), value = TRUE)
  if (length(visit_cols) == 0) {
    warning("No VISIT columns found; returning data unchanged.")
    return(data)
  }
  
  data %>%
    pivot_longer(
      cols      = all_of(visit_cols),
      names_to  = "Visit",
      values_to = "is_activity"
    ) %>%
    filter(is_activity == 1) %>%
    select(Visit, Activity, everything(), -is_activity)
}

#' Drop intermediate cost columns and reset VISIT-.001 to 1.
#' Used for sheets where every row must survive the pivot (UA, Setup & Closedown).
format_ua_columns <- function(data) {
  if (is.null(data) || nrow(data) == 0) return(data)
  
  ac_idx    <- which(names(data) == "Activity.Cost")
  total_idx <- which(names(data) == "Total.Activity.Cost")
  
  if (length(ac_idx) == 0 || length(total_idx) == 0) {
    warning("Columns 'Activity.Cost' / 'Total.Activity.Cost' not found; skipping strip.")
    return(data)
  }
  
  drop_start <- ac_idx + 2
  drop_end   <- total_idx - 1
  
  if (drop_start <= drop_end) {
    data <- data[, -(drop_start:drop_end), drop = FALSE]
  }
  
  if ("VISIT.-.001" %in% names(data)) {
    data[["VISIT.-.001"]] <- 1L
  }
  
  data
}

#' Collect rows matching a condition from all non-excluded sheets
extract_rows <- function(df_list, condition, exclude_sheets = character()) {
  cond <- enquo(condition)
  
  df_list[!names(df_list) %in% exclude_sheets] %>%
    lapply(function(df) filter(df, !!cond)) %>%
    bind_rows()
}

#' Remove rows matching a condition from all non-excluded sheets
remove_rows <- function(df_list, condition, exclude_sheets = character()) {
  cond <- enquo(condition)
  
  imap(df_list, function(df, nm) {
    if (nm %in% exclude_sheets) return(df)
    filter(df, !(!!cond) | is.na(!!cond))
  })
}


# ── Load data ─────────────────────────────────────────────────────────────────

sheets <- getSheetNames(ICT_CSV_PATH)
df     <- sapply(sheets, function(s) read.xlsx(ICT_CSV_PATH, sheet = s),
                 simplify = FALSE)

# Trim whitespace from sheet names to prevent silent mismatches
names(df) <- trimws(names(df))


# ── Step 1: Unscheduled Activities ────────────────────────────────────────────

if (UA_SHEET %in% names(df)) {
  df[[UA_SHEET]] <- format_ua_columns(df[[UA_SHEET]])
} else {
  df[[UA_SHEET]] <- df %>%
    extract_rows(Flag == UA_FLAG, exclude_sheets = PROTECTED_SHEETS) %>%
    format_ua_columns()
  
  df <- remove_rows(df, Flag == UA_FLAG, exclude_sheets = PROTECTED_SHEETS)
}


# ── Step 2: Setup & Closedown ─────────────────────────────────────────────────
# Every row must survive the pivot, so we set VISIT.-.001 = 1

if (SETUP_SHEET %in% names(df)) {
  df[[SETUP_SHEET]] <- format_ua_columns(df[[SETUP_SHEET]])
}


# ── Step 3: Pharmacy ──────────────────────────────────────────────────────────

pharm_new <- extract_rows(df, Department == PHARM_DEPT, exclude_sheets = PROTECTED_SHEETS)

if (nrow(pharm_new) > 0) {
  df[[PHARM_SHEET]] <- if (PHARM_SHEET %in% names(df)) {
    bind_rows(df[[PHARM_SHEET]], pharm_new)
  } else {
    pharm_new
  }
  
  df <- remove_rows(df, Department == PHARM_DEPT, exclude_sheets = PROTECTED_SHEETS)
}


# ── Step 4: Pivot all sheets to long format ───────────────────────────────────

df_long <- lapply(df, pivot_activity_data)
View(df_long)


# ── Step 5: Export ────────────────────────────────────────────────────────────

out_path <- sub("\\.xlsx$", "_Long_Format.xlsx", ICT_CSV_PATH)
write.xlsx(df_long, file = out_path)
message("Exported: ", out_path)

# 
# library(openxlsx)
# library(dplyr)
# 
# # Function to pivot and clean the data
# pivot_activity_data <- function(data) {
#   if (is.null(data) || nrow(data) == 0) return(data)
#   
#   data %>%
#     tidyr::pivot_longer(
#       cols = starts_with("VISIT"),
#       names_to = "Visit",
#       values_to = "is_activity"
#     ) %>%
#     # Keep only rows where activity occurred
#     filter(is_activity == 1) %>%
#     # Keep the relevant columns (and any others you might need, like Department)
#     select(Visit, Activity, everything(), -is_activity)
# }
# 
# # Function to strip columns and reset Visit 1
# format_ua_columns <- function(data) {
#   if (is.null(data) || nrow(data) == 0) return(data)
#   
#   # Logic: Keep Activity.Cost + 1, then skip the next few to keep Total.Activity.Cost
#   start_col <- which(names(data) == 'Activity.Cost') + 2
#   end_col <- which(names(data) == 'Total.Activity.Cost') - 1
#   
#   # Update Visit 1 value
#   if('VISIT.-.001' %in% names(data)) {
#     data$'VISIT.-.001' <- 1
#   }
#   
#   # Remove range if valid
#   if (start_col <= end_col) {
#     data <- data[, -(start_col:end_col)]
#   }
#   return(data)
# }
# 
# # Function to extract rows based on a condition, excluding specific sheets
# extract_rows <- function(df_list, condition_expr, exclude_sheets = c()) {
#   lapply(names(df_list), function(nm) {
#     if (nm %in% exclude_sheets) return(NULL)
#     df_list[[nm]] %>% filter(!!rlang::enquo(condition_expr))
#   }) %>% bind_rows()
# }
# 
# # Function to remove rows based on a condition, excluding specific sheets
# remove_rows <- function(df_list, condition_expr, exclude_sheets = c()) {
#   lapply(names(df_list), function(nm) {
#     if (nm %in% exclude_sheets) return(df_list[[nm]])
#     df_list[[nm]] %>% filter(!(!!rlang::enquo(condition_expr)) | is.na(!!rlang::enquo(condition_expr)))
#   }) %>% setNames(names(df_list))
# }
# 
# # --- INITIALIZATION ---
# ICT_CSV_PATH <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/testing_data2.xlsx"
# sheets <- getSheetNames(ICT_CSV_PATH)
# df <- sapply(sheets, function(s) read.xlsx(ICT_CSV_PATH, sheet = s), simplify = FALSE)
# 
# # --- STEP 1: PROCESS UNSCHEDULED ACTIVITIES ---
# ua_name <- 'Unscheduled Activities'
# 
# if (!(ua_name %in% names(df))) {
#   # Extract and format from all sheets
#   ua_extracted <- extract_rows(df, Flag == 'Unscheduled / Itemised Activities')
#   df[[ua_name]] <- format_ua_columns(ua_extracted)
#   
#   # Remove from original sources
#   df <- remove_rows(df, Flag == 'Unscheduled / Itemised Activities', exclude_sheets = ua_name)
# } else {
#   # Just format the existing one
#   df[[ua_name]] <- format_ua_columns(df[[ua_name]])
# }
# 
# # --- STEP 2: PROCESS PHARMACY DATA ---
# pharm_name <- 'Pharmacy'
# pharm_exclude <- c('Setup & Closedown', pharm_name)
# 
# # Extract Pharmacy rows (ignoring Setup & Closedown)
# pharm_extracted <- extract_rows(df, Department == 'Pharmacy', exclude_sheets = pharm_exclude)
# 
# if (nrow(pharm_extracted) > 0) {
#   # Add to existing Pharmacy sheet or create new
#   if (pharm_name %in% names(df)) {
#     df[[pharm_name]] <- bind_rows(df[[pharm_name]], pharm_extracted)
#   } else {
#     df[[pharm_name]] <- pharm_extracted
#   }
#   
#   # Remove Pharmacy rows from all other sheets (except Setup & Closedown)
#   df <- remove_rows(df, Department == 'Pharmacy', exclude_sheets = pharm_exclude)
# }
# 
# # --- STEP 3: PIVOT ALL SHEETS ---
# 
# # This applies the pivot function to every data frame in your list
# df_long_list <- lapply(df, pivot_activity_data)
# View(df_long_list$`Non-subset  `)
# 
# # --- STEP 4: EXPORT ---
# # You can now export the 'long' version of your workbook
# write.xlsx(df_long_list, file = gsub(".xlsx", "_Long_Format.xlsx", ICT_CSV_PATH))

# library(openxlsx)
# 
# format_ua_columns <- function(data) {
#   if (nrow(data) == 0) return(data)
#   
#   # Find indices based on your +2 logic
#   start_col <- which(names(data) == 'Activity.Cost') + 2
#   end_col <- which(names(data) == 'Total.Activity.Cost') - 1
#   
#   # Set Visit 1 to 1
#   if('VISIT.-.001' %in% names(data)) {
#     data$'VISIT.-.001' <- 1
#   }
#   
#   # Strip columns if the range is valid
#   if (start_col <= end_col) {
#     data <- data[, -(start_col:end_col)]
#   }
#   
#   return(data)
# }
# 
# library(openxlsx)
# library(dplyr)
# 
# 1. Load your data
ICT_CSV_PATH <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/testing_data2.xlsx"
sheets <- getSheetNames(ICT_CSV_PATH)
df <- sapply(sheets, function(s) read.xlsx(ICT_CSV_PATH, sheet = s), simplify = FALSE)
View(df$`Setup & Closedown`)
# 
# # 2. Set the target sheet name
# target_name <- 'Unscheduled Activities'
# 
# # 3. Execute the branch logic
# if (!(target_name %in% names(df))) {
#   
#   # SCENARIO: Create sheet from others
#   list_of_ua <- lapply(df, function(s) {
#     ua_rows <- s |> filter(Flag == 'Unscheduled / Itemised Activities')
#     if(nrow(ua_rows) > 0) format_ua_columns(ua_rows) else NULL
#   })
#   
#   df[[target_name]] <- bind_rows(list_of_ua)
#   
#   # Remove the moved rows from original sheets
#   df <- lapply(df, function(s) {
#     s |> filter(Flag != 'Unscheduled / Itemised Activities' | is.na(Flag))
#   })
#   
# } else {
#   
#   # SCENARIO: Clean the existing sheet
#   df[[target_name]] <- format_ua_columns(df[[target_name]])
#   
# }
# 
# View(df)
# 
# 
# # process_visit_columns <- function(data, visit_col = 'VISIT.-.001') {
# #   # 1. Identify indices
# #   # We use +2 here as per your updated logic to keep more leading columns
# #   start_col <- which(names(data) == 'Activity.Cost') + 2
# #   end_col <- which(names(data) == 'Total.Activity.Cost') - 1
# #   
# #   # 2. Update the specific visit column to 1
# #   # Using data[[visit_col]] makes it flexible for different column names
# #   if(visit_col %in% names(data)) {
# #     data[[visit_col]] <- 1
# #   }
# #   
# #   # 3. Remove the specified range
# #   if (start_col <= end_col) {
# #     data <- data[, -(start_col:end_col)]
# #   }
# #   
# #   return(data)
# # }
# # 
# # 
# # df <- sapply(sheets, function(s) read.xlsx(ICT_CSV_PATH, sheet = s))
# # View(df$`Non-subset  `)
# # 
# # ua_format <- function(rows) {
# #   
# #   start_col <- which(names(rows) == 'Activity.Cost') + 2
# #   end_col <- which(names(rows) == 'Total.Activity.Cost') - 1
# #   
# #   rows$'VISIT.-.001' <- 1
# #   
# #   rows[, -(start_col:end_col)]
# # }
# # 
# # list_of_ua <- lapply(df, function(s) {
# #   ua_rows <- s |> filter(Flag == 'Unscheduled / Itemised Activities')
# #   if(nrow(ua_rows) > 0) return(ua_format(ua_rows)) else return(NULL)
# # })
# # ua_sheet <- dplyr::bind_rows(list_of_ua)
# # 
# # # 2. REMOVE those rows from the original list (Discard the rows)
# # df <- lapply(df, function(s) {
# #   # Keep only rows where Flag is NOT 'Unscheduled / Itemised Activities'
# #   # Or keep rows where Flag is NA (to avoid accidentally deleting empty rows)
# #   s |> filter(Flag != 'Unscheduled / Itemised Activities' | is.na(Flag))
# # })
# # 
# # # 3. Add the compiled sheet back to the list
# # df[['Unscheduled Activities']] <- ua_sheet
# # View(df$`Unscheduled Activities`)
# # 
# # View(df$`Non-subset  `)
# # 
# # # if(!any(sheets == 'Unscheduled Activities')) {
# # #   
# # #   #ua_sheet <- df[['Unscheduled Activities']]
# # #   ua_sheet <- data.frame()
# # #   
# # #   for(s in df) {
# # #     
# # #     ua_rows <- s |>
# # #                filter(Flag == 'Unscheduled / Itemised Activities')
# # #     
# # #     ua_sheet <- rbind(ua_sheet, ua_format(ua_rows))
# # #   }
# # #   
# # # } else {
# # #   
# # #   print('not missing')
# # #   
# # # }
# 
# 
# 
# 
# 
# 
