#' Append a Study_Arm column to each sheet of the long-format output.
#'
#' Rules (matching ict_costing_tbl logic):
#'   - Flag == "Scheduled / Some Participants" - "SSP"
#'   - Flag == "Unscheduled / Itemised Activities" - "UA"
#'   - Flag == "Setup & Closedown"                - "UA"
#'   - All other flags (Scheduled / All)    - value of SheetName (Arm)
#'
#' @param df_long Named list of long-format dataframes returned by run_stage_b().
#' @return The same list with a Study_Arm column appended to each dataframe.
add_study_arm <- function(df_long) {
  imap(df_long, function(df, sheet_nm) {
    if (is.null(df) || nrow(df) == 0) return(df)
    
    if (!("Flag" %in% names(df))) {
      warning("add_study_arm(): 'Flag' column missing in sheet '", sheet_nm, "'; Study_Arm set to sheet name.")
      df$Study_Arm <- sheet_nm
      return(df)
    }
    
    df$Study_Arm <- dplyr::case_when(
      df$Flag == "Scheduled / Some Participants"     ~ "SSP",
      df$Flag == "Unscheduled / Itemised Activities" ~ "UA",
      df$Flag == "Setup & Closedown"                 ~ "SC",
      TRUE                                           ~ if ("SheetName" %in% names(df)) df$SheetName else sheet_nm
    )
    
    df
  })
}