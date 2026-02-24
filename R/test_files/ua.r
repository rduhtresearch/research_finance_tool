library(openxlsx)

# Path to your Excel file
ICT_CSV_PATH <- "/Users/tategraham/Documents/NHS/R scripts/Refactor/testing_data/testing_data2.xlsx"

sheets <- getSheetNames(ICT_CSV_PATH)

df <- sapply(sheets, function(s) read.xlsx(ICT_CSV_PATH, sheet = s))


if(!any(sheets == 'Unscheduled Activities')) {
  
  #ua_sheet <- df[['Unscheduled Activities']]
  ua_sheet <- data.frame()
  
  for(s in df) {
    
    ua_rows <- s |>
               filter(Flag == 'Unscheduled / Itemised Activities')
    
    ua_sheet <- rbind(ua_sheet, ua_rows)
  }
  
} else {
  
  print('not missing')
  
}





