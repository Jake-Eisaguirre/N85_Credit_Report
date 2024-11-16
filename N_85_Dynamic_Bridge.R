if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, here, DBI, odbc, padr, writexl)


# Connect to the `PLAYGROUND` database and append data if necessary
tryCatch({
  db_connection <- DBI::dbConnect(odbc::odbc(),  # Establish a database connection using ODBC for the playground database
                                  Driver = "SnowflakeDSIIDriver",  # Specify the Snowflake ODBC driver
                                  Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",  # Server address
                                  WAREHOUSE = "DATA_LAKE_READER",  # Specify the Snowflake warehouse
                                  Database = "ENTERPRISE",  # Specify the database name
                                  UID = "jacob.eisaguirre@hawaiianair.com",  # User ID for authentication
                                  authenticator = "externalbrowser")  # Use external browser for authentication
  print("Database Connected!")  # Print success message if connection is established
}, error = function(cond) {
  print("Unable to connect to Database.")  # Print error message if connection fails
})

# Set schema and retrieve data from `AA_FINAL_PAIRING` table
dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS") 

current_date <- Sys.Date()

#current_date <- as.Date("2024-10-31")

current_bid_period <- substr(as.character(current_date), 1, 7)


q_master_history <- paste0("SELECT * FROM CT_MASTER_HISTORY WHERE BID_PERIOD = '", current_bid_period, "';")

mh <- dbGetQuery(db_connection, q_master_history) 


q_master_pairing <- paste0("SELECT * FROM CT_MASTER_PAIRING WHERE BID_PERIOD = '", current_bid_period, "';")

mp <- dbGetQuery(db_connection, q_master_pairing) 


min_date <- min(mp$PAIRING_DATE, na.rm = TRUE) 

max_date <- max(mp$PAIRING_DATE, na.rm = TRUE) 


q_msched <- paste0("select * from CT_FLIGHT_LEG WHERE PAIRING_DATE BETWEEN'", min_date,"' AND '", max_date,"';")

raw_fl <- dbGetQuery(db_connection, q_msched)


pm_q <- paste0("select PAIRING_DATE, PAIRING_NO, FLIGHT_NO, CREW_INDICATOR, PAIRING_POSITION, BID_TYPE,
         ACT_CREDIT_INT_TIME, ACT_CREDIT_DOM_TIME, UPDATE_DATE, UPDATE_TIME
         from CT_PAIRING_MASTER 
         where PAIRING_DATE BETWEEN'", min_date,"' AND '", max_date,"';")

# ACT_CREDIT_CO_DOM_TIME, ACT_CREDIT_CO_INT_TIME, ACT_CREDIT_DOM_TIME, ACT_DEADHEAD_CREDIT_CO_DOM_TIME, ACT_DEADHEAD_CREDIT_CO_INT_TIME, ACT_DH_CREDIT_DOM_TIME,
#ACT_DH_CREDIT_INT_TIME,

raw_pm <- dbGetQuery(db_connection, pm_q)

update_dt_rlv <- paste0((as_datetime(paste0(current_bid_period, "-24 00:00:00")) - months(1)), " 00:00:00") 



rlv_FAs <- mh %>% 
  #filter(PAIRING_DATE == "2024-10-25") %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>% 
  group_by(CREW_ID) %>% 
  filter(update_dt <= update_dt_rlv, 
         CREW_INDICATOR == "FA",
         TRANSACTION_CODE %in% c("RLV", "RSV"),
         !LINE_TYPE == "B") %>% 
  ungroup() %>% 
  select(CREW_ID, BASE) %>% 
  distinct()


pairing_num_date <- mp %>%
  filter(
    CREW_INDICATOR == "FA",
    #PAIRING_DATE == "2024-10-25"
  ) %>%
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
  group_by(CREW_ID, PAIRING_DATE) %>%
  filter(update_dt == max(update_dt),
         #TRANSACTION_CODE %in% c("ARC"),
  ) %>% 
  select(CREW_ID, BID_PERIOD, PAIRING_NO, PAIRING_ASSIGNMENT_CODE, PAIRING_DATE, PAIRING_POSITION) %>% 
  ungroup() %>% 
  filter(!PAIRING_ASSIGNMENT_CODE == c("2SK"))


final_tran <- mh %>%
  filter(!TRANSACTION_CODE %in%  c("SNO", "RSK", "RRD", "ADV")) %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
  group_by(CREW_ID, PAIRING_DATE) %>%
  filter(update_dt == max(update_dt)) %>% 
  select(!c(update_dt)) %>% 
  ungroup() %>% 
  filter(TRANSACTION_CODE %in%  c("VC1", "VC2", "MIL", "UNI", "2CB", "2SK", "FLV", "FLP", "VCB")) %>% 
  filter(CREW_ID %in% rlv_FAs$CREW_ID) %>%
  group_by(CREW_ID, PAIRING_DATE) %>% 
  # Filter to keep only "FLV" if both "FLP" and "FLV" are present on the same day
  filter(!(any(TRANSACTION_CODE == "FLV") & TRANSACTION_CODE == "FLP")) %>%
  ungroup() %>% 
  group_by(CREW_ID, TRANSACTION_CODE) %>%
  select(CREW_ID, BID_PERIOD, BASE, PAIRING_NO, TRANSACTION_CODE, PAIRING_DATE, TO_DATE, PAIRING_POSITION, CREDIT) %>% 
  rename(combined_credit = CREDIT) %>% 
  mutate(combined_credit = round(as.numeric(hms(combined_credit)), 2) / 3600,
         flag = if_else(TRANSACTION_CODE == "2SK" & !is.na(PAIRING_NO), 1, 0),
         flag_3 = if_else(TRANSACTION_CODE == "FLP" & !is.na(PAIRING_NO), 1, 0)) %>% 
  filter(!flag == 1) %>%
  filter(!flag_3 == 1) %>% 
  ungroup() %>% 
  group_by(CREW_ID, TRANSACTION_CODE, PAIRING_DATE) %>%
  filter(combined_credit == max(combined_credit)) %>% 
  ungroup() %>%
  group_by(CREW_ID, TRANSACTION_CODE) %>%
  mutate(flag_2 = if_else(!PAIRING_DATE == TO_DATE & TRANSACTION_CODE == "2SK", 1, 0)) %>%
  filter(flag_2 == 0) %>%
  select(!c(flag, flag_2, flag_3)) %>% 
  ungroup()



mid_pm <- raw_pm %>% 
  #select(PAIRING_DATE, PAIRING_NO, FLIGHT_NO, CREW_INDICATOR, PAIRING_POSITION, ACT_CREDIT_INT_TIME,  UPDATE_DATE, UPDATE_TIME) %>% 
  filter(CREW_INDICATOR == "FA") %>% 
  group_by(PAIRING_DATE, PAIRING_NO, FLIGHT_NO) %>% 
  mutate(temp_id = cur_group_id(),
         update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
  filter(
    #!duplicated(temp_id),
    update_dt == max(update_dt)
  ) %>%
  select(-PAIRING_POSITION) %>%
  ungroup() %>%
  # Convert all columns starting with "ACT" to decimal hours
  mutate(across(starts_with("ACT"), ~ round(as.numeric(hms(.)), 2) / 3600)) %>%
  # Calculate combined_credit as the sum of all "ACT" columns
  mutate(combined_credit = rowSums(select(., starts_with("ACT")), na.rm = TRUE), .after = CREW_INDICATOR) %>% 
  select(PAIRING_NO, PAIRING_DATE, FLIGHT_NO, CREW_INDICATOR, combined_credit) 


cred_final <- inner_join(pairing_num_date, mid_pm) %>% 
  group_by(CREW_ID, PAIRING_NO, PAIRING_DATE, FLIGHT_NO) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id),
         CREW_ID %in% c(rlv_FAs$CREW_ID)) %>% 
  ungroup() %>% 
  mutate(PAIRING_NO = factor(PAIRING_NO, levels = unique(PAIRING_NO[order(nchar(PAIRING_NO), PAIRING_NO)]))) %>%
  # Arrange data by CREW_ID, PAIRING_DATE, and ordered PAIRING_NO
  arrange(CREW_ID, PAIRING_DATE, PAIRING_NO) %>%
  # Group by CREW_ID and PAIRING_DATE
  group_by(CREW_ID, PAIRING_DATE) %>%
  # Filter to keep only the row with the largest suffix in PAIRING_NO
  slice_max(PAIRING_NO) %>%
  ungroup() %>% 
  filter(!PAIRING_ASSIGNMENT_CODE %in% c("O75", "TTD", "TSD", "SOP", "VNO", "FLU", "PLS", "UNA", "NOS", "XXX", 
                                         "VC1", "VC2", "MIL", "UNI", "2CB", "2SK", "FLV", "FLP", "VCB", "REM",
                                         "PER", "FAR", "PER", "OCC", "N/S", "BSN", "ADD"), # Do we need "REM"?
         #PAIRING_ASSIGNMENT_CODE == "ASN"
  ) %>% 
  select(!c(temp_id)) %>% 
  plyr::rbind.fill(final_tran) %>% 
  unite("TRANSACTION_CODE", c(PAIRING_ASSIGNMENT_CODE, TRANSACTION_CODE), sep = "", na.rm = T) %>% 
  select(!c(BASE, CREW_INDICATOR, TO_DATE)) %>% 
  inner_join(rlv_FAs, by = c("CREW_ID")) 
# %>% 
#   filter(PAIRING_DATE <= current_date)



final_monnth_agg <- data.frame()

date_range <- sort(unique(cred_final$PAIRING_DATE))

for(i in 1:length(date_range)){

month_agg <- cred_final %>% 
  filter(PAIRING_DATE <= date_range[i]) %>% 
  group_by(CREW_ID, BASE, BID_PERIOD) %>% 
  reframe(monthly_credit = sum(combined_credit)) %>% 
  mutate(as_of_PAIRING_DATE = date_range[i])

final_monnth_agg <- rbind(month_agg, final_monnth_agg)

print(paste("Completed:", date_range[i]))

}

cur_final_month_agg <- final_monnth_agg %>% 
  filter(as_of_PAIRING_DATE <= current_date)


final_mean_hnl <- data.frame()
final_mean_lax <- data.frame()

for(i in 1:length(date_range)){

  mean_hnl <- final_monnth_agg %>% 
  filter(BASE == "HNL",
         as_of_PAIRING_DATE == date_range[i]) %>% 
  mutate(`Current Trend Rate` = median(monthly_credit),
         `Current Trend Rate` = round(`Current Trend Rate`, 2)) %>% 
  select(`Current Trend Rate`, BASE) %>% 
  distinct() %>% 
  mutate(as_of_PAIRING_DATE = date_range[i])
  
  final_mean_hnl <- rbind(mean_hnl, final_mean_hnl)

mean_lax <- final_monnth_agg %>% 
  filter(BASE == "LAX",
         as_of_PAIRING_DATE == date_range[i]) %>% 
  mutate(`Current Trend Rate` = median(monthly_credit),
         `Current Trend Rate` = round(`Current Trend Rate`, 2)) %>% 
  select(`Current Trend Rate`, BASE) %>% 
  distinct() %>% 
  mutate(as_of_PAIRING_DATE = date_range[i])
 
final_mean_lax <- rbind(mean_lax, final_mean_lax)

print(paste("Completed:", date_range[i]))

}

mean_bases <- rbind(final_mean_hnl, final_mean_lax) %>% 
  ungroup()


final_bid_rate <- data.frame()

for(i in 1:length(date_range)){

bid_rate <- cred_final %>% 
  mutate(number_days = (as.numeric(max_date - min_date) )) %>% 
  select(number_days, BASE) %>% 
  distinct() %>% 
  group_by(BASE) %>% 
  mutate(exp_rate = 70 / number_days,
         to_date = as.numeric(date_range[i] - min_date) + 1,
         `Linear Trend Rate` = exp_rate * to_date,
         `Linear Trend Rate` = round(`Linear Trend Rate`, 2),
         as_of_PAIRING_DATE = date_range[i]) 

final_bid_rate <- rbind(final_bid_rate, bid_rate)

print(paste("Completed:", date_range[i]))

}


final_final_bid_rate <- final_bid_rate %>% 
  inner_join(mean_bases) %>% 
  mutate(credit_rate_color = if_else(`Current Trend Rate` < `Linear Trend Rate`, "Green", "Red")) %>% 
  pivot_longer(
    cols = c(`Current Trend Rate`, `Linear Trend Rate`), 
    names_to = "Credit Rate", 
    values_to = "rate_value"
  ) %>% 
  select(BASE, as_of_PAIRING_DATE, `Credit Rate`, rate_value, credit_rate_color) %>% 
  ungroup() %>% 
  mutate(credit_rate_color = if_else(`Credit Rate` == "Linear Trend Rate", "Grey", credit_rate_color)) %>% 
  filter(as_of_PAIRING_DATE <= current_date)
  


write_xlsx(cur_final_month_agg, "Z:/OperationsResourcePlanningAnalysis/N85Report/month_agg.xlsx")

write_xlsx(final_final_bid_rate, "Z:/OperationsResourcePlanningAnalysis/N85Report/bid_rate_3.xlsx")



cred_final_greater_85_final <- data.frame()

for(i in 1:length(date_range)){

list_greater_85 <- final_monnth_agg %>% 
  filter(as_of_PAIRING_DATE == date_range[i],
         monthly_credit >= 85) %>% 
  select(CREW_ID)

cred_final_greater_85 <- mh %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>% 
  group_by(CREW_ID) %>% 
  filter(
    update_dt <= update_dt_rlv, 
    CREW_INDICATOR == "FA",
    TRANSACTION_CODE %in% c("RLV", "RSV"),
    CREW_ID %in% list_greater_85$CREW_ID
  ) %>% 
  select(CREW_ID, BASE, PAIRING_DATE, TRANSACTION_CODE) %>% 
  filter(PAIRING_DATE >= date_range[i]) %>% 
  mutate(reserve_day_count = n()) %>% 
  ungroup() %>% 
  add_row(CREW_ID = NA, BASE = NA, PAIRING_DATE = NA, TRANSACTION_CODE = NA, reserve_day_count = NA) %>% 
  mutate(as_of_PAIRING_DATE = date_range[i])

cred_final_greater_85_final <- rbind(cred_final_greater_85_final, cred_final_greater_85)

print(paste("Completed:", date_range[i]))

}


write_xlsx(cred_final_greater_85_final, "Z:/OperationsResourcePlanningAnalysis/N85Report/list_greater_85.xlsx")
