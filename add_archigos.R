# Process Archigos to create country-year-leader panel
archigos_processed <- archigos_raw %>%
        mutate(
                # Convert date strings to dates (already in date format from read_tsv)
                start_date = startdate,
                end_date = enddate,
                
                # Extract years
                start_year = year(start_date),
                end_year = year(end_date)
        ) %>%
        # Handle leaders still in office (end_year might be 2015 or missing)
        mutate(end_year = ifelse(is.na(end_year) | end_year < start_year, 
                                 2020, end_year)) %>%
        select(
                COWcode = ccode,
                leader_name = leader,
                leader_id = leadid,
                start_date, end_date,
                start_year, end_year,
                entry_type = entry,
                exit_type = exit,
                exitcode,
                post_tenure_fate = posttenurefate,
                gender,
                year_born = yrborn,
                year_died = yrdied,
                previous_times = prevtimesinoffice  # CORRECTED COLUMN NAME
        )

# -------------------------------------------------------------------------
# 2. EXPAND TO COUNTRY-YEAR PANEL (Dec 31 coding)
# -------------------------------------------------------------------------

# Create a row for each year a leader was in power
archigos_country_year <- archigos_processed %>%
        # For each leader, create all years they were in power
        rowwise() %>%
        mutate(year_list = list(start_year:end_year)) %>%
        unnest(year_list) %>%
        rename(year = year_list) %>%
        # Apply Dec 31 rule: Check if leader was in power on Dec 31
        mutate(
                dec31_date = ymd(paste0(year, "-12-31")),
                in_power_dec31 = dec31_date >= start_date & dec31_date <= end_date
        ) %>%
        filter(in_power_dec31) %>%
        # If multiple leaders in one year, keep the one in power on Dec 31
        group_by(COWcode, year) %>%
        arrange(COWcode, year, desc(start_date)) %>%
        slice(1) %>%
        ungroup()

# -------------------------------------------------------------------------
# 3. CALCULATE LEADER TENURE
# -------------------------------------------------------------------------

archigos_country_year <- archigos_country_year %>%
        group_by(COWcode, leader_id) %>%
        arrange(COWcode, leader_id, year) %>%
        mutate(
                # Years in office as of Dec 31 of this year
                leader_tenure_years = year - start_year + 1,
                
                # Days in office as of Dec 31 (more precise)
                leader_tenure_days = as.numeric(dec31_date - start_date),
                
                # Binary: First year in office
                leader_first_year = ifelse(year == start_year, 1, 0),
                
                # Binary: Last year in office
                leader_last_year = ifelse(year == end_year, 1, 0),
                
                # Age of leader (if birth year available)
                leader_age = ifelse(!is.na(year_born), year - year_born, NA)
        ) %>%
        ungroup()

# Create clean variables for merging
archigos_merge <- archigos_country_year %>%
        select(
                COWcode, year,
                leader_name, leader_id,
                leader_tenure_years, leader_tenure_days,
                leader_first_year, leader_last_year,
                leader_age, gender,
                entry_type, exit_type, exitcode,
                post_tenure_fate,
                previous_times
        )

# -------------------------------------------------------------------------
# 4. LOAD DYADIC DATA
# -------------------------------------------------------------------------

dyad_data <- read_csv(here("ready_data", "GRAVE_D_Master_Final.csv"))

# -------------------------------------------------------------------------
# 5. MERGE LEADER DATA FOR BOTH SIDES
# -------------------------------------------------------------------------

# Merge for Side A
dyad_with_leaders <- dyad_data %>%
        left_join(
                archigos_merge,
                by = c("COWcode_a" = "COWcode", "year" = "year")
        ) %>%
        rename_with(
                ~paste0(., "_a"),
                .cols = c(leader_name, leader_id, leader_tenure_years, 
                          leader_tenure_days, leader_first_year, leader_last_year,
                          leader_age, gender, entry_type, exit_type, exitcode,
                          post_tenure_fate, previous_times)
        )

# Merge for Side B
dyad_with_leaders <- dyad_with_leaders %>%
        left_join(
                archigos_merge,
                by = c("COWcode_b" = "COWcode", "year" = "year")
        ) %>%
        rename_with(
                ~paste0(., "_b"),
                .cols = c(leader_name, leader_id, leader_tenure_years,
                          leader_tenure_days, leader_first_year, leader_last_year,
                          leader_age, gender, entry_type, exit_type, exitcode,
                          post_tenure_fate, previous_times)
        )

# -------------------------------------------------------------------------
# 6. CREATE DYADIC LEADER VARIABLES
# -------------------------------------------------------------------------

dyad_with_leaders <- dyad_with_leaders %>%
        mutate(
                # Tenure difference (absolute)
                leader_tenure_diff = abs(leader_tenure_years_a - leader_tenure_years_b),
                
                # Both leaders are new (first year)
                both_leaders_new = ifelse(leader_first_year_a == 1 & leader_first_year_b == 1, 1, 0),
                
                # At least one leader is new
                any_leader_new = ifelse(leader_first_year_a == 1 | leader_first_year_b == 1, 1, 0),
                
                # Leader transition on either side
                leader_change_a = leader_first_year_a,
                leader_change_b = leader_first_year_b,
                
                # Same leader in both countries (unlikely but possible)
                same_leader = ifelse(!is.na(leader_id_a) & !is.na(leader_id_b) & 
                                             leader_id_a == leader_id_b, 1, 0),
                
                # Both female leaders
                both_female_leaders = ifelse(gender_a == "F" & gender_b == "F", 1, 0),
                
                # Age difference
                leader_age_diff = abs(leader_age_a - leader_age_b),
                
                # Irregular entry for either leader (for conflict risk)
                irregular_entry_a = ifelse(!is.na(entry_type_a) & entry_type_a == "Irregular", 1, 0),
                irregular_entry_b = ifelse(!is.na(entry_type_b) & entry_type_b == "Irregular", 1, 0),
                
                # Both leaders entered irregularly
                both_irregular_entry = ifelse(irregular_entry_a == 1 & irregular_entry_b == 1, 1, 0)
        )

# -------------------------------------------------------------------------
# 7. SAVE UPDATED DATASET
# -------------------------------------------------------------------------

write_csv(dyad_with_leaders, here("ready_data", "GRAVE_D_Master_with_Leaders.csv"))

# -------------------------------------------------------------------------
# 8. DIAGNOSTIC CHECKS
# -------------------------------------------------------------------------

# Check merge success rate
cat("\n=== MERGE DIAGNOSTICS ===\n")
cat("Total dyad-years:", nrow(dyad_with_leaders), "\n")
cat("Dyad-years with leader data for Side A:", 
    sum(!is.na(dyad_with_leaders$leader_id_a)), "\n")
cat("Dyad-years with leader data for Side B:", 
    sum(!is.na(dyad_with_leaders$leader_id_b)), "\n")
cat("Dyad-years with leader data for both sides:", 
    sum(!is.na(dyad_with_leaders$leader_id_a) & !is.na(dyad_with_leaders$leader_id_b)), "\n")

# Summary statistics for tenure
cat("\n=== LEADER TENURE SUMMARY (Side A) ===\n")
summary(dyad_with_leaders$leader_tenure_years_a)

cat("\n=== LEADER TENURE SUMMARY (Side B) ===\n")
summary(dyad_with_leaders$leader_tenure_years_b)

# Check for leader transitions
cat("\n=== LEADER TRANSITIONS ===\n")
cat("Dyad-years with new leader on Side A:", 
    sum(dyad_with_leaders$leader_change_a == 1, na.rm = TRUE), "\n")
cat("Dyad-years with new leader on Side B:", 
    sum(dyad_with_leaders$leader_change_b == 1, na.rm = TRUE), "\n")

# Check entry types
cat("\n=== IRREGULAR ENTRIES ===\n")
cat("Side A irregular entries:", 
    sum(dyad_with_leaders$irregular_entry_a == 1, na.rm = TRUE), "\n")
cat("Side B irregular entries:", 
    sum(dyad_with_leaders$irregular_entry_b == 1, na.rm = TRUE), "\n")

# Sample check: Show a few cases
cat("\n=== SAMPLE ROWS (USA dyads) ===\n")
dyad_with_leaders %>%
        filter(COWcode_a == 2 & year >= 2000) %>%
        select(year, COWcode_a, COWcode_b, leader_name_a, leader_tenure_years_a,
               leader_name_b, leader_tenure_years_b) %>%
        head(10) %>%
        print()

message("\n✓ Leader data successfully merged to dyadic dataset!")
