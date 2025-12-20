library(tidyverse)
library(dplyr)
library(tidyr)
library(haven) # For reading Stata (.dta) files
library(WDI)
library(zoo)   # For interpolation
library(countrycode)
library(here)  # For project-relative paths
library(readr)


## load V-Dem/ERT data into monadic panel named main_data

main_data <- read_csv(here("ready_data","vdem_ert_combined_panel_s.csv"))

main_data <- main_data %>%
        filter(year >= 1946)

# rename co_wcode to COWcode

main_data <- main_data %>%
        rename(COWcode = co_wcode)

# load directed dyad MIDS data into dataframe named mids 

MIDS <- read_csv(here("raw-data","dyadic_mid.csv"))

MIDS <- MIDS %>%
        filter(year >= 1946 & year <= 2020)


# convert MIDS to monadic data


library(dplyr)
library(tidyr)

# -------------------------------------------------------------------------
# STEP 1: CLEAN DYADIC DATA FIRST
# -------------------------------------------------------------------------

mids_clean <- MIDS %>%
        mutate(
                # CORRECTION: Using 'hihosta' as the variable name
                # Convert -9 (Missing) to NA to prevent calculation errors
                hihosta_clean = ifelse(hihosta == -9, NA, hihosta),
                
                # Convert -9 (Missing) to NA for Fatality
                fatlev_clean = ifelse(fatlev == -9, NA, fatlev)
        )

# -------------------------------------------------------------------------
# STEP 2: AGGREGATE TO MONADIC (COUNTRY-YEAR)
# -------------------------------------------------------------------------

mids_monadic <- mids_clean %>%
        # Group by the Focal Country (statea) and Year
        group_by(statea, year) %>%
        summarize(
                # --- FREQUENCY ---
                mid_count_total = n(),
                
                # --- DIRECTIONALITY (Conflict Export) ---
                # dyad_rolea == 1: Primary Initiator
                mid_count_initiated = sum(dyad_rolea == 1, na.rm = TRUE),
                mid_count_targeted = sum(dyad_rolea == 3, na.rm = TRUE),
                
                # --- INTENSITY (Hostility) ---
                # Max hostility reached by State A specifically (using corrected variable)
                mid_max_hostility = max(hihosta_clean, na.rm = TRUE),
                
                # --- FATALITY (Robust Handling) ---
                
                # Feature A: Did they experience a high-fatality event? (Binary)
                # Checks if ANY dispute that year reached level 4, 5, or 6 (>250 deaths)
                mid_high_fatality_event = max(ifelse(fatlev_clean >= 4, 1, 0), na.rm = TRUE),
                
                # Feature B: The "Deadliness" Ceiling
                # What was the highest category of fatality experienced this year?
                mid_max_fatality_cat = max(fatlev_clean, na.rm = TRUE),
                
                # Feature C: Fatal Dispute Count
                # How many disputes involved *at least* some deaths (Level > 0)?
                mid_count_fatal_disputes = sum(fatlev_clean > 0, na.rm = TRUE),
                
                .groups = "drop"
        ) %>%
        
        # -----------------------------------------------------------------------
# STEP 3: CLEAN UP INFINITIES AND NAs
# -----------------------------------------------------------------------
mutate(
        # Fix max() warnings where all inputs were NA (returns -Inf)
        mid_max_hostility = ifelse(is.infinite(mid_max_hostility), NA, mid_max_hostility),
        mid_max_fatality_cat = ifelse(is.infinite(mid_max_fatality_cat), NA, mid_max_fatality_cat),
        mid_high_fatality_event = ifelse(is.infinite(mid_high_fatality_event), 0, mid_high_fatality_event)
)

# -------------------------------------------------------------------------
# STEP 4: MERGE WITH MAIN V-DEM DATA
# -------------------------------------------------------------------------

final_dataset <- main_data %>%
        left_join(mids_monadic, by = c("COWcode" = "statea", "year" = "year")) %>%
        
        # Replace NAs with 0 for the COUNT variables (Peace Years)
        mutate(
                mid_count_total = replace_na(mid_count_total, 0),
                mid_count_initiated = replace_na(mid_count_initiated, 0),
                mid_count_targeted = replace_na(mid_count_targeted, 0),
                mid_count_fatal_disputes = replace_na(mid_count_fatal_disputes, 0),
                mid_high_fatality_event = replace_na(mid_high_fatality_event, 0),
                
                # For categorical maxes, if NA (Peace), set to lowest level
                mid_max_fatality_cat = replace_na(mid_max_fatality_cat, 0), 
                mid_max_hostility = replace_na(mid_max_hostility, 1) # 1 = No militarized action
        )

# Inspection
summary(final_dataset$mid_max_hostility)


# Add ALBA membership

# -------------------------------------------------------------------------
# STEP 1: DEFINE ALBA MEMBERSHIP DATA
# -------------------------------------------------------------------------
# Since ALBA membership is small, hardcoding a lookup table is safer 
# and faster than trying to merge an external CSV.
# Dates based on official accession/withdrawal.

alba_roster <- tribble(
        ~COWcode, ~start_year, ~end_year, ~country_name,
        101,      2004,        9999,      "Venezuela",
        40,       2004,        9999,      "Cuba",
        145,      2006,        2019,      "Bolivia",      # Withdrew after 2019 crisis (Jeanine Áñez)
        93,       2007,        9999,      "Nicaragua",
        53,       2008,        9999,      "Dominica",
        91,       2008,        2010,      "Honduras",     # Withdrew Jan 2010 after 2009 coup
        130,      2009,        2018,      "Ecuador",      # Withdrew Aug 2018 (Lenín Moreno)
        58,       2009,        9999,      "Antigua & Barbuda",
        56,       2009,        9999,      "St. Vincent & Grenadines",
        57,       2013,        9999,      "St. Lucia",
        55,       2014,        9999,      "Grenada",
        60,       2014,        9999,      "St. Kitts & Nevis"
        # Note: Suriname and Haiti are often "observers" or "special guests" 
        # but usually not considered full members for treaty purposes. 
        # Adjust above if your definition differs.
)

# -------------------------------------------------------------------------
# STEP 2: APPLY LOGIC TO MAIN DATASET
# -------------------------------------------------------------------------

main_data_alba <- main_data %>%
        # Perform a Left Join to attach ALBA dates to the matching countries
        left_join(alba_roster %>% select(COWcode, start_year, end_year), by = "COWcode") %>%
        
        mutate(
                # 1. DEFINE LATIN AMERICA & CARIBBEAN (LAC) REGION
                # COW codes 2-199 are Americas.
                # We exclude 2 (USA) and 20 (Canada).
                is_lac_region = (COWcode > 20 & COWcode < 200),
                
                # 2. DEFINE ELIGIBILITY (The "Possibility" Variable)
                # A country is "Eligible" (in the risk set) ONLY if:
                # A) It is in the LAC region
                # B) The year is 2004 or later (ALBA didn't exist before)
                alba_possible = ifelse(is_lac_region & year >= 2004, 1, 0),
                
                # 3. DEFINE TREATMENT (Membership)
                # Logic:
                # - Must have a start_year (meaning they matched the roster)
                # - Current year must be >= start_year
                # - Current year must be <= end_year (handles Honduras/Ecuador/Bolivia exits)
                # - Handling NAs: If start_year is NA, they are not members.
                alba_member = case_when(
                        is.na(start_year) ~ 0,           # Not in the roster -> 0
                        year >= start_year & year <= end_year ~ 1, # Active member
                        TRUE ~ 0                         # Before joining or after leaving -> 0
                )
        ) %>%
        
        # Cleanup auxiliary columns
        select(-start_year, -end_year, -is_lac_region)

# -------------------------------------------------------------------------
# CHECKS
# -------------------------------------------------------------------------
# Check 1: Non-LAC countries should be 0 for both
# Should return 0 rows
main_data_alba %>% 
        filter(COWcode >= 200 & (alba_member == 1 | alba_possible == 1)) %>%
        select(COWcode, year, alba_member, alba_possible)

# Check 2: Pre-2004 should be 0 for both
# Should return 0 rows
main_data_alba %>% 
        filter(year < 2004 & (alba_member == 1 | alba_possible == 1)) %>% 
        select(COWcode, year, alba_member, alba_possible)

# Check 3: Verify Honduras (Joined 2008, Left 2010)
main_data_alba %>% 
        filter(COWcode == 91 & year >= 2007 & year <= 2011) %>%
        select(COWcode, year, alba_member, alba_possible)


## Save this ave-m.csv

write_csv(main_data_alba, here("ready_data","ave-m.csv"))

## Save alba_roster as alba_roster.csv

write_csv(alba_roster, here("ready_data","alba_roster.csv"))


# load trade data from RED dataset

RED <- read_csv(here("raw-data","RED_full_final.csv"))

RED <- RED %>%
        filter(year >= 1946 & year <= 2020)



# -------------------------------------------------------------------------
# MODULE: RED DYADIC TO MONADIC TRANSFORMATION
# -------------------------------------------------------------------------

# 1. SETUP
# Define the COW codes for Strategic Partners
# USA: 2, China: 710, Venezuela: 101
strategic_partners <- c(2, 710, 101)

# Define ALBA Bloc (excluding Venezuela for separate tracking if desired, 
# or keep generic). Here we list the core members.
alba_bloc_codes <- c(40, 145, 93, 53, 91, 130, 58, 56, 57, 55, 60)

# 2. AGGREGATION
# We group by 'exporter_cow' because the RED dataset is directed.
# Row A->B contains A's export importance AND A's import importance regarding B.
# So 'exporter_cow' is effectively the "Focal Country".

red_monadic <- RED %>%
        group_by(exporter_cow, year) %>%
        summarize(
                # --- A. EXPORT DEPENDENCE (Market Vulnerability) ---
                # "If I lose this buyer, does my economy crash?"
                
                # 1. Structural Vulnerability (HHI)
                # Formula: Sum of (Share)^2. 
                # RED is 0-100, so we divide by 100 to get 0-1 range before squaring.
                # Result: 0 (Perfectly Diversified) to 1 (Monopsony Dependence).
                trade_export_hhi = sum((RED_export_importance / 100)^2, na.rm = TRUE),
                
                # 2. Strategic Dependence (Specific Partners)
                exp_dep_usa = sum(RED_export_importance[importer_cow == 2], na.rm = TRUE),
                exp_dep_china = sum(RED_export_importance[importer_cow == 710], na.rm = TRUE),
                exp_dep_venezuela = sum(RED_export_importance[importer_cow == 101], na.rm = TRUE),
                exp_dep_alba_bloc = sum(RED_export_importance[importer_cow %in% alba_bloc_codes], na.rm = TRUE),
                
                # --- B. IMPORT DEPENDENCE (Supply Vulnerability) ---
                # "If I lose this supplier, do I lose my energy/food?" (Crucial for Petrocaribe)
                
                # 1. Structural Vulnerability (HHI)
                trade_import_hhi = sum((RED_import_importance / 100)^2, na.rm = TRUE),
                
                # 2. Strategic Dependence (Specific Partners)
                # Note: High dependence on Ven here is the "Oil Leverage" signal.
                imp_dep_usa = sum(RED_import_importance[importer_cow == 2], na.rm = TRUE),
                imp_dep_china = sum(RED_import_importance[importer_cow == 710], na.rm = TRUE),
                imp_dep_venezuela = sum(RED_import_importance[importer_cow == 101], na.rm = TRUE),
                imp_dep_alba_bloc = sum(RED_import_importance[importer_cow %in% alba_bloc_codes], na.rm = TRUE),
                
                .groups = "drop"
        )

# 3. MERGE WITH MAIN DATASET
# Merge into your master 'main_data_alba' frame.
final_dataset <- main_data_alba %>%
        left_join(red_monadic, by = c("COWcode" = "exporter_cow", "year" = "year")) %>%
        
        # 4. HANDLING MISSING DATA
        # Unlike MIDS, missing RED data usually means "No Report," not "Zero".
        # We leave them as NA so MICE can impute them based on Region/GDP/Year.
        # However, if you prefer to assume missing = 0 trade (risky but common),
        # uncomment the mutate block below.
        
        # I recommend leaving as NA for the Transformer workflow.
        identity() 

# Check the Venezuela Import Dependence (Key for Autocracy Promotion theory)
summary(final_dataset$imp_dep_venezuela)

# save final_dataset as rave-m.csv

write_csv(final_dataset, here("ready_data","rave-m.csv"))


# load alliance data from ATOP dataset

ATOP_ddyr <- read_csv(here("raw-data","atop5_1ddyr_NNA.csv"))


# correct name so next step works

final_data <- final_dataset

# -------------------------------------------------------------------------
# MODULE: ATOP DYADIC TO MONADIC TRANSFORMATION
# -------------------------------------------------------------------------
# Prerequisites:
# 1. 'final_data' exists (Contains V-Dem + MIDS + ALBA + RED).
# 2. 'ATOP_ddyr' exists (Loaded from ATOP 5.0 Directed Dyad CSV).
# 3. 'COWcode' is the country key (CamelCase).
# 4. 'v2x_libdem' is the regime variable (snake_case).

# 1. SETUP: PREPARE PARTNER REGIME LOOKUP
# We pull this from 'final_data' to ensure we use the same regime definitions.
partner_regime_lookup <- final_data %>%
        select(COWcode, year, v2x_libdem) %>%
        mutate(
                # Define "Autocracy" vs "Democracy" for the weights.
                # v2x_libdem range is 0-1.
                # < 0.5 is the standard threshold for "Non-Liberal" (Autocracy/Anocracy).
                is_autocracy_partner = ifelse(v2x_libdem < 0.5, 1, 0),
                is_democracy_partner = ifelse(v2x_libdem >= 0.5, 1, 0)
        )

# 2. PREPARE ATOP DATA
atop_clean <- ATOP_ddyr %>%
        select(
                stateA,       # The Promisor (Focal Country)
                stateB,       # The Promisee (Partner)
                year,
                defense,      # Defense Pact (1=Yes)
                offense,      # Offense Pact (1=Yes) - Critical Signal for Aggression
                nonagg,       # Non-Aggression Pact (1=Yes)
                consul        # Consultation Pact (1=Yes)
        ) %>%
        # Filter to your study period (1946+) to match V-Dem
        filter(year >= 1946)

# 3. MERGE PARTNER REGIME INFO
# We join the lookup to 'stateB' (The Partner) using 'COWcode'
atop_weighted <- atop_clean %>%
        left_join(partner_regime_lookup, by = c("stateB" = "COWcode", "year" = "year"))

# 4. AGGREGATION (Partner-Weighted)
# We group by the Focal Country (stateA) and Year.
atop_monadic <- atop_weighted %>%
        group_by(stateA, year) %>%
        summarize(
                # --- DEFENSE PACTS (Protective Shield) ---
                atop_defense_total = sum(defense, na.rm = TRUE),
                # The "Autocratic Defense Network"
                atop_defense_with_auto = sum(defense * is_autocracy_partner, na.rm = TRUE),
                
                # --- OFFENSE PACTS (Aggressive Intent) ---
                atop_offense_total = sum(offense, na.rm = TRUE),
                # Explicit coordination with other autocrats for aggression
                atop_offense_with_auto = sum(offense * is_autocracy_partner, na.rm = TRUE),
                
                # --- NON-AGGRESSION PACTS (Strategic Clearance) ---
                atop_nonagg_total = sum(nonagg, na.rm = TRUE),
                atop_nonagg_with_auto = sum(nonagg * is_autocracy_partner, na.rm = TRUE),
                
                # --- CONSULTATION PACTS (Signaling) ---
                atop_consul_total = sum(consul, na.rm = TRUE),
                
                .groups = "drop"
        )

# 5. MERGE WITH FINAL DATASET
# Overwrite 'final_data' to include the ATOP variables.
final_data <- final_data %>%
        left_join(atop_monadic, by = c("COWcode" = "stateA", "year" = "year")) %>%
        
        # 6. MISSING DATA HANDLING
        # If a country is in V-Dem but not in ATOP for a given year, 
        # it implies they are an "Isolate" (no alliances). 
        # We code these NAs as 0.
        mutate(
                atop_defense_total = replace_na(atop_defense_total, 0),
                atop_defense_with_auto = replace_na(atop_defense_with_auto, 0),
                atop_offense_total = replace_na(atop_offense_total, 0),
                atop_offense_with_auto = replace_na(atop_offense_with_auto, 0),
                atop_nonagg_total = replace_na(atop_nonagg_total, 0),
                atop_nonagg_with_auto = replace_na(atop_nonagg_with_auto, 0),
                atop_consul_total = replace_na(atop_consul_total, 0)
        )

# Quick Inspection
summary(final_data$atop_defense_with_auto)

## Save final_data as rave-m2.csv


write_csv(final_data, here("ready_data","rave-m2.csv"))

# load Global Dataset on Political Leaders, 1945-2020
# data set is identifying_ideologues.dta a STATA file

GDPL <- haven::read_dta(here("raw-data","identifying_ideologues.dta"))




# -------------------------------------------------------------------------
# MODULE: GLI (Global Leader Ideology) INTEGRATION
# -------------------------------------------------------------------------
# Prerequisites:
# 1. 'final_data' exists (V-Dem + MIDS + ALBA + RED + ATOP).
# 2. 'GDPL' exists (Loaded from the Herre 2023 dataset).
# 3. Variable names match your report: 'country_code_cow', 'hog_ideology', 'leader_ideology'.

# 1. CLEAN AND STANDARDIZE GLI DATA
gli_clean <- GDPL %>%
        # Select and rename for clarity
        select(
                COWcode = country_code_cow,  # Rename to match master dataset
                year,
                leader_ideology_raw = leader_ideology,
                hog_ideology_raw = hog_ideology # Head of Govt (if different)
        ) %>%
        # Drop rows with missing COW codes (if any)
        filter(!is.na(COWcode)) %>%
        
        # 2. RECODE IDEOLOGY STRINGS TO NUMERIC FACTORS
        # Values: "leftist", "centrist", "rightist", "not applicable", "no information"
        mutate(
                # --- LEADER IDEOLOGY (Primary) ---
                gli_leader_ideology_num = case_when(
                        leader_ideology_raw == "leftist" ~ 1,
                        leader_ideology_raw == "centrist" ~ 2,
                        leader_ideology_raw == "rightist" ~ 3,
                        # Treat "not applicable" and "no information" as 0 (No Ideology/Missing)
                        leader_ideology_raw %in% c("not applicable", "no information") ~ 0,
                        TRUE ~ 0 # Fallback for NAs
                ),
                
                # --- HEAD OF GOVT IDEOLOGY (Secondary) ---
                gli_hog_ideology_num = case_when(
                        hog_ideology_raw == "leftist" ~ 1,
                        hog_ideology_raw == "centrist" ~ 2,
                        hog_ideology_raw == "rightist" ~ 3,
                        hog_ideology_raw %in% c("not applicable", "no information") ~ 0,
                        TRUE ~ 0
                ),
                
                # --- DUMMY VARIABLES (For Interaction Terms) ---
                # Useful if you want to test "Leftist Leader" explicitly in the model
                is_leftist_leader = ifelse(leader_ideology_raw == "leftist", 1, 0),
                is_rightist_leader = ifelse(leader_ideology_raw == "rightist", 1, 0),
                is_centrist_leader = ifelse(leader_ideology_raw == "centrist", 1, 0)
        )

# 3. MERGE WITH FINAL DATASET
# Overwrite 'final_data' to include GLI variables.
final_data <- final_data %>%
        left_join(gli_clean, by = c("COWcode", "year"))

# 4. FINAL CLEANUP
# If GLI didn't cover a year (e.g. pre-1945), these will be NA.
# We explicitly code them as 0 (No Info) to allow the model to run without crashing.
final_data <- final_data %>%
        mutate(
                gli_leader_ideology_num = replace_na(gli_leader_ideology_num, 0),
                gli_hog_ideology_num = replace_na(gli_hog_ideology_num, 0),
                is_leftist_leader = replace_na(is_leftist_leader, 0),
                is_rightist_leader = replace_na(is_rightist_leader, 0),
                is_centrist_leader = replace_na(is_centrist_leader, 0)
        )

# Quick Inspection
table(final_data$gli_leader_ideology_num)


# save data as grave-m.csv

write_csv(final_data, here("ready_data","grave-m.csv"))




# -------------------------------------------------------------------------
# HELPER FUNCTIONS: ROBUST JOINS
# -------------------------------------------------------------------------
# These functions check the types of the 'by' variables in both dataframes.
# If they differ (e.g., numeric vs character), they coerce 'y' to match 'x'.

coerce_join_keys <- function(x, y, by) {
        # Handle named 'by' vectors (e.g., c("a" = "b"))
        by_x <- if (is.null(names(by))) by else names(by)
        by_x[by_x == ""] <- by[by_x == ""] # Fill in unnamed elements
        
        by_y <- if (is.null(names(by))) by else unname(by)
        
        for (i in seq_along(by_x)) {
                col_x <- by_x[i]
                col_y <- by_y[i]
                
                if (col_x %in% names(x) && col_y %in% names(y)) {
                        cls_x <- class(x[[col_x]])[1]
                        cls_y <- class(y[[col_y]])[1]
                        
                        if (cls_x != cls_y) {
                                message(sprintf("Robust Join: Coercing y$%s (%s) to match x$%s (%s)", 
                                                col_y, cls_y, col_x, cls_x))
                                if (cls_x %in% c("numeric", "integer", "double")) {
                                        y[[col_y]] <- as.numeric(y[[col_y]])
                                } else if (cls_x == "character") {
                                        y[[col_y]] <- as.character(y[[col_y]])
                                } else {
                                        # Fallback for factors or other types
                                        class(y[[col_y]]) <- cls_x
                                }
                        }
                }
        }
        return(y)
}

robust_left_join <- function(x, y, by = NULL, ...) {
        y_mod <- coerce_join_keys(x, y, by)
        dplyr::left_join(x, y_mod, by = by, ...)
}

robust_right_join <- function(x, y, by = NULL, ...) {
        y_mod <- coerce_join_keys(x, y, by)
        dplyr::right_join(x, y_mod, by = by, ...)
}

robust_inner_join <- function(x, y, by = NULL, ...) {
        y_mod <- coerce_join_keys(x, y, by)
        dplyr::inner_join(x, y_mod, by = by, ...)
}

robust_full_join <- function(x, y, by = NULL, ...) {
        y_mod <- coerce_join_keys(x, y, by)
        dplyr::full_join(x, y_mod, by = by, ...)
}

# -------------------------------------------------------------------------
# MODULE: ECONOMIC & GOVERNANCE INTEGRATION
# -------------------------------------------------------------------------
# Prerequisites:
# 1. 'final_data' exists (V-Dem/MIDS/ALBA/RED/ATOP/GLI).
# 2. Raw files in 'raw-data' subfolder.

# -------------------------------------------------------------------------
# 1. MADDISON PROJECT (Historical Baseline)
# -------------------------------------------------------------------------
# Load Maddison Stata file from raw-data
maddison_raw <- read_dta(here("raw-data", "maddison2023_web.dta"))

maddison_clean <- maddison_raw %>%
        mutate(
                COWcode = countrycode(
                        countrycode, 
                        "iso3c", 
                        "cown",
                        custom_match = c(
                                "CSK" = 315, # Czechoslovakia
                                "SUN" = 365, # Soviet Union
                                "YUG" = 345, # Yugoslavia
                                "SRB" = 345  # Serbia
                        )
                ),
                pop_raw = pop * 1000
        ) %>%
        mutate(COWcode = as.numeric(COWcode)) %>% # Force numeric to prevent join errors
        filter(!is.na(COWcode)) %>%
        # AGGREGATE to ensure uniqueness (avoids many-to-many errors)
        group_by(COWcode, year) %>%
        summarise(
                maddison_gdp_pc = mean(gdppc, na.rm = TRUE),
                maddison_pop = mean(pop_raw, na.rm = TRUE),
                .groups = "drop"
        )

# -------------------------------------------------------------------------
# 2. WDI & WGI (Modern Indicators & Governance)
# -------------------------------------------------------------------------
# Fetch WDI/WGI from 1960-2024
indicators_econ <- c(
        "gdp_pc" = "NY.GDP.PCAP.PP.KD",       # GDP pc PPP (Constant 2017)
        "pop" = "SP.POP.TOT",                 # Total Population
        "resource_rents" = "NY.GDP.TOTL.RT.ZS" # Natural Resource Rents %
)

indicators_gov <- c(
        "corruption_control" = "CC.EST",      # WGI Control of Corruption
        "govt_effectiveness" = "GE.EST"       # WGI Govt Effectiveness
)

# Helper function to fetch in batches safely
fetch_wdi_batched <- function(indicators, start_year, end_year) {
        all_iso2 <- unique(WDI::WDI_data$country$iso2c)
        all_iso2 <- all_iso2[!is.na(all_iso2) & all_iso2 != ""]
        chunks <- split(all_iso2, ceiling(seq_along(all_iso2) / 50))
        
        results_list <- list()
        for (i in seq_along(chunks)) {
                tryCatch({
                        dat <- WDI(indicator = indicators, country = chunks[[i]], 
                                   start = start_year, end = end_year, extra = FALSE)
                        results_list[[i]] <- dat
                        Sys.sleep(0.5)
                }, error = function(e) warning(paste("Batch", i, "failed:", e$message)))
        }
        bind_rows(results_list)
}

# Execute Batched Downloads
wdi_econ <- fetch_wdi_batched(indicators_econ, 1960, 2024)

# Robust renaming
names(wdi_econ)[names(wdi_econ) == "NY.GDP.PCAP.PP.KD"] <- "gdp_pc"
names(wdi_econ)[names(wdi_econ) == "SP.POP.TOT"] <- "pop"
names(wdi_econ)[names(wdi_econ) == "NY.GDP.TOTL.RT.ZS"] <- "resource_rents"
if (!"gdp_pc" %in% names(wdi_econ)) wdi_econ$gdp_pc <- NA
if (!"pop" %in% names(wdi_econ)) wdi_econ$pop <- NA
if (!"resource_rents" %in% names(wdi_econ)) wdi_econ$resource_rents <- NA

wdi_gov  <- fetch_wdi_batched(indicators_gov, 1960, 2024)

names(wdi_gov)[names(wdi_gov) == "CC.EST"] <- "corruption_control"
names(wdi_gov)[names(wdi_gov) == "GE.EST"] <- "govt_effectiveness"
if (!"corruption_control" %in% names(wdi_gov)) wdi_gov$corruption_control <- NA
if (!"govt_effectiveness" %in% names(wdi_gov)) wdi_gov$govt_effectiveness <- NA

wdi_raw <- wdi_econ %>%
        left_join(wdi_gov %>% select(iso2c, year, corruption_control, govt_effectiveness), 
                  by = c("iso2c", "year"))

wdi_clean <- wdi_raw %>%
        mutate(
                COWcode = countrycode(
                        iso2c, 
                        "iso2c", 
                        "cown",
                        custom_match = c("CS" = 315, "SU" = 365, "YU" = 345, "RS" = 345)
                )
        ) %>%
        mutate(COWcode = as.numeric(COWcode)) %>% # Force numeric
        filter(!is.na(COWcode)) %>%
        # AGGREGATE to handle cases where multiple ISO codes map to one COW code
        group_by(COWcode, year) %>%
        summarise(
                wdi_gdp_pc = mean(gdp_pc, na.rm = TRUE),
                wdi_pop = sum(pop, na.rm = TRUE), # Sum pop if split, or mean if dup. Sum is safer for fragments.
                resource_rents = mean(resource_rents, na.rm = TRUE),
                corruption_control = mean(corruption_control, na.rm = TRUE),
                govt_effectiveness = mean(govt_effectiveness, na.rm = TRUE),
                .groups = "drop"
        ) %>%
        arrange(COWcode, year) %>%
        # Interpolate AFTER aggregation to ensure time series continuity per COWcode
        mutate(
                corruption_control = na.approx(corruption_control, x = year, rule = 2, na.rm = FALSE),
                govt_effectiveness = na.approx(govt_effectiveness, x = year, rule = 2, na.rm = FALSE)
        )

# -------------------------------------------------------------------------
# 3. ROSS OIL & GAS (Historical Rents)
# -------------------------------------------------------------------------
ross_raw <- read.csv(here("raw-data", "ross_oil_gas.csv"))

ross_clean <- ross_raw %>%
        select(COWcode = id, year, oil_gas_pop = oil_gas_valuePOP_2014) %>%
        mutate(
                COWcode = as.numeric(COWcode), # Force numeric
                is_petro_state_ross = ifelse(oil_gas_pop > 100, 1, 0)
        ) %>%
        filter(!is.na(COWcode)) %>%
        # Ensure uniqueness
        distinct(COWcode, year, .keep_all = TRUE)

# -------------------------------------------------------------------------
# 4. SWIID (Inequality)
# -------------------------------------------------------------------------
swiid_raw <- read.csv(here("raw-data", "swiid_summary.csv"))

swiid_clean <- swiid_raw %>%
        mutate(
                COWcode = countrycode(
                        country, 
                        "country.name", 
                        "cown",
                        custom_match = c(
                                "Serbia" = 345, "Micronesia" = 987,
                                "Anguilla" = NA, "Greenland" = NA, "Hong Kong" = NA,
                                "Palestinian Territories" = NA, "Puerto Rico" = NA, 
                                "Turks and Caicos Islands" = NA
                        )
                )
        ) %>%
        mutate(COWcode = as.numeric(COWcode)) %>% # Force numeric
        filter(!is.na(COWcode)) %>%
        # AGGREGATE duplicates (e.g. Serbia/Yugoslavia overlap)
        group_by(COWcode, year) %>%
        summarise(gini_disp = mean(gini_disp, na.rm = TRUE), .groups = "drop")

# -------------------------------------------------------------------------
# 5. MASTER MERGE & UNIFICATION (Using Robust Joins)
# -------------------------------------------------------------------------

final_data_complete <- final_data %>%
        # Aggregation steps above ensure uniqueness, preventing many-to-many errors.
        robust_left_join(maddison_clean, by = c("COWcode", "year")) %>%
        robust_left_join(wdi_clean, by = c("COWcode", "year")) %>%
        robust_left_join(ross_clean, by = c("COWcode", "year")) %>%
        robust_left_join(swiid_clean, by = c("COWcode", "year")) %>%
        
        # CONSTRUCT UNIFIED VARIABLES
        mutate(
                # A. UNIFIED GDP PC
                unified_gdp_pc = ifelse(!is.na(wdi_gdp_pc), wdi_gdp_pc, maddison_gdp_pc),
                log_gdp_pc = log(unified_gdp_pc),
                
                # B. UNIFIED POPULATION
                unified_pop = ifelse(!is.na(wdi_pop), wdi_pop, maddison_pop),
                log_pop = log(unified_pop),
                
                # C. UNIFIED PETRO-STATE DUMMY
                is_petro_state = case_when(
                        !is.na(is_petro_state_ross) ~ is_petro_state_ross,
                        !is.na(resource_rents) & resource_rents > 10 ~ 1,
                        !is.na(resource_rents) & resource_rents <= 10 ~ 0,
                        TRUE ~ 0
                ),
                
                # D. CONTINUOUS RESOURCE WEALTH
                log_oil_gas_wealth = log(oil_gas_pop + 1)
        ) %>%
        select(-maddison_gdp_pc, -maddison_pop, -wdi_gdp_pc, -wdi_pop, 
               -is_petro_state_ross, -oil_gas_pop)

# -------------------------------------------------------------------------
# 6. EXPORT FINAL DATASET
# -------------------------------------------------------------------------
# Ensure ready_data directory exists (optional safety check)
if (!dir.exists(here("ready_data"))) dir.create(here("ready_data"))

write.csv(final_data_complete, here("ready_data", "GRAVE_M_Master_Dataset_Final_v3.csv"), row.names = FALSE)

summary(final_data_complete %>% filter(year < 1960) %>% select(log_gdp_pc))