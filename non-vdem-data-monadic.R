# -------------------------------------------------------------------------
# GRAVE-M Master Data Pipeline
# Author: Tom Hanna
# Description: Merges V-Dem, MIDS, ALBA, RED, ATOP, GLI, and Econ/Gov Data
# -------------------------------------------------------------------------

library(tidyverse)
library(dplyr)
library(tidyr)
library(haven)       # For reading Stata (.dta) files
library(WDI)         # World Development Indicators
library(zoo)         # For interpolation
library(countrycode) # For standardized country codes
library(here)        # For project-relative paths
library(readr)       # For reading CSVs

# Ensure output directory exists
if (!dir.exists(here("ready_data"))) dir.create(here("ready_data"))

# -------------------------------------------------------------------------
# LOAD V-DEM/ERT DATA (Monadic Panel)
# -------------------------------------------------------------------------

main_data <- read_csv(here("ready_data", "vdem_ert_combined_panel_s.csv"))

main_data <- main_data %>%
        filter(year >= 1946) %>%
        rename(COWcode = co_wcode) # rename co_wcode to COWcode

# -------------------------------------------------------------------------
# LOAD & PROCESS MIDS (Directed Dyad -> Monadic)
# -------------------------------------------------------------------------

MIDS <- read_csv(here("raw-data", "dyadic_mid.csv"))

MIDS <- MIDS %>%
        filter(year >= 1946 & year <= 2020)

# STEP 1: CLEAN DYADIC DATA FIRST
mids_clean <- MIDS %>%
        mutate(
                # CORRECTION: Using 'hihosta' as the variable name
                # Convert -9 (Missing) to NA to prevent calculation errors
                hihosta_clean = ifelse(hihosta == -9, NA, hihosta),
                
                # Convert -9 (Missing) to NA for Fatality
                fatlev_clean = ifelse(fatlev == -9, NA, fatlev)
        )

# STEP 2: AGGREGATE TO MONADIC (COUNTRY-YEAR)
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
                # Max hostility reached by State A specifically
                mid_max_hostility = max(hihosta_clean, na.rm = TRUE),
                
                # --- FATALITY (Robust Handling) ---
                # Feature A: Did they experience a high-fatality event? (Binary)
                mid_high_fatality_event = max(ifelse(fatlev_clean >= 4, 1, 0), na.rm = TRUE),
                
                # Feature B: The "Deadliness" Ceiling
                mid_max_fatality_cat = max(fatlev_clean, na.rm = TRUE),
                
                # Feature C: Fatal Dispute Count
                mid_count_fatal_disputes = sum(fatlev_clean > 0, na.rm = TRUE),
                
                .groups = "drop"
        ) %>%
        # STEP 3: CLEAN UP INFINITIES AND NAs
        mutate(
                # Fix max() warnings where all inputs were NA (returns -Inf)
                mid_max_hostility = ifelse(is.infinite(mid_max_hostility), NA, mid_max_hostility),
                mid_max_fatality_cat = ifelse(is.infinite(mid_max_fatality_cat), NA, mid_max_fatality_cat),
                mid_high_fatality_event = ifelse(is.infinite(mid_high_fatality_event), 0, mid_high_fatality_event)
        )

# STEP 4: MERGE WITH MAIN V-DEM DATA
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

# -------------------------------------------------------------------------
# ALBA MEMBERSHIP MODULE
# -------------------------------------------------------------------------

# STEP 1: DEFINE ALBA MEMBERSHIP DATA
alba_roster <- tribble(
        ~COWcode, ~start_year, ~end_year, ~country_name,
        101,      2004,        9999,      "Venezuela",
        40,       2004,        9999,      "Cuba",
        145,      2006,        2019,      "Bolivia",      # Withdrew after 2019 crisis
        93,       2007,        9999,      "Nicaragua",
        53,       2008,        9999,      "Dominica",
        91,       2008,        2010,      "Honduras",     # Withdrew Jan 2010
        130,      2009,        2018,      "Ecuador",      # Withdrew Aug 2018
        58,       2009,        9999,      "Antigua & Barbuda",
        56,       2009,        9999,      "St. Vincent & Grenadines",
        57,       2013,        9999,      "St. Lucia",
        55,       2014,        9999,      "Grenada",
        60,       2014,        9999,      "St. Kitts & Nevis"
)

# STEP 2: APPLY LOGIC TO MAIN DATASET
main_data_alba <- final_dataset %>%
        left_join(alba_roster %>% select(COWcode, start_year, end_year), by = "COWcode") %>%
        mutate(
                # 1. DEFINE LATIN AMERICA & CARIBBEAN (LAC) REGION
                # COW codes 2-199 are Americas. Exclude 2 (USA) and 20 (Canada).
                is_lac_region = (COWcode > 20 & COWcode < 200),
                
                # 2. DEFINE ELIGIBILITY
                alba_possible = ifelse(is_lac_region & year >= 2004, 1, 0),
                
                # 3. DEFINE TREATMENT (Membership)
                alba_member = case_when(
                        is.na(start_year) ~ 0,
                        year >= start_year & year <= end_year ~ 1,
                        TRUE ~ 0
                )
        ) %>%
        select(-start_year, -end_year, -is_lac_region)

# CHECKS
# Check 1: Non-LAC countries should be 0 (Should return 0 rows)
main_data_alba %>% 
        filter(COWcode >= 200 & (alba_member == 1 | alba_possible == 1)) %>%
        select(COWcode, year, alba_member, alba_possible)

# Check 2: Pre-2004 should be 0 (Should return 0 rows)
main_data_alba %>% 
        filter(year < 2004 & (alba_member == 1 | alba_possible == 1)) %>% 
        select(COWcode, year, alba_member, alba_possible)

# Save intermediate files
write_csv(main_data_alba, here("ready_data", "ave-m.csv"))
write_csv(alba_roster, here("ready_data", "alba_roster.csv"))

# -------------------------------------------------------------------------
# RED (TRADE) MODULE: DYADIC TO MONADIC
# -------------------------------------------------------------------------

RED <- read_csv(here("raw-data", "RED_full_final.csv"))
RED <- RED %>% filter(year >= 1946 & year <= 2020)

# 1. SETUP
strategic_partners <- c(2, 710, 101) # USA, China, Venezuela
alba_bloc_codes <- c(40, 145, 93, 53, 91, 130, 58, 56, 57, 55, 60)

# 2. AGGREGATION
red_monadic <- RED %>%
        group_by(exporter_cow, year) %>%
        summarize(
                # --- A. EXPORT DEPENDENCE (Market Vulnerability) ---
                # Structural Vulnerability (HHI)
                trade_export_hhi = sum((RED_export_importance / 100)^2, na.rm = TRUE),
                
                # Strategic Dependence
                exp_dep_usa = sum(RED_export_importance[importer_cow == 2], na.rm = TRUE),
                exp_dep_china = sum(RED_export_importance[importer_cow == 710], na.rm = TRUE),
                exp_dep_venezuela = sum(RED_export_importance[importer_cow == 101], na.rm = TRUE),
                exp_dep_alba_bloc = sum(RED_export_importance[importer_cow %in% alba_bloc_codes], na.rm = TRUE),
                
                # --- B. IMPORT DEPENDENCE (Supply Vulnerability) ---
                # Structural Vulnerability (HHI)
                trade_import_hhi = sum((RED_import_importance / 100)^2, na.rm = TRUE),
                
                # Strategic Dependence
                imp_dep_usa = sum(RED_import_importance[importer_cow == 2], na.rm = TRUE),
                imp_dep_china = sum(RED_import_importance[importer_cow == 710], na.rm = TRUE),
                imp_dep_venezuela = sum(RED_import_importance[importer_cow == 101], na.rm = TRUE),
                imp_dep_alba_bloc = sum(RED_import_importance[importer_cow %in% alba_bloc_codes], na.rm = TRUE),
                
                .groups = "drop"
        )

# 3. MERGE WITH MAIN DATASET
final_dataset <- main_data_alba %>%
        left_join(red_monadic, by = c("COWcode" = "exporter_cow", "year" = "year"))

# Check Venezuela Import Dependence
summary(final_dataset$imp_dep_venezuela)

# Save intermediate
write_csv(final_dataset, here("ready_data", "rave-m.csv"))

# -------------------------------------------------------------------------
# ATOP ALLIANCES MODULE
# -------------------------------------------------------------------------

ATOP_ddyr <- read_csv(here("raw-data", "atop5_1ddyr_NNA.csv"))
final_data <- final_dataset # Update working object name

# 1. SETUP: PREPARE PARTNER REGIME LOOKUP
partner_regime_lookup <- final_data %>%
        select(COWcode, year, v2x_libdem) %>%
        mutate(
                # v2x_libdem < 0.5 is threshold for "Non-Liberal"
                is_autocracy_partner = ifelse(v2x_libdem < 0.5, 1, 0),
                is_democracy_partner = ifelse(v2x_libdem >= 0.5, 1, 0)
        )

# 2. PREPARE ATOP DATA
atop_clean <- ATOP_ddyr %>%
        select(
                stateA, stateB, year, defense, offense, nonagg, consul
        ) %>%
        filter(year >= 1946)

# 3. MERGE PARTNER REGIME INFO
atop_weighted <- atop_clean %>%
        left_join(partner_regime_lookup, by = c("stateB" = "COWcode", "year" = "year"))

# 4. AGGREGATION (Partner-Weighted)
atop_monadic <- atop_weighted %>%
        group_by(stateA, year) %>%
        summarize(
                # --- DEFENSE PACTS ---
                atop_defense_total = sum(defense, na.rm = TRUE),
                atop_defense_with_auto = sum(defense * is_autocracy_partner, na.rm = TRUE),
                
                # --- OFFENSE PACTS ---
                atop_offense_total = sum(offense, na.rm = TRUE),
                atop_offense_with_auto = sum(offense * is_autocracy_partner, na.rm = TRUE),
                
                # --- NON-AGGRESSION PACTS ---
                atop_nonagg_total = sum(nonagg, na.rm = TRUE),
                atop_nonagg_with_auto = sum(nonagg * is_autocracy_partner, na.rm = TRUE),
                
                # --- CONSULTATION PACTS ---
                atop_consul_total = sum(consul, na.rm = TRUE),
                
                .groups = "drop"
        )

# 5. MERGE WITH FINAL DATASET
final_data <- final_data %>%
        left_join(atop_monadic, by = c("COWcode" = "stateA", "year" = "year")) %>%
        mutate(
                atop_defense_total = replace_na(atop_defense_total, 0),
                atop_defense_with_auto = replace_na(atop_defense_with_auto, 0),
                atop_offense_total = replace_na(atop_offense_total, 0),
                atop_offense_with_auto = replace_na(atop_offense_with_auto, 0),
                atop_nonagg_total = replace_na(atop_nonagg_total, 0),
                atop_nonagg_with_auto = replace_na(atop_nonagg_with_auto, 0),
                atop_consul_total = replace_na(atop_consul_total, 0)
        )

# Inspection
summary(final_data$atop_defense_with_auto)
write_csv(final_data, here("ready_data", "rave-m2.csv"))

# -------------------------------------------------------------------------
# GLI (Global Leader Ideology) MODULE
# -------------------------------------------------------------------------

GDPL <- haven::read_dta(here("raw-data", "identifying_ideologues.dta"))

gli_clean <- GDPL %>%
        select(
                COWcode = country_code_cow,
                year,
                leader_ideology_raw = leader_ideology,
                hog_ideology_raw = hog_ideology
        ) %>%
        filter(!is.na(COWcode)) %>%
        mutate(
                # --- LEADER IDEOLOGY ---
                gli_leader_ideology_num = case_when(
                        leader_ideology_raw == "leftist" ~ 1,
                        leader_ideology_raw == "centrist" ~ 2,
                        leader_ideology_raw == "rightist" ~ 3,
                        leader_ideology_raw %in% c("not applicable", "no information") ~ 0,
                        TRUE ~ 0
                ),
                # --- HEAD OF GOVT IDEOLOGY ---
                gli_hog_ideology_num = case_when(
                        hog_ideology_raw == "leftist" ~ 1,
                        hog_ideology_raw == "centrist" ~ 2,
                        hog_ideology_raw == "rightist" ~ 3,
                        hog_ideology_raw %in% c("not applicable", "no information") ~ 0,
                        TRUE ~ 0
                ),
                # --- DUMMY VARIABLES ---
                is_leftist_leader = ifelse(leader_ideology_raw == "leftist", 1, 0),
                is_rightist_leader = ifelse(leader_ideology_raw == "rightist", 1, 0),
                is_centrist_leader = ifelse(leader_ideology_raw == "centrist", 1, 0)
        )

# MERGE AND CLEANUP
final_data <- final_data %>%
        left_join(gli_clean, by = c("COWcode", "year")) %>%
        mutate(
                gli_leader_ideology_num = replace_na(gli_leader_ideology_num, 0),
                gli_hog_ideology_num = replace_na(gli_hog_ideology_num, 0),
                is_leftist_leader = replace_na(is_leftist_leader, 0),
                is_rightist_leader = replace_na(is_rightist_leader, 0),
                is_centrist_leader = replace_na(is_centrist_leader, 0)
        )

# Inspection
table(final_data$gli_leader_ideology_num)
write_csv(final_data, here("ready_data", "grave-m.csv"))

# -------------------------------------------------------------------------
# HELPER FUNCTIONS: ROBUST JOINS
# -------------------------------------------------------------------------
coerce_join_keys <- function(x, y, by) {
        by_x <- if (is.null(names(by))) by else names(by)
        by_x[by_x == ""] <- by[by_x == ""]
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

# -------------------------------------------------------------------------
# MODULE: ECONOMIC & GOVERNANCE INTEGRATION
# -------------------------------------------------------------------------

# 1. MADDISON PROJECT (Historical Baseline)
maddison_raw <- read_dta(here("raw-data", "maddison2023_web.dta"))
maddison_clean <- maddison_raw %>%
        mutate(
                COWcode = countrycode(countrycode, "iso3c", "cown",
                                      custom_match = c("CSK" = 315, "SUN" = 365, "YUG" = 345, "SRB" = 345)),
                pop_raw = pop * 1000
        ) %>%
        mutate(COWcode = as.numeric(COWcode)) %>%
        filter(!is.na(COWcode)) %>%
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

# 3. ROSS OIL & GAS
ross_raw <- read.csv(here("raw-data", "ross_oil_gas.csv"))
ross_clean <- ross_raw %>%
        mutate(
                COWcode = countrycode(cty_name, "country.name", "cown",
                                      custom_match = c("Czechoslovakia" = 315, "Soviet Union" = 365, "Yugoslavia" = 345,
                                                       "Serbia" = 345, "Serbia and Montenegro" = 345, 
                                                       "Yemen People's Republic" = 680, "Yemen Arab Republic" = 678,
                                                       "German Federal Republic" = 260, "German Democratic Republic" = 265))
        ) %>%
        mutate(COWcode = as.numeric(COWcode)) %>%
        select(COWcode, year, oil_gas_pop = oil_gas_valuePOP_2014) %>%
        mutate(is_petro_state_ross = ifelse(!is.na(oil_gas_pop) & oil_gas_pop > 100, 1, 0)) %>%
        filter(!is.na(COWcode)) %>%
        distinct(COWcode, year, .keep_all = TRUE)

# 4. SWIID (Inequality)
swiid_raw <- read.csv(here("raw-data", "swiid_summary.csv"))
swiid_clean <- swiid_raw %>%
        mutate(
                COWcode = countrycode(country, "country.name", "cown",
                                      custom_match = c("Serbia" = 345, "Micronesia" = 987, "Anguilla" = NA, "Greenland" = NA,
                                                       "Hong Kong" = NA, "Palestinian Territories" = NA, "Puerto Rico" = NA,
                                                       "Turks and Caicos Islands" = NA))
        ) %>%
        mutate(COWcode = as.numeric(COWcode)) %>%
        filter(!is.na(COWcode)) %>%
        group_by(COWcode, year) %>%
        summarise(gini_disp = mean(gini_disp, na.rm = TRUE), .groups = "drop")

# 4.5 FRASER INSTITUTE (Black Market Premium)
fraser_raw <- read.csv(here("raw-data", "black_market_exchange_rates.csv"))
if("black_market_exchange_rates" %in% names(fraser_raw)) {
        names(fraser_raw)[names(fraser_raw) == "black_market_exchange_rates"] <- "black_market_exchange_rate"
}

fraser_ready <- fraser_raw %>%
        mutate(
                COWcode = countrycode(iso_code, "iso3c", "cown",
                                      custom_match = c("CSK" = 315, "SUN" = 365, "YUG" = 345, "SRB" = 345))
        ) %>%
        mutate(COWcode = as.numeric(COWcode)) %>%
        filter(!is.na(COWcode)) %>%
        group_by(COWcode, year) %>%
        summarise(fraser_bmp_score = mean(black_market_exchange_rate, na.rm = TRUE), .groups = "drop")

# 5. MASTER MERGE & UNIFICATION
final_data_complete <- final_data %>%
        robust_left_join(maddison_clean, by = c("COWcode", "year")) %>%
        robust_left_join(wdi_clean, by = c("COWcode", "year")) %>%
        robust_left_join(ross_clean, by = c("COWcode", "year")) %>%
        robust_left_join(swiid_clean, by = c("COWcode", "year")) %>%
        robust_left_join(fraser_ready, by = c("COWcode", "year")) %>%
        mutate(
                # A. UNIFIED GDP PC
                unified_gdp_pc = ifelse(!is.na(wdi_gdp_pc), wdi_gdp_pc, maddison_gdp_pc),
                log_gdp_pc = log(unified_gdp_pc),
                
                # B. UNIFIED POPULATION
                unified_pop = ifelse(!is.na(wdi_pop), wdi_pop, maddison_pop),
                log_pop = log(unified_pop),
                
                # C. UNIFIED PETRO-STATE DUMMY
                # Priority: Ross (Better history) > WDI (Modern supplement)
                is_petro_state = case_when(
                        !is.na(is_petro_state_ross) ~ is_petro_state_ross,
                        !is.na(resource_rents) & resource_rents > 10 ~ 1,
                        !is.na(resource_rents) & resource_rents <= 10 ~ 0,
                        TRUE ~ 0
                ),
                
                # D. CONTINUOUS RESOURCE WEALTH
                log_oil_gas_wealth = log(oil_gas_pop + 1)
        )

# -------------------------------------------------------------------------
# MISSING DATA HANDLING MODULE (Fixes Top 20 Missing Items)
# -------------------------------------------------------------------------
final_data_complete <- final_data_complete %>%
        mutate(
                # 1. FIX ERT "STRUCTURAL" MISSINGNESS
                # These are only valid during active episodes. We create clean binaries.
                
                # Autocratization Episode Active?
                is_aut_episode = ifelse(!is.na(aut_ep_id), 1, 0),
                
                # Democratization Episode Active?
                is_dem_episode = ifelse(!is.na(dem_ep_id), 1, 0),
                
                # 2. FIX GOVERNANCE/CORRUPTION (The 1946-1995 WGI Gap)
                # If V-Dem Political Corruption (v2x_corr) exists, use it to backfill or replace WGI.
                # Logic: Use WGI if available (modern), else fill with V-Dem (scaled to approx match if needed, 
                # but here we just ensure we have *some* corruption measure).
                # Note: V-Dem is 0 (Clean) to 1 (Corrupt). WGI is -2.5 (Corrupt) to 2.5 (Clean).
                # We create a unified "Corruption Level" (Higher = More Corrupt)
                
                unified_corruption = case_when(
                        # Prefer V-Dem because it's consistent 1946-2020
                        "v2x_corr" %in% names(.) & !is.na(v2x_corr) ~ v2x_corr,
                        # Fallback to inverted WGI normalized (roughly) if V-Dem missing
                        !is.na(corruption_control) ~ (2.5 - corruption_control) / 5, 
                        TRUE ~ NA_real_
                )
        ) %>%
        # 3. EXPLICITLY DROP THE HIGH-MISSINGNESS ORIGINAL COLUMNS
        select(
                -maddison_gdp_pc, -maddison_pop, 
                -wdi_gdp_pc, -wdi_pop, 
                -is_petro_state_ross, -oil_gas_pop, 
                -starts_with("e_"),               # Remove raw V-Dem ordinals
                
                # Remove Structural ERT variables (replaced by binaries)
                -starts_with("aut_ep_"),          # aut_ep_id, aut_ep_start_year, etc.
                -starts_with("dem_ep_"),          # dem_ep_id, dem_ep_termination, etc.
                -contains("founding_elec"),       # aut_founding_elec, dem_founding_elec
                
                # Remove Raw WGI variables (replaced by unified_corruption)
                -corruption_control, 
                -govt_effectiveness
        )

# 6. EXPORT FINAL DATASET
write.csv(final_data_complete, here("ready_data", "GRAVE_M_Master_Dataset_Final_v3.csv"), row.names = FALSE)

# Final Summary Check
summary(final_data_complete %>% select(fraser_bmp_score, unified_corruption, is_aut_episode))

library(tidyverse)
library(zoo) # Essential for time series interpolation/LOCF

# 1. Load Data
# Assuming WRP_national.csv is in your working directory
wrp_raw <- read_csv(here("raw-data","WRP_national.csv"))

# 2. Define Variables of Interest
# Selecting the percentage variables likely relevant for GRAVE-M (e.g., Catholic/Protestant for LAC)
# You can add others from the raw CSV if needed (e.g., sunni/shia if relevant outside LAC)
wrp_selected <- wrp_raw %>%
        rename(COWcode = state) %>% # Rename to match GRAVE-M key
        select(
                year, 
                COWcode,
                chrstprotpct, # Protestant %
                chrstcatpct,  # Catholic %
                chrstorthpct, # Orthodox %
                chrstgenpct,  # Christian General %
                judgenpct,    # Jewish General %
                islmgenpct,   # Muslim General %
                budgenpct,    # Buddhist General %
                hindgenpct,   # Hindu General %
                nonreligpct,  # Non-Religious %
                sumreligpct   # Sum of all religions
        )

# 3. Create Target Time Grid
# WRP data is every 5 years (1945, 1950...). GRAVE-M is yearly (1946-2016).
# We create a full grid of all countries and all years to force R to recognize the missing years.
target_years <- 1946:2016
country_list <- unique(wrp_selected$COWcode)

full_grid <- expand_grid(COWcode = country_list, year = target_years)

# 4. Merge and Impute
wrp_imputed <- full_grid %>%
        left_join(wrp_selected, by = c("COWcode", "year")) %>%
        arrange(COWcode, year) %>%
        group_by(COWcode) %>%
        mutate(
                # Step A: Linear Interpolation for gaps (e.g., 1946-1949)
                # na.approx fills values between existing data points
                across(
                        ends_with("pct"), 
                        ~na.approx(., na.rm = FALSE) 
                ),
                # Step B: Last Observation Carried Forward for post-2010
                # na.locf fills trailing NAs with the last known value (2010 data)
                across(
                        ends_with("pct"), 
                        ~na.locf(., na.rm = FALSE) 
                )
        ) %>%
        ungroup()

# 5. Clean-up
# Some countries might have leading NAs if they didn't exist in 1945.
# We replace remaining NAs with 0 ONLY if appropriate, otherwise leave as NA.
# Here we filter to only the finished rows.
final_wrp_data <- wrp_imputed %>%
        filter(year >= 1946 & year <= 2016)

# Save the processed component separately for safety
write_csv(final_wrp_data, "processed_wrp_religion_data.csv")

# 6. Merge with Master Dataset
# We assume 'final_data_complete' exists in the environment from previous steps.
# If running standalone, uncomment the read_csv line below.
# final_data_complete <- read_csv("GRAVE_M_Master_Dataset_Final_v3.csv")

if (exists("final_data_complete")) {
        print("Merging WRP data into Master Dataset...")
        
        final_data_complete <- final_data_complete %>%
                left_join(final_wrp_data, by = c("COWcode", "year"))
        
        # Save the updated master dataset with the new filename
        write_csv(final_data_complete, here("ready_data","GRAVE_M_Master_Dataset_Final_w_religion.csv"))
        
        print("Merge complete. Updated dataset saved as 'GRAVE_M_Master_Dataset_Final_w_religion.csv'.")
} else {
        warning("Object 'final_data_complete' not found. WRP data saved as component only.")
}

print("WRP Data Processing Complete: Interpolation (1945-2010) and LOCF (2011-2016) applied.")