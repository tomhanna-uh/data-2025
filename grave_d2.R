# -------------------------------------------------------------------------
# GRAVE-D Master Data Pipeline (Directed Dyadic)
# Author: Tom Hanna
# Description: Fresh script using fbic.csv as spine with HHI integration
# -------------------------------------------------------------------------
library(tidyverse)
library(here)
library(countrycode)

# 1. CALCULATE MONADIC HHI (Market Concentration)
# Market vulnerability is calculated monadically before dyadic expansion
red_raw <- read_csv(here("raw-data", "RED_full_final.csv"))

monadic_hhi_export <- red_raw %>%
        group_by(exporter_cow, year) %>%
        summarize(
                hhi_export = sum((RED_export_importance / 100)^2, na.rm = TRUE),
                .groups = "drop"
        ) %>%
        rename(COWcode = exporter_cow)

monadic_hhi_import <- red_raw %>%
        group_by(importer_cow, year) %>%
        summarize(
                hhi_import = sum((RED_import_importance / 100)^2, na.rm = TRUE),
                .groups = "drop"
        ) %>%
        rename(COWcode = importer_cow)

# 2. LOAD THE DYADIC SPINE (Renamed FBIC)
fbic_dyadic <- read_csv(here("raw-data", "fbic.csv")) %>%
  mutate(
    COWcode_a = countrycode(iso3a, "iso3c", "cown"),
    COWcode_b = countrycode(iso3b, "iso3c", "cown")
  ) %>%
  filter(!is.na(COWcode_a) & !is.na(COWcode_b))

# 3. INTEGRATE RAW DYADIC SOURCES (MIDS & RED)
mids_dyadic <- read_csv(here("raw-data", "dyadic_mid.csv")) %>%
  select(year, statea, stateb, hihosta, fatlev) %>%
  rename(COWcode_a = statea, COWcode_b = stateb)

grave_d <- fbic_dyadic %>%
  left_join(mids_dyadic, by = c("year", "COWcode_a", "COWcode_b")) %>%
  left_join(red_raw %>% select(year, exporter_cow, importer_cow, RED_export_importance), 
            by = c("year", "COWcode_a" = "exporter_cow", "COWcode_b" = "importer_cow"))

# 4. LOAD MONADIC ATTRIBUTES (Removing collapsed variables to prevent leakage)
monadic_data <- read_csv(here("ready_data", "GRAVE_M_Master_Dataset_Final_w_religion.csv"))

monadic_clean <- monadic_data %>%
  select(-starts_with("mid_"), -starts_with("exp_dep_"), -starts_with("atop_"), 
                  -any_of(c("trade_export_hhi", "trade_import_hhi"))) %>%
  left_join(monadic_hhi_export, by = c("COWcode", "year")) %>%
  left_join(monadic_hhi_import, by = c("COWcode", "year"))

# -------------------------------------------------------------------------
# 4b. STANDARDIZE INCONSISTENT VARIABLE NAMES
# Some columns in the monadic source data already carry an 'a' or 'b' suffix
# from their origin (inconsistent with the rest of the dataset). We rename
# them to clean base names HERE, BEFORE the _a / _b dyadic suffixes are
# appended in step 5. This prevents double-suffixed column names such as
# gdpmer2011usda_a, milex2011usdmila_a, igosa_a, countrya_a, etc.
#
# rename(any_of(...)) silently skips columns that do not exist, so this
# block is safe to run regardless of which version of the source data is
# present. Named vector format: c(new_name = "old_name").
# -------------------------------------------------------------------------
monadic_clean <- monadic_clean %>%
  rename(any_of(c(
    # GDP (Market Exchange Rate) -----------------------------------------------
    gdpmer2011usd   = "gdpmer2011usda",

    # Military Expenditure -----------------------------------------------------
    milex2011usdmil = "milex2011usdmila",

    # Military Stockpile -------------------------------------------------------
    milstock        = "milstocka",

    # IGO Memberships ----------------------------------------------------------
    igos            = "igosa",
    igoscount       = "igosaccount",    # alternate old name

    # Country string and ISO3 identifiers --------------------------------------
    country         = "countrya",
    iso3            = "iso3a",

    # UN Geographic Region -----------------------------------------------------
    unregion        = "unregiona"
  )))

# 5. INTEGRATE SENDER (A) AND RECEIVER (B) PROFILES
# Merge for Country A (Sender)
grave_d <- grave_d %>%
  left_join(monadic_clean, by = c("COWcode_a" = "COWcode", "year" = "year")) %>%
  rename_with(~paste0(., "_a"), .cols = names(monadic_clean)[!names(monadic_clean) %in% c("COWcode", "year")])

# Merge for Country B (Receiver)
grave_d <- grave_d %>%
  left_join(monadic_clean, by = c("COWcode_b" = "COWcode", "year" = "year")) %>%
  rename_with(~paste0(., "_b"), .cols = names(monadic_clean)[!names(monadic_clean) %in% c("COWcode", "year")])

# 6. VARIABLE ENGINEERING: DYADIC CONTEXT & DISTANCE
grave_d <- grave_d %>%
  mutate(
    # Market Vulnerability Gap (Difference in HHI concentration)
    hhi_export_gap = hhi_export_a - hhi_export_b,
    
    # Targeted Democracy Logic
    targeted_democracy = ifelse(v2x_libdem_b > 0.5 & v2x_libdem_a < 0.3, 1, 0),
    
    # Religious Distance (Difference in major religion shares)
    islm_dist  = abs(islmgenpct_a - islmgenpct_b),
    chrst_dist = abs(chrstgenpct_a - chrstgenpct_b)
  )

# 7. SAVE THE GRAVE-D MASTER
write_csv(grave_d, here("ready_data", "GRAVE_D_Master_Final.csv"))
