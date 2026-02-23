# =============================================================================
# sinetgub.R — GRAVE-M Imputed Dataset: Column Inspection and Variable Renaming
# =============================================================================
# Purpose: Load the imputed monadic dataset, inspect column names, rename
# variables to match naming conventions expected by the dyadic pipeline
# (grave_d2.R) and the autocracy_conflict_signaling analysis scripts.
#
# After dyadic expansion in grave_d2.R, monadic base names get _a / _b
# suffixes appended. So base names here must match the expected stems:
#   e.g., national_military_capabilities -> sidea_national_military_capabilities
#         religious_support               -> sidea_religious_support
# =============================================================================

library(readr)
library(dplyr)
library(here)

# -----------------------------------------------------------------------------
# 1. Load the imputed dataset
# -----------------------------------------------------------------------------
imputed_data <- read_csv(here("ready_data", "GRAVE_M_Master_Dataset_Final_v3_imputed.csv"))

# Inspect current column names before renaming
cat("\n--- Column names in imputed dataset (before rename) ---\n")
print(colnames(imputed_data))

cat("\n--- Structure ---\n")
str(imputed_data)

cat("\n--- Summary ---\n")
summary(imputed_data)

# -----------------------------------------------------------------------------
# 2. Rename variables
# rename(any_of(...)) silently skips columns that do not exist.
# Named vector format: c(new_name = "old_name").
#
# Capabilities (COW CINC)
#   Source name(s): cinc, national_capabilities, nmc, cow_cinc
#   Target name:    national_military_capabilities
#     -> dyadic result: sidea_national_military_capabilities, sideb_...
#
# GRAVE-D Support Group Variables
#   Source name(s): vary by dataset version (with or without sidea_ prefix)
#   Target base names (no side prefix; suffix added by grave_d2.R):
#     religious_support, party_elite_support, rural_worker_support,
#     military_support, ethnic_racial_support
#
# GRAVE-D Leadership Ideology Variables
#   Target base names:
#     revisionist_domestic
#     nationalist_revisionist_domestic, socialist_revisionist_domestic,
#     religious_revisionist_domestic, reactionary_revisionist_domestic
#     dynamic_leader
#
# Selectorate theory
#   winning_coalition_size
# -----------------------------------------------------------------------------

imputed_data <- imputed_data %>%
  rename(any_of(c(
    # --- National Military Capabilities (COW CINC) ----------------------------
    national_military_capabilities = "cinc",
    national_military_capabilities = "nmc",
    national_military_capabilities = "cow_cinc",
    national_military_capabilities = "national_capabilities",
    national_military_capabilities = "milit_capabilities",

    # --- GRAVE-D Support Group Variables (base names, no side prefix) ---------
    # These get _a / _b appended in grave_d2.R dyadic expansion
    religious_support          = "sidea_religious_support",
    party_elite_support        = "sidea_party_elite_support",
    rural_worker_support       = "sidea_rural_worker_support",
    military_support           = "sidea_military_support",
    ethnic_racial_support      = "sidea_ethnic_racial_support",

    # If stored without side prefix already, these are no-ops (already correct):
    # religious_support, party_elite_support, etc.

    # --- GRAVE-D Leadership Ideology (base names) ----------------------------
    revisionist_domestic                  = "sidea_revisionist_domestic",
    nationalist_revisionist_domestic      = "sidea_nationalist_revisionist_domestic",
    socialist_revisionist_domestic        = "sidea_socialist_revisionist_domestic",
    religious_revisionist_domestic        = "sidea_religious_revisionist_domestic",
    reactionary_revisionist_domestic      = "sidea_reactionary_revisionist_domestic",
    dynamic_leader                        = "sidea_dynamic_leader",

    # --- Selectorate Theory --------------------------------------------------
    winning_coalition_size = "sidea_winning_coalition_size",
    winning_coalition_size = "w",
    winning_coalition_size = "wcs"
  )))

# Verify renamed columns
cat("\n--- Column names after rename ---\n")
print(colnames(imputed_data))

# -----------------------------------------------------------------------------
# 3. Check that key expected columns are present
# -----------------------------------------------------------------------------
expected_cols <- c(
  # Identifiers
  "COWcode", "year",
  # V-Dem
  "v2x_libdem", "v2exl_legitideol", "v2exl_legitlead", "v2exl_legitperf",
  # Capabilities
  "national_military_capabilities",
  # GRAVE support groups
  "religious_support", "party_elite_support", "rural_worker_support",
  "military_support", "ethnic_racial_support",
  # GRAVE ideology
  "revisionist_domestic", "dynamic_leader",
  # Selectorate
  "winning_coalition_size"
)

missing_cols <- setdiff(expected_cols, colnames(imputed_data))
if (length(missing_cols) > 0) {
  warning(
    sprintf(
      "[sinetgub.R] Missing expected columns after rename:\n  %s",
      paste(missing_cols, collapse = "\n  ")
    )
  )
} else {
  message("[sinetgub.R] All expected columns present after rename.")
}

# -----------------------------------------------------------------------------
# 4. Save the renamed dataset (overwrites input; update path if needed)
# -----------------------------------------------------------------------------
write_csv(
  imputed_data,
  here("ready_data", "GRAVE_M_Master_Dataset_Final_v3_imputed.csv")
)

message("[sinetgub.R] Variable renaming complete. Dataset saved.")
