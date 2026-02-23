# =============================================================================
# sinetgub.R — GRAVE-D Master Dataset: Column Inspection and Validation
# =============================================================================
# Purpose: Load GRAVE_D_Master_with_Leaders.csv, inspect column names,
# validate against the column sets expected by 01_load_data.R in the
# autocracy_conflict_signaling analysis project, and apply any needed
# safe renames so column names match exactly.
#
# This is a DYADIC dataset (one row per directed dyad-year). It is NOT
# the monadic imputed dataset. The correct file is:
#   ready_data/GRAVE_D_Master_with_Leaders.csv
# =============================================================================

library(readr)
library(dplyr)
library(here)

# -----------------------------------------------------------------------------
# 1. Load the dyadic master dataset
# -----------------------------------------------------------------------------
grave_d <- read_csv(here("ready_data", "GRAVE_D_Master_with_Leaders.csv"),
                    show_col_types = FALSE)

cat("\n--- Dimensions ---\n")
cat(sprintf("%d rows x %d columns\n", nrow(grave_d), ncol(grave_d)))

cat("\n--- Column names ---\n")
print(colnames(grave_d))

cat("\n--- Head ---\n")
print(head(grave_d))

# -----------------------------------------------------------------------------
# 2. Column sets required by autocracy_conflict_signaling/R/01_load_data.R
# -----------------------------------------------------------------------------
DYAD_REQUIRED_COLS <- c(
  # Identifiers
  "COWcode_a", "COWcode_b", "year",
  # Democracy scores (V-Dem)
  "v2x_libdem_a", "v2x_libdem_b",
  # Conflict outcome
  "hihosta",
  # V-Dem legitimation variables
  "v2exl_legitideol_a",
  "v2exl_legitlead_a",
  "v2exl_legitperf_a",
  # Capabilities
  "sidea_national_military_capabilities",
  "sideb_national_military_capabilities"
)

DYAD_GRAVE_COLS <- c(
  # GRAVE-D leadership ideology
  "sidea_revisionist_domestic",
  # GRAVE-D support group variables
  "sidea_religious_support",
  "sidea_party_elite_support",
  "sidea_rural_worker_support",
  "sidea_ethnic_racial_support",
  "sidea_military_support",
  "sidea_nationalist_revisionist_domestic",
  "sidea_socialist_revisionist_domestic",
  "sidea_religious_revisionist_domestic",
  "sidea_reactionary_revisionist_domestic",
  # GRAVE-D dynamic leadership
  "sidea_dynamic_leader",
  # Selectorate theory
  "sidea_winning_coalition_size"
)

# -----------------------------------------------------------------------------
# 3. Check for missing columns
# -----------------------------------------------------------------------------
missing_required <- setdiff(DYAD_REQUIRED_COLS, colnames(grave_d))
missing_grave    <- setdiff(DYAD_GRAVE_COLS,    colnames(grave_d))

if (length(missing_required) == 0) {
  message("[sinetgub] All core required columns present.")
} else {
  warning(sprintf(
    "[sinetgub] Missing CORE columns:\n  %s",
    paste(missing_required, collapse = "\n  ")
  ))
}

if (length(missing_grave) == 0) {
  message("[sinetgub] All GRAVE-D columns present.")
} else {
  warning(sprintf(
    "[sinetgub] Missing GRAVE-D columns:\n  %s",
    paste(missing_grave, collapse = "\n  ")
  ))
}

# -----------------------------------------------------------------------------
# 4. Safe rename block
# rename(any_of(...)) silently skips columns that do not exist.
# Named vector format: c(new_name = "old_name").
#
# Add entries here whenever the source CSV uses a different column name
# than what 01_load_data.R expects. Common discrepancies:
#   cinc_a / nmc_a        -> sidea_national_military_capabilities
#   cinc_b / nmc_b        -> sideb_national_military_capabilities
#   statea / ccode_a      -> COWcode_a
#   stateb / ccode_b      -> COWcode_b
# -----------------------------------------------------------------------------
grave_d <- grave_d %>%
  rename(any_of(c(
    # --- Identifiers ----------------------------------------------------------
    COWcode_a = "statea",
    COWcode_a = "ccode_a",
    COWcode_a = "ccodea",
    COWcode_b = "stateb",
    COWcode_b = "ccode_b",
    COWcode_b = "ccodeb",

    # --- National Military Capabilities (COW CINC) ---------------------------
    sidea_national_military_capabilities = "cinc_a",
    sidea_national_military_capabilities = "nmc_a",
    sidea_national_military_capabilities = "cow_cinc_a",
    sidea_national_military_capabilities = "national_capabilities_a",
    sideb_national_military_capabilities = "cinc_b",
    sideb_national_military_capabilities = "nmc_b",
    sideb_national_military_capabilities = "cow_cinc_b",
    sideb_national_military_capabilities = "national_capabilities_b",

    # --- V-Dem legitimation --------------------------------------------------
    v2exl_legitideol_a = "v2exl_legitideol_a",  # no-op if already correct
    v2exl_legitlead_a  = "v2exl_legitlead_a",
    v2exl_legitperf_a  = "v2exl_legitperf_a",

    # --- GRAVE-D support groups ----------------------------------------------
    sidea_religious_support          = "religious_support_a",
    sidea_party_elite_support        = "party_elite_support_a",
    sidea_rural_worker_support       = "rural_worker_support_a",
    sidea_military_support           = "military_support_a",
    sidea_ethnic_racial_support      = "ethnic_racial_support_a",

    # --- GRAVE-D ideology ----------------------------------------------------
    sidea_revisionist_domestic             = "revisionist_domestic_a",
    sidea_nationalist_revisionist_domestic = "nationalist_revisionist_domestic_a",
    sidea_socialist_revisionist_domestic   = "socialist_revisionist_domestic_a",
    sidea_religious_revisionist_domestic   = "religious_revisionist_domestic_a",
    sidea_reactionary_revisionist_domestic = "reactionary_revisionist_domestic_a",
    sidea_dynamic_leader                   = "dynamic_leader_a",

    # --- Selectorate theory --------------------------------------------------
    sidea_winning_coalition_size = "winning_coalition_size_a",
    sidea_winning_coalition_size = "w_a",
    sidea_winning_coalition_size = "wcs_a"
  )))

# -----------------------------------------------------------------------------
# 5. Re-check after rename
# -----------------------------------------------------------------------------
missing_required_after <- setdiff(DYAD_REQUIRED_COLS, colnames(grave_d))
missing_grave_after    <- setdiff(DYAD_GRAVE_COLS,    colnames(grave_d))

cat("\n--- Column check after rename ---\n")
if (length(missing_required_after) == 0 && length(missing_grave_after) == 0) {
  message("[sinetgub] All expected columns present after rename. Ready to save.")
} else {
  if (length(missing_required_after) > 0)
    warning(sprintf("[sinetgub] Still missing CORE columns:\n  %s",
                    paste(missing_required_after, collapse = "\n  ")))
  if (length(missing_grave_after) > 0)
    warning(sprintf("[sinetgub] Still missing GRAVE-D columns:\n  %s",
                    paste(missing_grave_after, collapse = "\n  ")))
}

# -----------------------------------------------------------------------------
# 6. Save validated/renamed dataset
# Output path matches what 01_load_data.R expects:
#   here("data", "GRAVE_D_Master_with_Leaders.csv")
# Run this script from the data-2025 project root before running the
# autocracy_conflict_signaling analysis.
# -----------------------------------------------------------------------------
write_csv(grave_d, here("ready_data", "GRAVE_D_Master_with_Leaders.csv"))

message("[sinetgub] Done. Validated dataset saved to ready_data/GRAVE_D_Master_with_Leaders.csv")
