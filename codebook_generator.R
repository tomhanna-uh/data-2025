# -------------------------------------------------------------------------
# GRAVE-M Codebook & Diagnostics Generator
# Author: Tom Hanna
# Description: Generates summary statistics and missingness profiles for 
#              the final GRAVE-M dataset (v3).
# -------------------------------------------------------------------------

library(tidyverse)
library(here)
library(dplyr)

# 1. Load the Final Dataset
# This assumes the pipeline has been run and the file exists.
df <- read_csv(here("ready_data", "GRAVE_M_Master_Dataset_Final_v3.csv"))

# -------------------------------------------------------------------------
# SECTION 1.5: DATA TYPE CASTING (CRITICAL FOR ANALYSIS)
# -------------------------------------------------------------------------
# Defining variable groups for correct processing
binary_vars <- c(
        "is_petro_state", 
        "is_aut_episode", 
        "is_dem_episode", 
        "alba_member", 
        "mid_onset", 
        "is_leftist_leader", 
        "is_rightist_leader"
)

ordinal_vars <- c(
        "gli_leader_ideology_num", # 0=None, 1=Left, 2=Center, 3=Right
        "fatality"                 # 0-6 Scale
)

# Explicitly cast these to factors so R treats them as categories, not numbers.
# This ensures summary() gives counts, and future models use embeddings.
df <- df %>%
        mutate(
                across(any_of(binary_vars), as.factor),
                across(any_of(ordinal_vars), as.factor)
        )

cat("\n--- DATA TYPE UPDATE ---\n")
cat("Converted", length(binary_vars), "binary variables and", length(ordinal_vars), "ordinal variables to Factors.\n")

# -------------------------------------------------------------------------
# SECTION 1: QUICK DIAGNOSTICS
# -------------------------------------------------------------------------
cat("\n--- QUICK DIAGNOSTICS ---\n")
cat("Total Observations:", nrow(df), "\n")
cat("Unique Countries:", length(unique(df$COWcode)), "\n")
cat("Year Range:", min(df$year), "-", max(df$year), "\n")

# -------------------------------------------------------------------------
# SECTION 2: SPECIFIC VARIABLE CHECKS
# -------------------------------------------------------------------------
cat("\n--- FRASER BMP STATUS ---\n")
if("fraser_bmp_score" %in% names(df)) {
        total_missing <- sum(is.na(df$fraser_bmp_score))
        total_obs <- nrow(df)
        coverage_pct <- round((1 - (total_missing / total_obs)) * 100, 1)
        
        cat("  - Found in dataset: YES\n")
        cat("  - Missing values:", total_missing, "\n")
        cat("  - Coverage:", coverage_pct, "%\n")
} else {
        cat("  - Found in dataset: NO\n")
}

cat("\n--- UNIFIED CORRUPTION STATUS ---\n")
if("unified_corruption" %in% names(df)) {
        total_missing <- sum(is.na(df$unified_corruption))
        total_obs <- nrow(df)
        coverage_pct <- round((1 - (total_missing / total_obs)) * 100, 1)
        
        cat("  - Found in dataset: YES\n")
        cat("  - Missing values:", total_missing, "\n")
        cat("  - Coverage:", coverage_pct, "%\n")
} else {
        cat("  - Found in dataset: NO\n")
}

# -------------------------------------------------------------------------
# SECTION 3: MISSINGNESS PROFILE (Top 20)
# -------------------------------------------------------------------------
cat("\n--- MISSINGNESS PROFILE (TOP 20 VARIABLES) ---\n")

missing_profile <- df %>%
        summarise(across(everything(), ~ sum(is.na(.)) / n() * 100)) %>%
        pivot_longer(everything(), names_to = "Variable", values_to = "Missing_Percent") %>%
        arrange(desc(Missing_Percent)) %>%
        slice(1:20) %>%
        mutate(Missing_Percent = paste0(round(Missing_Percent, 2), "%"))

print(as.data.frame(missing_profile))

# -------------------------------------------------------------------------
# SECTION 4: STRUCTURAL VARIABLE CHECKS
# -------------------------------------------------------------------------
cat("\n--- STRUCTURAL VARIABLE CHECKS (COUNTS) ---\n")
# Using table() now because they are factors
cat("Autocratization Episodes Distribution:\n")
print(table(df$is_aut_episode, useNA = "ifany"))

cat("\nDemocratization Episodes Distribution:\n")
print(table(df$is_dem_episode, useNA = "ifany"))

cat("\nLeader Ideology Distribution (0=Miss, 1=L, 2=C, 3=R):\n")
if("gli_leader_ideology_num" %in% names(df)) {
        print(table(df$gli_leader_ideology_num, useNA = "ifany"))
}

# -------------------------------------------------------------------------
# SECTION 5: EXPORT TYPED DATASET
# -------------------------------------------------------------------------
# 1. Save as CSV (Standard, but loses Factor metadata)
csv_filename <- "GRAVE_M_Master_Dataset_Final_v3_factors.csv"
write_csv(df, here("ready_data", csv_filename))

# 2. Save as RDS (Preserves Factor Types perfectly for R)
rds_filename <- "GRAVE_M_Master_Dataset_Final_v3.rds"
write_rds(df, here("ready_data", rds_filename))

cat("\n--- EXPORT COMPLETE ---\n")
cat("Saved CSV to:", csv_filename, "\n")
cat("Saved RDS to:", rds_filename, "(Use read_rds() to load this with factors preserved)\n")

# -------------------------------------------------------------------------
# SECTION 6: VERIFICATION
# -------------------------------------------------------------------------
cat("\n--- VERIFICATION OF SAVED FILE ---\n")
# Reload the RDS to prove it kept the types
check_df <- read_rds(here("ready_data", rds_filename))

cat("Checking class of 'is_petro_state' in RDS:\n")
print(class(check_df$is_petro_state))

cat("Checking class of 'gli_leader_ideology_num' in RDS:\n")
print(class(check_df$gli_leader_ideology_num))

if(is.factor(check_df$is_petro_state)) {
        cat("SUCCESS: Variables are correctly stored as Factors in the RDS file.\n")
} else {
        cat("WARNING: Variables are NOT Factors. Check the casting step.\n")
}