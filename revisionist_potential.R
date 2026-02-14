library(tidyverse)
library(here)

# 1. Load the dataset from your ready_data folder
df <- read_csv(here("ready_data", "GRAVE_D_Master_Final.csv"))


# 2. Calculate Ideological Extremity (Annual Latent Variable Median)
df <- df %>%
        group_by(year) %>%
        mutate(
                # Median of the latent legitimacy variable for that year
                global_med_ideol = median(v2exl_legitideol_a, na.rm = TRUE),
                
                # Absolute distance from the system-wide median
                ideol_extremity_a = abs(v2exl_legitideol_a - global_med_ideol),
                ideol_extremity_b = abs(v2exl_legitideol_b - global_med_ideol)
        ) %>%
        ungroup()

# 2. Calculate Ideological Extremity (Annual Latent Variable Median)
df <- df %>%
        group_by(year) %>%
        mutate(
                global_med_ideol = median(v2exl_legitideol_a, na.rm = TRUE),
                ideol_extremity_a = abs(v2exl_legitideol_a - global_med_ideol),
                ideol_extremity_b = abs(v2exl_legitideol_b - global_med_ideol)
        ) %>%
        ungroup()

# 3. Calculate Revisionist Potential & Revisionism Distance
df <- df %>%
        mutate(
                # Side A (Sender) Calculation
                dem_constraint_inv_a = 1 - v2x_libdem_a,
                rev_potential_a = (ideol_extremity_a + dem_constraint_inv_a + v2exl_legitideol_a) / 3,
                
                # Side B (Receiver) Calculation
                dem_constraint_inv_b = 1 - v2x_libdem_b,
                rev_potential_b = (ideol_extremity_b + dem_constraint_inv_b + v2exl_legitideol_b) / 3,
                
                # Dyadic Distance (Gap in revisionist posture)
                revisionism_distance = abs(rev_potential_a - rev_potential_b)
        )


# 3. Standardization and Composite Index Calculation
# We use Z-scores to prevent the wide range of v2exl_legitideol from swamping the index
df <- df %>%
        mutate(
                # Invert LibDem (Low Democratic Constraint)
                dem_constraint_inv_a = 1 - v2x_libdem_a,
                dem_constraint_inv_b = 1 - v2x_libdem_b,
                
                # Standardize Side A components (Z-scores: Mean 0, SD 1)
                z_legit_a = as.numeric(scale(v2exl_legitideol_a)),
                z_dem_a   = as.numeric(scale(dem_constraint_inv_a)),
                z_ext_a   = as.numeric(scale(ideol_extremity_a)),
                
                # Standardize Side B components
                z_legit_b = as.numeric(scale(v2exl_legitideol_b)),
                z_dem_b   = as.numeric(scale(dem_constraint_inv_b)),
                z_ext_b   = as.numeric(scale(ideol_extremity_b)),
                
                # Final Standardized Revisionist Potential (Arithmetic Mean of Z-scores)
                rev_potential_a = (z_legit_a + z_dem_a + z_ext_a) / 3,
                rev_potential_b = (z_legit_b + z_dem_b + z_ext_b) / 3,
                
                # Dyadic Distance (The revisionist gap)
                revisionism_distance = abs(rev_potential_a - rev_potential_b)
        )

# 4. Save back to ready_data
write_csv(df, here("ready_data", "GRAVE_D_Master_Final.csv"))

# 5. CLEAN VIEW: Verify results without the tibble mess
# This will open a clean, readable window in RStudio
df %>%
        select(rev_potential_a, rev_potential_b, revisionism_distance) %>%
        summary() %>%
        as.data.frame() %>%
        View("Revisionism_Summary")

message("Success: Standardized Revisionist Potential saved and summary window opened.")