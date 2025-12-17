library(tidyverse)
library(cshapes)
library(here)

# Load the Data
vdem_ert_data <- read_csv(here("ready_data","vdem_ert_combined_panel_s.csv"))

# Ensure essential columns are correctly typed
vdem_ert_data <- vdem_ert_data %>%
        mutate(year = as.integer(year),
               country_id = as.integer(country_id))

# Separate Identifiers from Substantive Data
substantive_vars <- vdem_ert_data %>%
        select(-country_name, -country_text_id)

# Create the Complete Dyad-Year Structure (with all required ID columns)
# This step prepares the core identification variables, ensuring vdem_id is kept.
unique_id_year <- vdem_ert_data %>%
        dplyr::select(country_id, year, country_name, vdem_id, country_text_id) %>%
        dplyr::distinct() 

dyads_raw <- unique_id_year %>%
        dplyr::rename(country_id_A = country_id,
                      year = year,
                      country_name_A = country_name,
                      vdem_id_A = vdem_id,
                      country_text_id_A = country_text_id) %>% # <-- NEW RENAME for A
        tidyr::crossing(
                unique_id_year %>%
                        dplyr::rename(country_id_B = country_id,
                                      country_name_B = country_name,
                                      vdem_id_B = vdem_id,
                                      country_text_id_B = country_text_id) %>% # <-- NEW RENAME for B
                        dplyr::select(-year)
        )


# Filter out self-pairs and arrange (this part remains standard)
dyads_panel <- dyads_raw %>%
        dplyr::filter(country_id_A != country_id_B) %>%
        dplyr::arrange(year, country_id_A, country_id_B)


# --- NEW: Create Dyad Identifiers (Step 2.5) ---

dyads_panel <- dyads_panel %>%
        mutate(
                # Create a directed dyad ID by combining the COWcode based country_ids.
                # We ensure they are ordered A -> B to maintain the directionality.
                # We use paste0 and 'as.character' for concatenation, separated by an underscore.
                dyad_id = paste0(country_id_A, "_", country_id_B),
                
                # Create a unique dyad-year ID.
                dyad_year_id = paste0(dyad_id, "_", year)
        )

# 3. Prepare Substantive Data for Joining (Pre-suffixing)
data_A <- substantive_vars %>%
        rename_with(~ paste0(., "_A"),
                    .cols = -c(country_id, year))

data_B <- substantive_vars %>%
        rename_with(~ paste0(., "_B"),
                    .cols = -c(country_id, year))

# 4. Merge Substantive Data into the Dyad Structure
dyads_panel <- dyads_panel %>%
        left_join(data_A, by = c("country_id_A" = "country_id", "year")) %>%
        left_join(data_B, by = c("country_id_B" = "country_id", "year"))


# cshapes needs dates

dyads_panel <- dyads_panel %>%
        dplyr::filter(year < 2020)

dyads_panel$date <- as.Date(paste0(dyads_panel$year, "-01-01"))


unique_dates <- unique(dyads_panel$date)

# Get unique years
unique_years <- unique(dyads_panel$date)

# Loop over each year and build distance dataframes
dist_list <- lapply(unique_years, function(current_date) {
        # Obtain distance matrix for capitals for this date
        dist_mat <- distmatrix(current_date, type = "capdist")
        
        # Convert matrix to long dataframe
        dist_df <- as.data.frame(as.table(dist_mat))
        colnames(dist_df) <- c("country_id_A", "country_id_B", "dist_km")
        
        # Add date column for joining
        dist_df$date <- current_date
        
        return(dist_df)
})

# Combine all years into one dataframe
all_distances <- bind_rows(dist_list)

# Join the distances back to dyads_panel by date and country codes
dyads_panel_with_dist <- dyads_panel %>%
        left_join(all_distances,
                  by = c("date", "country_id_A", "country_id_B"))





# 7. Save the Resulting Dyadic Dataset
output_dir <- "ready_data"
write_csv(dyads_panel_final,
          file.path(output_dir, "vdem_ert_dyads_cshapes_cleaned.csv"))