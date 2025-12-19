library(tidyverse)
library(janitor)
library(vdemdata)
library(ERT)

# 2. Load the V-Dem and ERT datasets

# Load the V-Dem dataset using the 'vdem' function [3]
# We assume the standard country-year dataset format is desired for panel analysis.
vdem_data <- vdem

# Load the ERT dataset using the 'get_eps' function from the ERT package [4]
# This dataset contains the Episodes of Regime Transformation [5].
ert_data <- get_eps()

# 3. Combine the datasets into a single panel dataframe

# We merge the datasets using standard identifier variables:
# country_id (V-Dem Country ID) [6-8] and year [6, 7, 9].

vdem_ert_data <- left_join(vdem_data, ert_data,
                           by = c("country_id", "year"))

 vdem_ert_data <- vdem_ert_data %>%
         rename(country_name = country_name.x)

 vdem_ert_data <- vdem_ert_data %>%
        rename(country_text_id = country_text_id.x)

 vdem_ert_data <- vdem_ert_data %>%

      # 1. Keep the original V-Dem Country ID but rename it for clarity.
        # The original merge used "country_id" [4].
         rename(vdem_id = country_id)





# 1. Store the original names (V-Dem tags)
original_names <- names(vdem_ert_data)

# 2. Apply janitor cleaning
vdem_ert_data_cleaned_names <- vdem_ert_data %>%
        clean_names()

# 3. Store the new names
cleaned_names <- names(vdem_ert_data_cleaned_names)

# 4. Create the mapping data frame
name_map <- data.frame(
        original_tag = original_names,
        janitor_name = cleaned_names
)

# 5. Save the mapping table (critical step for seamless integration)
# IMPORTANT: Save this to a file you can load in your second script
saveRDS(name_map, "variable_name_map.rds")



# --- B. Define Original V-Dem Tags to Keep ---
# This list uses the standard V-Dem tags (as found in the codebook)
original_tags_to_keep <- c(
        # Core Identifiers (Identifier variables often use underscores) [1, 1-4,22]
        "country_name", "year", "country_id", "country_text_id","COWcode",
        
        # High-Level Democracy Indices (5 Core) [5]
        "v2x_polyarchy",    # Electoral democracy index [4]
        "v2x_libdem",       # Liberal democracy index
        "v2x_partipdem",    # Participatory democracy index
        "v2xdl_delib",      # Deliberative democracy component index
        "v2x_egaldem",      # Egalitarian democracy index
        
        # Neopatrimony Index (KEEP) [6]
        "v2x_neopat",
        
        # 1. Regime Ideology / Legitimation (KEEP) (Section 3.14.6) [7]
        "v2exl_legitideol",   # Ideology [7]
        "v2exl_legitideolcr", # Ideology character (likely a composite/mean, but derived from the same section)
        "v2exl_legitlead",    # Person of the Leader [7, 8]
        "v2exl_legitperf",    # Performance legitimation [7, 9]
        "v2exl_legitratio",   # Rational-legal legitimation [7]
        
        # 2. Regime Support and Opposition Group Size and Ideology (KEEP) (Section 3.5.1)
        "v2regsupgroupssize", # Regime support groups size [10, 11]
        "v2regoppgroupssize", # Regime opposition groups size [11, 12]
        "v2regsupgroups",     # Regime support groups (needed for identifying the groups themselves, although this tag may be a placeholder for selection of the component tags) [11]
        "v2regoppgroups",     # Regime opposition groups [11, 13]
        "v2regproreg",        # Strongest pro-regime preferences [11, 14]
        "v2regantireg",       # Strongest anti-regime preferences [11, 14, 15]
        
        # 3. Rule of Law and Property Rights (Indices & Indicators - KEEP) (Section 5.9.1, 5.9.2)
        "v2x_rule",           # Rule of law index [16]
        "v2xcl_acjst",        # Access to justice index [17, 18]
        "v2xcl_prpty",        # Property rights index [17-19]
        
        # Underlying indicators for ROL/Property Rights (Section 3.9.2, 3.9.4)
        "v2cltrnslw",         # Transparent laws with predictable enforcement [20]
        "v2clrspct",          # Rigorous and impartial public administration [21]
        "v2clacjstm",         # Access to justice for men [17, 18, 22]
        "v2clacjstw",         # Access to justice for women [17, 18, 23]
        "v2clprptym",         # Property rights for men [17-19, 24]
        "v2clprptyw"          # Property rights for women [17-19, 25]
)

# --- C. Map Tags to Cleaned Names ---
# Retrieve the exact janitor names corresponding to the required V-Dem tags.
# We must ensure we only select names that actually exist in the original data file (e.g., if you didn't load all optional indices).
mapped_names_to_keep <- name_map %>%
        filter(original_tag %in% original_tags_to_keep) %>%
        pull(janitor_name)

# --- D. Execute Cleaning using the Mapped Names ---

vdem_ert_final <- vdem_ert_data_cleaned_names %>% 
        
        # 1. Select variables using the *exact* janitor names retrieved from the map
        select(
                all_of(mapped_names_to_keep), 
                
                # 2. Keep ERT-specific variables (ERT variables are usually already clean, but janitor affects them too)
                # Note: 'clean_names' usually converts camelCase to snake_case.
                # Check if 'aut_ep' or 'dem_ep' patterns persist after cleaning.
                contains("reg_trans"), 
                contains("aut_"),      
                contains("dem_")       
        ) %>%
        
        # 3. Remove Historical V-Dem data (v3 prefix)
        select(-starts_with("v3")) %>%
        
        # 4. Remove measurement model metadata (suffixes)
        # Ensure these suffixes match what janitor produces (usually underscores remain underscores)
        select(-ends_with("_codelow"), -ends_with("_codehigh"), -ends_with("_sd"),
               -ends_with("_mean"), -ends_with("_nr"), -ends_with("_osp"), -ends_with("_ord")) %>%
        
        # 5. Deduplicate rows
        distinct()

print(names(vdem_ert_final))
print(dim(vdem_ert_final))

vdem_ert_data_cleaned <- vdem_ert_final %>%
        dplyr::filter(year >= 1946)


# 5. Save the panel dataframe to the 'ready_data' directory

# Define the directory name
output_dir <- "ready_data"

# Create the directory if it does not already exist
if (!dir.exists(output_dir)) {
        dir.create(output_dir)
}

# Save the final merged dataframe as a CSV file in the specified directory
write_csv(vdem_ert_data_cleaned, 
          file.path(output_dir, "vdem_ert_combined_panel_s.csv"))

print(paste("Data successfully cleaned and saved to:", 
            file.path(output_dir, "vdem_ert_combined_panel_s.csv")))