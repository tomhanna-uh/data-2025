# Quick summary check
df %>%
        select(rev_potential_a, rev_potential_b, revisionism_distance) %>%
        summary()

# Check coverage
sum(is.na(df$rev_potential_a)) / nrow(df) * 100

# regime type sanity check

df %>%
        mutate(regime_cat = ifelse(v2x_libdem_a > 0.7, "Democracy", 
                                   ifelse(v2x_libdem_a < 0.3, "Autocracy", "Mixed"))) %>%
        group_by(regime_cat) %>%
        summarize(
                mean_rev = mean(rev_potential_a, na.rm = TRUE),
                sd_rev = sd(rev_potential_a, na.rm = TRUE),
                n = n()
        )

# dyadic asymmetry check 

df %>%
        filter(v2x_libdem_a < 0.3 & v2x_libdem_b > 0.7) %>%
        summarize(
                avg_gap = mean(revisionism_distance, na.rm = TRUE),
                max_gap = max(revisionism_distance, na.rm = TRUE)
        )


# correlation matrix for leakage

# Check how much the new index correlates with its constituent parts
df %>%
        select(rev_potential_a, v2x_libdem_a, v2exl_legitideol_a, ideol_extremity_a) %>%
        cor(use = "complete.obs")

# variance of components


df %>%
        select(v2exl_legitideol_a, dem_constraint_inv_a, ideol_extremity_a) %>%
        summarise(across(everything(), list(sd = ~sd(., na.rm = TRUE), range = ~diff(range(., na.rm = TRUE)))))

# revisionism or just market vulnerability

df %>%
        filter(rev_potential_a > mean(rev_potential_a, na.rm = TRUE)) %>%
        summarize(correlation_with_target_vulnerability = cor(rev_potential_a, hhi_import_b, use = "complete.obs"))


# identification of revisionist leaders

df %>%
        filter(year == 2022) %>%
        distinct(iso3a, rev_potential_a) %>%
        arrange(desc(rev_potential_a)) %>%
        head(10)