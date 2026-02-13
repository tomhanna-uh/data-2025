## Plot the v2x_libdem score for the countries with alba_member == 1
## for any year in imputed_data
## using R 

library(ggplot2)
library(dplyr)

data("imputed_data")

# Filter the data for alba_member == 1
alba_data <- imputed_data %>%
  filter(alba_member == 1)
# Plot the v2x_libdem score for alba_member countries
ggplot(alba_data, aes(x = year, y = v2x_libdem,
                     color = country_name)) +
  geom_line() +
  labs(title = "V-Dem")

  xlab("Year") +
  ylab("Liberal Democracy)
  theme_minimal() +

