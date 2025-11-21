# workforce-attrition-analysis

## Project Overview
This project analyzes workforce attrition dynamics using high-frequency administrative data from a large manufacturing firm. It overcomes significant data quality challenges (missing timestamps, duplicate IDs, discontinuous employment spells) to estimate the causal impact of retention interventions using **Survival Analysis** and **Inverse Propensity Weighting (IPW)**.

## Key Features
* **Complex ETL Pipeline:** Reconstructs daily employment panels from raw, disjointed attendance logs for over 450k worker-day observations.
* **Algorithmic Imputation:** Implements logic to impute missing exit dates based on re-entry gaps.
* **Multi-Spell Survival Analysis:** Models recurrent events (workers quitting and rejoining up to 15 times) to accurately estimate hazard rates.
* **Causal Inference:** Uses IPW and High-Dimensional Fixed Effects (HDFE) to control for selection bias in treatment evaluation.

## Code Structure
* `01_data_cleaning.do`: Standardizes variables, imputes missing dates, and reshapes wide-format employment history into long spells.
* `02_survival_prep.do`: Sets up the data for survival analysis (`stset`), defining failure events and censoring criteria for multi-spell data.
* `03_analysis_models.do`: Runs DiD and ATE regressions with date and individual fixed effects; exports results to formatted tables.

## Tools Used
* **Stata 17**
* **Packages:** `reghdfe`, `stset`, `esttab`
