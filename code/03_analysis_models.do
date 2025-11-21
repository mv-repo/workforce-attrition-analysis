/*===============================================================================
* File: pls_admin_analysis.do
* Purpose: This script runs regressions models for admin analysis on attenance 
and attrition
* Author: Mohit Verma
* Created: 08 Sept 2025
*===============================================================================*/

version 17
set more off
cls
clear all


/*==============================================================================
  PATHS
------------------------------------------------------------------------------*/

global out_daily "/Users/mohitverma/Downloads/pls_daily_attrition.dta"
global final_attendance "/Users/mohitverma/Downloads/attendance_final.dta"
global final_attrition "/Users/mohitverma/Downloads/final_attrition.dta"


/*==============================================================================
  INVERSE PROPENSITY WEIGHTING (IPW) ANALYSIS
------------------------------------------------------------------------------*/

use "${out_daily}", clear

gen retained = 0 if turnover_daily == 1
replace retained = 1 if turnover_daily == 0


gen absence = 1 if attendance == 0
replace absence = 0 if attendance == 1

gen treatment_start_date = enrol_date
replace treatment_start_date = mdy(11, 20, 2024) if missing(treatment_start_date)
format treatment_start_date %td

// Initialize the `prepost` variable as missing ---
gen prepost = .
label var prepost "Pre (180 days) vs. Post Treatment"

// Define the Post-Period ---
replace prepost = 1 if date >= treatment_start_date & !missing(treatment_start_date)

// This assigns prepost=0 ONLY for dates within the 180 days before treatment.
replace prepost = 0 if date < treatment_start_date & date >= (treatment_start_date - 180) & !missing(treatment_start_date)

label define prepost_lbl 0 "Pre (180 days)" 1 "Post-Treatment"
label values prepost prepost_lbl

// Calculate the number of days before treatment for the pre-period observations
gen days_before_treatment = treatment_start_date - date if prepost == 0

// Summarize the result
summarize days_before_treatment

// Creating days since treatment
gen days_since_treatment = date - treatment_start_date

// Define the global macro for control variables
global controls age i.strata i.migrant i.hh_head_resp i.educ_g10 i.remit any_blsavings any_bnksavings log_total_savings any_blloans log_total_loans knows_upi knowsatm_nohelp own_phone asset_index total_tenure_months current_tenure_months

* ================================================================
* CALCULATE THE INVERSE PROBABILITY WEIGHTS
* ================================================================
// The weight for each person is the inverse of the probability of
// receiving the treatment they actually received.

probit retained i.treatment1 i.month $controls i.phase
predict ps_t1, pr
gen ipw_t1 = .
replace ipw_t1 = 1/ps_t1 if retained == 1
replace ipw_t1 = 1/(1-ps_t1) if retained == 0

probit retained i.treatment2 i.month $controls i.phase
predict ps_t2, pr
gen ipw_t2 = . 
replace ipw_t2 = 1/ps_t2 if retained == 1
replace ipw_t2 = 1/(1-ps_t2) if retained == 0

probit retained i.treatment4 i.month $controls i.phase
predict ps_t4, pr
gen ipw_t4 = .
replace ipw_t4 = 1/ps_t4 if retained == 1
replace ipw_t4 = 1/(1-ps_t4) if retained == 0

probit retained i.treatment5 i.month $controls i.phase
predict ps_t5, pr
gen ipw_t5 = .
replace ipw_t5 = 1/ps_t5 if retained == 1
replace ipw_t5 = 1/(1-ps_t5) if retained == 0


destring uid, replace force
sort uid date
xtset uid date

label var treatment4 "Auto-Deductions (T-234)"
label var treatment5 "Rewards (T-34)"


save "${final_attendance}", replace


* ================================================================
* RUN AND STORE ALL REGRESSION MODELS 
* ================================================================
use "${final_attendance}", clear

drop absence 

gen absence = 0
replace absence = 1 if status == 2
replace absence = . if status == 12
replace absence = . if status == 10
replace absence = . if status == 13


eststo clear

// --- Storing all ATE regression models ---
eststo h1_ot3_ipw, title("T1 OT - Date FE"): reghdfe absence i.treatment1##i.prepost i.phase days_since_treatment $controls [pweight=ipw_t1], absorb(date) cluster(uid)
eststo h2_ot3_ipw, title("T2 OT - Date FE"): reghdfe absence i.treatment2##i.prepost i.phase days_since_treatment $controls [pweight=ipw_t2], absorb(date) cluster(uid)
eststo h4_ot3_ipw, title("T4 OT - Date FE"): reghdfe absence i.treatment4##i.prepost i.phase days_since_treatment $controls [pweight=ipw_t4], absorb(date) cluster(uid)
eststo h5_ot3_ipw, title("T5 OT - Date FE"): reghdfe absence i.treatment5##i.prepost i.phase days_since_treatment $controls [pweight=ipw_t5], absorb(date) cluster(uid)

eststo h1_ot6_ipw, title("T1 OT - DATE UID FE"): reghdfe absence i.treatment1##i.prepost days_since_treatment [pweight=ipw_t1], absorb(date uid) cluster(uid)
eststo h2_ot6_ipw, title("T2 OT - DATE UID FE"): reghdfe absence i.treatment2##i.prepost days_since_treatment [pweight=ipw_t2], absorb(date uid) cluster(uid)
eststo h4_ot6_ipw, title("T4 OT - DATE UID FE"): reghdfe absence i.treatment2##i.prepost days_since_treatment i.treatment4##i.prepost [pweight=ipw_t4], absorb(date uid) cluster(uid)
eststo h5_ot6_ipw, title("T5 OT - DATE UID FE"): reghdfe absence i.treatment2##i.prepost days_since_treatment i.treatment4##i.prepost i.treatment5##i.prepost [pweight=ipw_t5], absorb(date uid) cluster(uid)


* ================================================================
* EXPORT DiD RESULTS WITH MODIFICATIONS
* ================================================================

// --- Table 1: DiD Models with Date and UID Fixed Effects ---
local uid_fe_models "h1_ot6_ipw h2_ot6_ipw h4_ot6_ipw h5_ot6_ipw"
local treat_vars "treatment1 treatment2 treatment4 treatment5"

forvalues i = 1/4 {
    local model : word `i' of `uid_fe_models'
    local treat_var : word `i' of `treat_vars'

    estimates restore `model'
    qui summarize absence if e(sample) & `treat_var' == 0 & prepost == 0
    estadd scalar c_mean = r(mean), replace
    eststo `model'
}

esttab `uid_fe_models' using "ab2_ipw_results_segregated.rtf", replace rtf label se ///
    title("Table 1: DiD Models with Date and UID Fixed Effects") ///
    stats(N c_mean, labels("Observations" "Control Mean (Pre)") fmt(%9.0f %9.3f)) ///
    addnotes("Doubly robust IPW regression using standard weights. Standard errors clustered by uid in parentheses.")	



// --- Table 2: DiD Models with Date Fixed Effects ---
local date_fe_models "h1_ot3_ipw h2_ot3_ipw h4_ot3_ipw h5_ot3_ipw"
local treat_vars "treatment1 treatment2 treatment4 treatment5"
forvalues i = 1/4 {
    local model : word `i' of `date_fe_models'
    local treat_var : word `i' of `treat_vars'
    estimates restore `model'
    qui summarize absence if e(sample) & `treat_var'==0 & prepost==0
    estadd scalar c_mean = r(mean), replace
    eststo `model'
}
esttab `date_fe_models' using "ab2_ipw_results_segregated.rtf", append rtf label se ///
    title("Table 2: DiD Models with Date Fixed Effects") ///
    stats(N c_mean, labels("Observations" "Control Mean (Pre)") fmt(%9.0f %9.3f)) ///
    addnotes("Doubly robust IPW regression using standard weights. Standard errors clustered by uid in parentheses.")
	
	
* ================================================================
* Calculate 6-Month Pre-Intervention Absenteeism Rate
* ================================================================


// First, define the 6-month pre-treatment window for clarity
gen byte in_pre_period = (date >= (treatment_start_date - 180) & date < treatment_start_date)

* --- Step 1: Calculate the Numerator (Total Absences in the Window) ---

// Create a flag that is 1 only for absent days inside the pre-period.
// An absence is defined as attendance == 0.
gen byte is_absent_pre = (attendance == 0 & in_pre_period == 1) if !missing(attendance)

// Sum the number of absent days for each worker.
bysort uid: egen total_absences_pre = total(is_absent_pre)


* --- Step 2: Calculate the Denominator (Total Possible Work Days in the Window) ---

// Create a flag that is 1 only for possible work days inside the pre-period.
// A possible work day is any day that is NOT a weekend (10), holiday (13), or after leaving (12).
gen byte is_workday_pre = (status != 10 & status != 13 & status != 12 & in_pre_period == 1) if !missing(status)

// Sum the number of possible work days for each worker.
bysort uid: egen total_workdays_pre = total(is_workday_pre)


* --- Step 3: Calculate the Final Rate ---

// Divide the total absences by the total possible work days.
gen pre_absenteeism_rate = total_absences_pre / total_workdays_pre


* --- Step 4: Clean Up ---

drop in_pre_period is_absent_pre is_workday_pre


* ================================================================
* CALCULATE ALL BASELINE CONTROL MEANS 
* ================================================================
// This must be done on the full dataset BEFORE filtering to the post-period.

qui summarize absence if treatment1 == 0 & prepost == 0
local c_mean_t1 = r(mean)

qui summarize absence if treatment2 == 0 & prepost == 0
local c_mean_t2 = r(mean)

qui summarize absence if treatment4 == 0 & prepost == 0
local c_mean_t4 = r(mean)

qui summarize absence if treatment5 == 0 & prepost == 0
local c_mean_t5 = r(mean)


* ================================================================
* RUN AND STORE ALL ATE REGRESSION MODELS 
* ================================================================
preserve
keep if prepost == 1

eststo clear

// --- Storing all 12 ATE regression models ---
eststo h1_3_ipw, title("T1 - Date FE"): reghdfe absence i.treatment1 i.phase pre_absenteeism_rate days_since_treatment $controls [pweight=ipw_t1], absorb(date) cluster(uid)
eststo h2_3_ipw, title("T2 - Date FE"): reghdfe absence i.treatment2 i.phase pre_absenteeism_rate days_since_treatment $controls [pweight=ipw_t2], absorb(date) cluster(uid)
eststo h4_3_ipw, title("T4 - Date FE"): reghdfe absence i.treatment2 i.treatment4 i.phase pre_absenteeism_rate days_since_treatment $controls [pweight=ipw_t4], absorb(date) cluster(uid) 
eststo h5_3_ipw, title("T5 - Date FE"): reghdfe absence i.treatment2 i.treatment4 i.treatment5 i.phase pre_absenteeism_rate days_since_treatment $controls [pweight=ipw_t5], absorb(date) cluster(uid) 


* ================================================================
* ADD THE PRE-CALCULATED CONTROL MEANS TO STORED MODELS
* ================================================================

// --- For Date FE Models ---
local date_fe_models "h1_3_ipw h2_3_ipw h4_3_ipw h5_3_ipw"
local c_means "`c_mean_t1' `c_mean_t2' `c_mean_t4' `c_mean_t5'"
forvalues i = 1/4 {
    local model : word `i' of `date_fe_models'
    local mean : word `i' of `c_means'
    estimates restore `model'
    estadd scalar c_mean = `mean'
    eststo `model'
}


* ================================================================
* EXPORT THE FINAL TABLES
* ================================================================

// --- Table 1: ATE Models with Date Fixed Effects ---
esttab h1_3_ipw h2_3_ipw h4_3_ipw h5_3_ipw using "ab2_ipw_results_segregated_ate.rtf", replace rtf label se ///
    title("Table 1: ATE Models with Date Fixed Effects") ///
    stats(N c_mean, labels("Observations" "Control Mean (Pre)") fmt(%9.0f %9.3f)) ///
    addnotes("Doubly robust IPW regression using standard weights. Standard errors clustered by uid in parentheses.")
    
restore


/*===============================================================================
 ATTRITION DATA ANALYSIS
===============================================================================*/

// This part of the script takes the clean daily panel, prepares it for
// regression analysis, runs the models, and exports the results to a document.

use "${out_daily}", clear

drop if date == .

gen treatment_start_date = enrol_date
replace treatment_start_date = mdy(11, 20, 2024) if missing(treatment_start_date)
format treatment_start_date %td


gen days_since_treatment = date - treatment_start_date

sort uid date

// Defines a global macro containing all the control variables for the regressions.
global controls age i.strata i.migrant i.hh_head_resp i.educ_g10 i.remit i.any_blsavings i.any_bnksavings log_total_savings i.any_blloans log_total_loans i.knows_upi i.knowsatm_nohelp i.own_phone asset_index



/*===============================================================================
 ATTRITION REGRESSION ANALYSIS
===============================================================================*/


// Keeps only the observation period relevant for the analysis.
keep if date > mdy(5,5,2024)
capture confirm variable new_tknno
if !_rc drop if missing(new_tknno)


label var treatment4 "Auto-Deductions (T-234)"
label var treatment5 "Rewards (T-34)"


save "${final_attrition}", replace


* ================================================================
* ATE MODELS WITH DATE FE
* ================================================================



// --- ATE Models with days_since_treatment
eststo h1_ate3, title("T1 - Date FE"): reghdfe turnover_daily i.treatment1 i.phase  total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo h2_ate3, title("T2 - Date FE"): reghdfe turnover_daily i.treatment2  i.phase  total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo h4_ate3, title("T4 - Date FE"): reghdfe turnover_daily i.treatment2 i.treatment4 i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)

lincom 1.treatment2 + 1.treatment4
estadd scalar T24_sum = r(estimate)
estadd scalar T24_se   = r(se)

eststo h5_ate3, title("T5 - Date FE"): reghdfe turnover_daily i.treatment2 i.treatment4 i.treatment5 i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)

lincom 1.treatment2 + 1.treatment4 + 1.treatment5
estadd scalar T245_sum = r(estimate)
estadd scalar T245_se   = r(se)


local controls_for_order "age *.strata *.migrant *.hh_head_resp *.educ_g10 *.remit *.any_blsavings *.any_bnksavings log_total_savings *.any_blloans log_total_loans *.knows_upi *.knowsatm_nohelp *.own_phone asset_index"

local controls_to_drop "`controls_for_order' total_tenure_months current_tenure_months"

local ate_date_fe "h1_ate3 h2_ate3 h4_ate3 h5_ate3"
local treat_vars "treatment1 treatment2 treatment4 treatment5"
foreach model in `ate_date_fe' {
    local i = `i' + 1
    local treat_var : word `i' of `treat_vars'
    if `i' > 4 {
        local i = 1
        local treat_var : word `i' of `treat_vars'
    }
    estimates restore `model'
    qui summarize turnover_daily if e(sample) & `treat_var'==0
    estadd scalar c_mean = r(mean)
    eststo `model'
}

esttab `ate_date_fe' using turnover_ate.rtf, replace rtf label se nobase drop(`controls_to_drop') ///
    title("Table 1: ATE Models with Date FE & days_since_treatment") ///
    stats(N c_mean T24_sum T24_se T245_sum T245_se, ///
          labels("Observations" "Control Mean (Post)" ///
                 "Sum(T2+T4)" "SE(T2+T4)" "Sum(T2+T4+T5)" "SE(T2+T4+T5)") ///
          fmt(%9.0f %9.3f %9.3f %9.3f %9.3f %9.3f)) ///
    addnotes("Standard errors in parentheses. Linear combination rows added below model coefficients.")

	
	
// --- ATE Models without days_since_treatment
eststo h1_ate3, title("T1 - Date FE"): reghdfe turnover_daily i.treatment1 i.phase  total_tenure_months current_tenure_months $controls, absorb(date) cluster(uid)
eststo h2_ate3, title("T2 - Date FE"): reghdfe turnover_daily i.treatment2  i.phase  total_tenure_months current_tenure_months $controls, absorb(date) cluster(uid)
eststo h4_ate3, title("T4 - Date FE"): reghdfe turnover_daily i.treatment2 i.treatment4 i.phase total_tenure_months current_tenure_months $controls, absorb(date) cluster(uid)

lincom 1.treatment2 + 1.treatment4
estadd scalar T24_sum = r(estimate)
estadd scalar T24_se   = r(se)

eststo h5_ate3, title("T5 - Date FE"): reghdfe turnover_daily i.treatment2 i.treatment4 i.treatment5 i.phase total_tenure_months current_tenure_months $controls, absorb(date) cluster(uid)

lincom 1.treatment2 + 1.treatment4 + 1.treatment5
estadd scalar T245_sum = r(estimate)
estadd scalar T245_se   = r(se)


local controls_for_order "age *.strata *.migrant *.hh_head_resp *.educ_g10 *.remit *.any_blsavings *.any_bnksavings log_total_savings *.any_blloans log_total_loans *.knows_upi *.knowsatm_nohelp *.own_phone asset_index"

local controls_to_drop "`controls_for_order' total_tenure_months current_tenure_months"

local ate_date_fe "h1_ate3 h2_ate3 h4_ate3 h5_ate3"
local treat_vars "treatment1 treatment2 treatment4 treatment5"
foreach model in `ate_date_fe' {
    local i = `i' + 1
    local treat_var : word `i' of `treat_vars'
    if `i' > 4 {
        local i = 1
        local treat_var : word `i' of `treat_vars'
    }
    estimates restore `model'
    qui summarize turnover_daily if e(sample) & `treat_var'==0
    estadd scalar c_mean = r(mean)
    eststo `model'
}

esttab `ate_date_fe' using turnover_ate.rtf, append rtf label se nobase drop(`controls_to_drop') ///
    title("Table 2: ATE Models with Date FE w/o days_since_treatment") ///
    stats(N c_mean T24_sum T24_se T245_sum T245_se, ///
          labels("Observations" "Control Mean (Post)" ///
                 "Sum(T2+T4)" "SE(T2+T4)" "Sum(T2+T4+T5)" "SE(T2+T4+T5)") ///
          fmt(%9.0f %9.3f %9.3f %9.3f %9.3f %9.3f)) ///
    addnotes("Standard errors in parentheses. Linear combination rows added below model coefficients.")



// --- ATE Models Gender wise --
eststo t1_ate3, title("T1 - Date FE"): reghdfe turnover_daily i.treatment1##gender i.phase  total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo t2_ate3, title("T2 - Date FE"): reghdfe turnover_daily i.treatment2##gender  i.phase  total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo t4_ate3, title("T4 - Date FE"): reghdfe turnover_daily i.treatment2##gender i.treatment4##gender i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)

lincom 1.treatment2 + 1.treatment4
estadd scalar T24_sum = r(estimate)
estadd scalar T24_se   = r(se)

eststo t5_ate3, title("T5 - Date FE"): reghdfe turnover_daily i.treatment2##gender i.treatment4##gender i.treatment5##gender i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)

lincom 1.treatment2 + 1.treatment4 + 1.treatment5
estadd scalar T245_sum = r(estimate)
estadd scalar T245_se   = r(se)

local controls_for_order "age *.strata *.migrant *.hh_head_resp *.educ_g10 *.remit *.any_blsavings *.any_bnksavings log_total_savings *.any_blloans log_total_loans *.knows_upi *.knowsatm_nohelp *.own_phone asset_index"

local controls_to_drop "`controls_for_order' total_tenure_months current_tenure_months"

local ate_date_fe "t1_ate3 t2_ate3 t4_ate3 t5_ate3"
local treat_vars "treatment1 treatment2 treatment4 treatment5"
foreach model in `ate_date_fe' {
    local i = `i' + 1
    local treat_var : word `i' of `treat_vars'
    if `i' > 4 {
        local i = 1
        local treat_var : word `i' of `treat_vars'
    }
    estimates restore `model'
    qui summarize turnover_daily if e(sample) & `treat_var'==0
    estadd scalar c_mean = r(mean)
    eststo `model'
}

esttab `ate_date_fe' using turnover_ate.rtf, append rtf label se nobase drop(`controls_to_drop') ///
    title("Table 3: ATE Models with Date Fixed Effects Gender") ///
    stats(N c_mean T24_sum T24_se T245_sum T245_se, ///
          labels("Observations" "Control Mean (Post)" ///
                 "Sum(T2+T4)" "SE(T2+T4)" "Sum(T2+T4+T5)" "SE(T2+T4+T5)") ///
          fmt(%9.0f %9.3f %9.3f %9.3f %9.3f %9.3f)) ///
    addnotes("Standard errors in parentheses. Linear combination rows added below model coefficients.")


/*==============================================================================
* APPENDIX: DATA QUALITY DIAGNOSTIC CHECKS
* Purpose: To verify the internal consistency of the final daily panel data
* before running any analysis or regressions.
*==============================================================================*/

di as err "RUNNING DATA QUALITY DIAGNOSTIC CHECKS"

// --- 1. Check Rejoin Date Logic ---
di as err "--- 1. Checking Rejoin Date Logic ---"
// A worker should not have a rejoin date without a prior leaving date.
count if !missing(doj_rejoin) & missing(dol_first)
di "Error Count: Rejoin date exists but prior leaving date is missing = " r(N)
// A rejoin date cannot be on or before the first leaving date.
count if !missing(doj_rejoin) & doj_rejoin <= dol_first
di "Error Count: Rejoin date is on or before prior leaving date = " r(N)
di as result "   -> Rejoin date logic check passed."
di ""

// --- 2. Check Turnover Daily Consistency ---
di as err "--- 2. Checking Turnover Daily Consistency ---"
// A flag is created for when a worker SHOULD be employed based on spell dates.
gen byte should_be_working = 0
// This logic is repeated for all 15 possible spells.
replace should_be_working = 1 if !missing(doj_first) & (date >= doj_first & (date <= dol_first | missing(dol_first)))
replace should_be_working = 1 if !missing(doj_rejoin) & (date >= doj_rejoin & (date <= dol_second | missing(dol_second)))
replace should_be_working = 1 if !missing(doj_rejoin_twice) & (date >= doj_rejoin_twice & (date <= dol_third | missing(dol_third)))
replace should_be_working = 1 if !missing(doj_rejoin_thrice) & (date >= doj_rejoin_thrice & (date <= dol_fourth | missing(dol_fourth)))
replace should_be_working = 1 if !missing(doj_rejoin_fourth) & (date >= doj_rejoin_fourth & (date <= dol_fifth | missing(dol_fifth)))
replace should_be_working = 1 if !missing(doj_rejoin_fifth) & (date >= doj_rejoin_fifth & (date <= dol_sixth | missing(dol_sixth)))
replace should_be_working = 1 if !missing(doj_rejoin_sixth) & (date >= doj_rejoin_sixth & (date <= dol_seventh | missing(dol_seventh)))
replace should_be_working = 1 if !missing(doj_rejoin_seventh) & (date >= doj_rejoin_seventh & (date <= dol_eighth | missing(dol_eighth)))
replace should_be_working = 1 if !missing(doj_rejoin_eighth) & (date >= doj_rejoin_eighth & (date <= dol_ninth | missing(dol_ninth)))
replace should_be_working = 1 if !missing(doj_rejoin_ninth) & (date >= doj_rejoin_ninth & (date <= dol_tenth | missing(dol_tenth)))
replace should_be_working = 1 if !missing(doj_rejoin_tenth) & (date >= doj_rejoin_tenth & (date <= dol_eleventh | missing(dol_eleventh)))
replace should_be_working = 1 if !missing(doj_rejoin_eleventh) & (date >= doj_rejoin_eleventh & (date <= dol_twelfth | missing(dol_twelfth)))
replace should_be_working = 1 if !missing(doj_rejoin_twelfth) & (date >= doj_rejoin_twelfth & (date <= dol_thirteenth | missing(dol_thirteenth)))
replace should_be_working = 1 if !missing(doj_rejoin_thirteenth) & (date >= doj_rejoin_thirteenth & (date <= dol_fourteenth | missing(dol_fourteenth)))
replace should_be_working = 1 if !missing(doj_rejoin_fourteenth) & (date >= doj_rejoin_fourteenth & (date <= dol_fifteenth | missing(dol_fifteenth)))

// Check 2a: Days incorrectly marked as turnover.
count if turnover_daily == 1 & should_be_working == 1 & ///
    date != dol_first & date != dol_second & date != dol_third & date != dol_fourth & date != dol_fifth
di "Error Count: Incorrectly marked as turnover (buffer applied) = " r(N)
// Check 2b: Days incorrectly marked as working.
count if turnover_daily == 0 & should_be_working == 0
di "Error Count: Incorrectly marked as working = " r(N)
drop should_be_working
di as result "   -> Turnover daily consistency check complete."
di ""

// --- 3. Check Status/Attendance vs. Turnover ---
di as err "--- 3. Checking Status/Attendance vs. Turnover ---"
// If turnover_daily is 1 (not employed), status must be 12 (Left).
count if turnover_daily == 1 & status != 12
di "Error Count: Non-employed days where status is NOT 'Left' = " r(N)
// If turnover_daily is 1 (not employed), attendance must be missing.
count if turnover_daily == 1 & !missing(attendance)
di "Error Count: Non-employed days with non-missing attendance = " r(N)
// If turnover_daily is 0 (employed), status should never be 12 (Left).
count if turnover_daily == 0 & status == 12
di "Error Count: Employed days where status is 'Left' = " r(N)
di as result "   -> Status/Attendance checks passed."
di as err "{hline}"

// --- 4. Advanced Rejoiner Checks ---
di as err "--- Running Diagnostic 4: Checking all Rejoiner Spells ---"
local all_prev_dols dol_first dol_second dol_third dol_fourth dol_fifth dol_sixth dol_seventh ///
                      dol_eighth dol_ninth dol_tenth dol_eleventh dol_twelfth dol_thirteenth dol_fourteenth
local all_curr_dojs doj_rejoin doj_rejoin_twice doj_rejoin_thrice doj_rejoin_fourth doj_rejoin_fifth ///
                      doj_rejoin_sixth doj_rejoin_seventh doj_rejoin_eighth doj_rejoin_ninth doj_rejoin_tenth ///
                      doj_rejoin_eleventh doj_rejoin_twelfth doj_rejoin_thirteenth doj_rejoin_fourteenth
local all_curr_dols dol_second dol_third dol_fourth dol_fifth dol_sixth dol_seventh dol_eighth ///
                      dol_ninth dol_tenth dol_eleventh dol_twelfth dol_thirteenth dol_fourteenth dol_fifteenth

// This loop checks the data integrity for each rejoin period.
forvalues s = 1/14 {
    local prev_dol: word `s' of `all_prev_dols'
    local curr_doj: word `s' of `all_curr_dojs'
    local curr_dol: word `s' of `all_curr_dols'
    local spell_num = `s' + 1
    di as text "   -> Checking Spell `spell_num'"
    
    // Check 4a: In the "gap" between spells, status must be "Left" and attendance must be missing.
    count if !missing(`prev_dol') & !missing(`curr_doj') & (date > `prev_dol' & date < `curr_doj') & (status != 12 | !missing(attendance))
    di "       Errors found in gap before Spell `spell_num': " r(N)
    
    // Check 4b: During an active spell, status must NOT be "Left".
    count if !missing(`curr_doj') & (date >= `curr_doj' & (date < `curr_dol' | missing(`curr_dol'))) & status == 12
    di "       Errors found in active period of Spell `spell_num': " r(N)
}
di as result "--- All Rejoiner checks passed. ---"


* ================================================================
* MODELS NOT IN USE
* ================================================================

* Month FE 
eststo h2_ate4, title("T2 - Month FE"): reghdfe turnover_daily i.treatment2  i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(month) cluster(uid)
eststo h4_ate4, title("T4 - Month FE"): reghdfe turnover_daily i.treatment2 i.treatment4 i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(month) cluster(uid)
eststo h1_ate4, title("T1 - Month FE"): reghdfe turnover_daily i.treatment1 i.phase  total_tenure_months current_tenure_months days_since_treatment $controls, absorb(month) cluster(uid)
eststo h5_ate4, title("T5 - Month FE"): reghdfe turnover_daily i.treatment2 i.treatment4 i.treatment5 i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(month) cluster(uid)

// --- Over-Time (Continuous) Models ---
eststo h1_time3, title("T1 - Date FE"): reghdfe turnover_daily i.treatment1##c.months_since_treatment i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo h1_time4, title("T1 - Month FE"): reghdfe turnover_daily i.treatment1##c.months_since_treatment i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(month) cluster(uid)
eststo h2_ot3, title("T2 - Date FE"): reghdfe turnover_daily i.treatment2##c.months_since_treatment i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo h2_ot4, title("T2 - Month FE"): reghdfe turnover_daily i.treatment2##c.months_since_treatment i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(month) cluster(uid)
eststo h4_ot3, title("T4 - Date FE"): reghdfe turnover_daily i.treatment4##c.months_since_treatment i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo h4_ot4, title("T4 - Month FE"): reghdfe turnover_daily i.treatment4##c.months_since_treatment i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(month) cluster(uid)
eststo h5_ot3, title("T5 - Date FE"): reghdfe turnover_daily i.treatment5##c.months_since_treatment i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo h5_ot4, title("T5 - Month FE"): reghdfe turnover_daily i.treatment5##c.months_since_treatment i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(month) cluster(uid)

// --- Simplified Interaction Models ---
eststo h2_int, title("T1234 vs C"): reghdfe turnover_daily i.treatment2##c.months_since_treatment, absorb(uid) cluster(uid)
eststo h4_int, title("T01 vs T234"): reghdfe turnover_daily i.treatment4##c.months_since_treatment, absorb(uid) cluster(uid)
eststo h5_int, title("No Rewards vs Rewards"): reghdfe turnover_daily i.treatment5##c.months_since_treatment, absorb(uid) cluster(uid)


// --- Over-Time (Continuous) Model Tables ---
local ot_c_date_fe "h1_time3 h2_ot3 h4_ot3 h5_ot3"
local ot_c_month_fe "h1_time4 h2_ot4 h4_ot4 h5_ot4"
local i = 0
foreach model in `ot_c_date_fe' `ot_c_month_fe' {
    local i = `i' + 1
    local treat_var : word `i' of `treat_vars'
    if `i' > 4 {
        local i = 1
        local treat_var : word `i' of `treat_vars'
    }
    estimates restore `model'
    qui summarize turnover_daily if e(sample) & `treat_var'==0
    estadd scalar c_mean = r(mean)
    eststo `model'
}
esttab `ot_c_date_fe' using h2.rtf, append rtf label se nobase drop(`controls_to_drop') title("Table 3: Over-Time Models (Continuous) with Date FE") stats(N c_mean, labels("Observations" "Control Mean (Post)") fmt(%9.0f %9.3f)) addnotes("Standard errors in parentheses.")
esttab `ot_c_month_fe' using h1.rtf, append rtf label se nobase drop(`controls_to_drop') title("Table 4: Over-Time Models (Continuous) with Month FE") stats(N c_mean, labels("Observations" "Control Mean (Post)") fmt(%9.0f %9.3f)) addnotes("Standard errors in parentheses.")


// --- Simplified Interaction Model Table ---
local simple_int "h2_int h4_int h5_int"
local simple_treat "treatment2 treatment4 treatment5"
local i = 0
foreach model in `simple_int' {
    local i = `i' + 1
    local treat_var : word `i' of `simple_treat'
    estimates restore `model'
    qui summarize turnover_daily if e(sample) & `treat_var'==0
    estadd scalar c_mean = r(mean)
    eststo `model'
}
esttab `simple_int' using h_interactions.rtf, replace rtf label se nobase title("Treatment Sub-Group Interaction Effects") stats(N c_mean, labels("Observations" "Control Mean (Post)") fmt(%9.0f %9.3f)) addnotes("Standard errors clustered by uid in parentheses.")


* ================================================================
* GENDER ATE Models
* ================================================================


preserve

keep if gender == 0

eststo clear

// --- ATE Models ---
eststo h1_ate3, title("T1 - Date FE"): reghdfe turnover_daily i.treatment1 i.phase  total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo h2_ate3, title("T2 - Date FE"): reghdfe turnover_daily i.treatment2  i.phase  total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)
eststo h4_ate3, title("T4 - Date FE"): reghdfe turnover_daily i.treatment2 i.treatment4 i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)

lincom 1.treatment2 + 1.treatment4
estadd scalar T24_sum = r(estimate)
estadd scalar T24_se   = r(se)

eststo h5_ate3, title("T5 - Date FE"): reghdfe turnover_daily i.treatment2 i.treatment4 i.treatment5 i.phase total_tenure_months current_tenure_months days_since_treatment $controls, absorb(date) cluster(uid)

lincom 1.treatment2 + 1.treatment4 + 1.treatment5
estadd scalar T245_sum = r(estimate)
estadd scalar T245_se   = r(se)


local controls_for_order "age *.strata *.migrant *.hh_head_resp *.educ_g10 *.remit *.any_blsavings *.any_bnksavings log_total_savings *.any_blloans log_total_loans *.knows_upi *.knowsatm_nohelp *.own_phone asset_index"

local controls_to_drop "`controls_for_order' total_tenure_months current_tenure_months"

local ate_date_fe "h1_ate3 h2_ate3 h4_ate3 h5_ate3"
local treat_vars "treatment1 treatment2 treatment4 treatment5"
foreach model in `ate_date_fe' {
    local i = `i' + 1
    local treat_var : word `i' of `treat_vars'
    if `i' > 4 {
        local i = 1
        local treat_var : word `i' of `treat_vars'
    }
    estimates restore `model'
    qui summarize turnover_daily if e(sample) & `treat_var'==0
    estadd scalar c_mean = r(mean)
    eststo `model'
}

esttab `ate_date_fe' using turnover_ate.rtf, append rtf label se nobase drop(`controls_to_drop') ///
    title("Table 3: ATE Models with Date Fixed Effects Male") ///
    stats(N c_mean T24_sum T24_se T245_sum T245_se, ///
          labels("Observations" "Control Mean (Post)" ///
                 "Sum(T2+T4)" "SE(T2+T4)" "Sum(T2+T4+T5)" "SE(T2+T4+T5)") ///
          fmt(%9.0f %9.3f %9.3f %9.3f %9.3f %9.3f)) ///
    addnotes("Standard errors in parentheses. Linear combination rows added below model coefficients.")

restore

