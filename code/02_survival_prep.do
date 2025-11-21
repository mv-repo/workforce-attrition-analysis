/*===============================================================================
* File: pls_admin_analysis.do
* Purpose: This script takes wide, person-level employment data, reshapes it to
* a long, spell-level format, and sets it up for a multi-spell
* survival analysis. The timescale is days since the study began, and
* a "failure" is defined only as an exit from the final observed spell.
* Author: Mohit Verma
* Created: 08 Sept 2025
*===============================================================================*/

clear all
set more off


// Define the path to data file
global out_daily "/Users/mohitverma/Downloads/survival_prep.dta" 
global out_survival "/Users/mohitverma/Downloads/pls_survival.dta" 


// Define the uniform study end date for everyone
local study_end_date = mdy(7, 31, 2025)

use "${out_daily}", clear 

duplicates drop uid, force 
* ---------------------------------------------------
* Step 1: Standardize spell variables (doj, dol)
* ---------------------------------------------------
// The purpose of this section is to robustly prepare the wide data for the `reshape` command.
// It creates a full set of standardized variable "slots" (doj_1, dol_1, etc.)
// and then maps the existing, inconsistently named variables into them.

forvalues i = 1/15 {
    capture confirm variable doj_`i'
    if _rc gen double doj_`i' = .
    capture confirm variable dol_`i'
    if _rc gen double dol_`i' = .
}

// Define the lists of old, inconsistent variable names.
local doj_map "doj_first doj_rejoin doj_rejoin_twice doj_rejoin_thrice doj_rejoin_fourth doj_rejoin_fifth doj_rejoin_sixth doj_rejoin_seventh doj_rejoin_eighth doj_rejoin_ninth doj_rejoin_tenth doj_rejoin_eleventh doj_rejoin_twelfth doj_rejoin_thirteenth doj_rejoin_fourteenth"
local dol_map "dol_first dol_second dol_third dol_fourth dol_fifth dol_sixth dol_seventh dol_eighth dol_ninth dol_tenth dol_eleventh dol_twelfth dol_thirteenth dol_fourteenth dol_fifteenth"

// Loop through the old DOJ names and copy their data into the standardized slots (doj_1, doj_2, etc.).
local i = 1
foreach v of local doj_map {
    capture confirm variable `v'
    if !_rc replace doj_`i' = `v' if !missing(`v')
    local ++i
}

// Loop through the old DOL names and copy their data into the standardized slots (dol_1, dol_2, etc.).
local i = 1
foreach v of local dol_map {
    capture confirm variable `v'
    if !_rc replace dol_`i' = `v' if !missing(`v')
    local ++i
}

// Ensure all newly populated date variables have the correct Stata date format.
forvalues i = 1/15 {
    format doj_`i' %td
    format dol_`i' %td
}

// Define the fixed start and end dates for the overall study period.
local int_start = td(06may2024)
local int_end   = td(30june2025)


* ---------------------------------------------------
* Step 3: Reshape wide → long spells
* ---------------------------------------------------
reshape long doj_ dol_, i(uid) j(spell_num)
rename doj_ spell_start
rename dol_ spell_end

drop if missing(spell_start)

// Filter the data to keep only spells that have some overlap with the study period.
gen temp_end = cond(missing(spell_end), `int_end', spell_end)
keep if (spell_start <= `int_end') & (temp_end >= `int_start')
drop temp_end


// Sort the data by person and chronological order of their spells
sort uid spell_start

// Generate a new variable that counts the spells (1, 2, 3...) for each person
// `_n` is Stata's internal counter for the observation number within each `uid` group.
bysort uid: gen spell_order_in_study = _n

// You can now drop the old spell_num and use the new one
drop spell_num
rename spell_order_in_study spell_num

// You can check the result for a specific person
// list uid spell_num spell_start if uid == 12345 // replace with a real uid


* ---------------------------------------------------
* Step 4: Build survival-time variables
* ---------------------------------------------------
// Create the key variables needed for the `stset` command. The timescale here is
// calendar time, measured in days since the study started (`int_start`).

// A temporary failure flag is created for any spell that ends in attrition within the study period.
gen byte failure = !missing(spell_end) & spell_end <= `int_end'

// The end date of observation for each spell is either the actual leaving date or the study end date.
gen end_date = cond(failure==1, spell_end, `int_end')
format end_date %td

// The start date of observation for each spell is the LATER of the spell's start or the study's start.
gen start_date = cond(spell_start < `int_start', `int_start', spell_start)

// Create time0 (entry time) and time1 (exit time) on the "days since study start" scale.
gen time0 = start_date - `int_start'
gen time1 = end_date   - `int_start'


// Identify the final spell for each person to define the final failure event.
bysort uid (spell_end spell_start): gen is_last = (_n == _N)

// NOTE: This is a specific analytical choice. We are defining a "failure" as an
// attrition event that occurs ONLY in the person's final observed spell.
// All intermediate spells that end in attrition are treated as censored in this setup.
gen failure_new = 0
replace failure_new = failure if is_last

label define failurelbl 0 "Censored" 1 "Failure"
label values failure_new failurelbl

// Check the distribution of the final failure variable.
tab failure_new

* ---------------------------------------------------
* Step 6: Declare survival data
* ---------------------------------------------------
// Sort the data by person and spell start time. This is best practice before stset.
sort uid spell_start

// Declare the data as multi-spell survival data.
stset time1, id(uid) failure(failure_new) enter(time0)

* -----------------------------------------------------------
* Diagnostic Checks for Survival Data Prepared with stset
* -----------------------------------------------------------

// 1. Checks for any spells with a negative or zero duration. Should return 0 obs.
list uid spell_num start_date end_date time0 time1 failure if time0 < 0 | time1 <= time0

// 2. This is another way to check for invalid time intervals. Should return 0 obs.
list uid spell_num start_date end_date time0 time1 failure if time0 >= time1

// 3. Intends to check if any person has zero valid spells after filtering.
bysort uid (time0 time1): gen spell_count = _N
list uid spell_num time0 time1 if spell_count == 0

// 4. Checks for duplicate records based on person and spell number. Should be none.
duplicates report uid spell_num

// 5. Checks if the `time0` and `time1` variables were calculated correctly from the date variables. Should return 0 obs.
list uid spell_num start_date end_date time0 time1 if time0 != start_date - `int_start' | time1 != end_date - `int_start'

// 6. Verifies the logic that failure is only marked in the final spell.
bysort uid (spell_start spell_end): gen last_failure = failure[_N]
list uid spell_num spell_start spell_end failure last_failure if failure != last_failure & last_failure == 1

// 7. Reviews observations that `stset` might have had trouble with, by checking its internal variables.
list uid spell_num start_date end_date time0 time1 failure _t0 _t _d if _t0 >= _t | missing(_t0) | missing(_t)

// 8. Counts the total number of observations with invalid intervals before the final fix.
count if time0 >= time1

// 9. Provides a summary of the final time variables.
summarize time0 time1

save "${out_survival}", replace

