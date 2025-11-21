/*===============================================================================
* File: pls_admin_setup.do
* Purpose: This script cleans and processes raw HR data to track employee
* attrition and attendance. It creates a daily panel dataset ready for analysis.
* Author: Mohit Verma
* Created: 08 Sep 2025
*===============================================================================*/

cls
set more off
clear all

set maxvar 120000

/*==============================================================================
  PART 0: FILE PATHS AND MACROS
------------------------------------------------------------------------------*/

// --- Input Files ---
global uid_map "/Users/mohitverma/Downloads/Empcode_StudyID_deidentified.dta"
global raw_attendance_dir "/Users/mohitverma/Downloads/Transfer"
global attrition_cleaned "/Users/mohitverma/Downloads/pls_attrition_cleaned.dta"
global treatstat_data "/Users/mohitverma/Downloads/pls_treatstat4.2_20nov24.dta"
global implementation_data "/Users/mohitverma/Downloads/pls_implementation_clean.dta"
global control_csv "/Users/mohitverma/Downloads/pls_purecon_consent_03122024_WIDE.csv"
global hfs_round_data "/Users/mohitverma/Downloads/pls_datafor_monthly_analysis.dta"
global bl_data "/Users/mohitverma/Downloads/pls_hfs_sample5m_241224.dta"
global study_sample "/Users/mohitverma/Downloads/pls_datafor_takeup_analysis.dta"


// --- Output Files ---
global out_cleaned_attendance "/Users/mohitverma/Downloads/pls_attendance_cleaned.dta"
global out_final_prepost "/Users/mohitverma/Downloads/pls_attendance_cleaned_prepost.dta"
global out_daily "/Users/mohitverma/Downloads/pls_daily_attrition.dta"

// --- Regression Output Tables ---
global out_doc_dir "/Users/mohitverma/Downloads" // Directory for result tables


// Important dates for the analysis are also defined here as local macros.
local bl_start = td(01dec2023)     
local censor_date = mdy(5,29,2024)  // The official end date for tenure calculation.
local impute_cutoff = td(30nov2024) // A later date used for imputing leaving dates for some factories.



/*===============================================================================
 PART 1: PREPARE A BASELINE MAP (Linking UID to Study ID)
===============================================================================*/
// The goal here is to create a clean, reliable list that maps each person's
// unique ID (`uid`) to the specific employee code (`tokenno`) they had at the
// start of the study. Workers get new employee codes if they rejoin, so
// this 'baseline map' is crucial to track them consistently over time.

use ${bl_data}, clear

// First, the code ensures the ID variables are numeric and clean.
destring uid, force replace
capture confirm variable tokenno
if _rc {
    // Sometimes the variable has a different name, so this checks for an alternative.
    capture confirm variable new_tknno
    if !_rc rename new_tknno tokenno
}
destring tokenno, force replace

// Now, the script creates the unique mapping.
keep uid empcode group tokenno
sort uid tokenno
bysort uid: keep if _n==1 // If a person has multiple entries, only the first one is kept.

// These variables are renamed to make it clear they are from the baseline period.
rename tokenno study_tokenno
rename empcode study_empcode
format uid %12.0f
label var study_tokenno "Baseline tokenno"
label var study_empcode "Empcode at baseline"

// Finally, this map is saved in a temporary file to be used in the next step.
tempfile baseline_map
save `baseline_map'

/*===============================================================================
 PART 2: LOAD EMPLOYMENT HISTORY (SPELL DATA) & MERGE BASELINE INFO
===============================================================================*/
// This section loads the main dataset, which contains every employment "spell"
// (i.e., each period of employment) for all workers.

use ${uid_map}, clear

// Just like before, the ID variables are cleaned up.
capture confirm variable uid
if _rc {
    di as err "UID not found in spells file. Exiting."
    exit 198
}
destring uid, force replace
capture confirm variable new_tknno
if !_rc {
    rename new_tknno tokenno
}
destring tokenno, force replace

// Here, the script merges the baseline map created in Part 1. This adds the
// `study_tokenno` to every spell record for each person. This is how all their
// employment history is linked back to their identity at the start of the study.
merge m:1 uid using `baseline_map'
tab _merge   // Checking the merge quality is good practice.
drop _merge
keep if factory == "B14"

/*===============================================================================
 PART 2.5: IMPUTE MISSING LEAVING DATES (DOLs)
===============================================================================*/
// Sometimes, a worker's record shows they rejoined, but the leaving date from
// their previous spell is missing. This section fills in those gaps.

// --- Step 1: Calculate the average time it takes for someone to rejoin. ---
sort uid doj
bysort uid: gen next_doj = doj[_n+1] // Finds the start date of the *next* spell.
format next_doj %td
gen rejoin_gap_days = next_doj - dol // Calculates the gap for spells where data is available.
summarize rejoin_gap_days, meanonly
scalar avg_rejoin_gap = r(mean) // Stores the average gap.

// --- Step 2: Apply the imputation logic. ---
// This is only done for spells that are missing a leaving date but are followed by another spell.
gen byte needs_imputation = missing(dol) & !missing(next_doj)

// The primary method: estimate the leaving date by subtracting the average gap from the next start date.
gen imputed_dol_primary = next_doj - avg_rejoin_gap

// A fallback method, just in case: place the leaving date halfway between the two start dates.
gen imputed_dol_fallback = floor(doj + (next_doj - doj) / 2)

// Now, apply the imputation.
gen imputed_dol = imputed_dol_primary if needs_imputation == 1
// If the primary method gives an invalid result (e.g., leaving after rejoining), the fallback is used.
replace imputed_dol = imputed_dol_fallback if needs_imputation == 1 & (imputed_dol <= doj | missing(imputed_dol))

// Fill in the missing `dol` values with the imputed dates.
replace dol = imputed_dol if needs_imputation == 1
format dol %td

// --- Step 3: Clean up the temporary helper variables. ---
drop next_doj rejoin_gap_days needs_imputation imputed_*

/*===============================================================================
 PART 3: CALCULATE TENURE
===============================================================================*/
// In this section, the script calculates two different kinds of tenure for each worker.

// First, it ensures the date variables are in Stata's date format.
capture confirm variable doj
if _rc di as err "doj not found"
else format doj %td
capture confirm variable dol
if !_rc format dol %td

// --- TOTAL TENURE: A worker's cumulative time with the company. ---

// This defines the cutoff date for the calculation.
local tenure_cutoff_date = mdy(5,29,2024)

// 1. For each individual spell, its length is calculated, capped at the cutoff date.
gen temp_end_date = cond(missing(dol), `tenure_cutoff_date', dol)
gen spell_end_date_capped = min(temp_end_date, `tenure_cutoff_date')
format spell_end_date_capped %td
gen capped_spell_tenure = max(0, spell_end_date_capped - doj)

// 2. Then, the lengths of all spells are summed for each worker to get their total tenure.
bysort uid: egen total_tenure_days = total(capped_spell_tenure)
gen total_tenure_years = total_tenure_days / 365.25

// --- CURRENT TENURE: Length of the continuous spell active on a specific date. ---

// 3. This is the specific "as of" date for this calculation.
local as_of_date = mdy(5,29,2024)

// 4. The script flags which spell was active for each worker on this date.
gen byte is_active_on_date = (doj <= `as_of_date' & (dol >= `as_of_date' | missing(dol)))

// 5. From that active spell, it pulls the start date.
gen current_spell_doj = doj if is_active_on_date == 1
format current_spell_doj %td

// 6. This single start date is then copied to all rows for that worker. This is key for the next step.
bysort uid: egen final_current_doj = max(current_spell_doj)
format final_current_doj %td

// 7. Finally, it calculates the tenure from that spell's start date up to the "as of" date.
gen current_tenure_days = (`as_of_date' - final_current_doj) + 1
gen current_tenure_years = current_tenure_days / 365.25

// --- 8. Clean up all the helper variables created in this section. ---
drop temp_end_date spell_end_date_capped capped_spell_tenure is_active_on_date current_spell_doj
rename final_current_doj latest_effective_doj

// Save this processed spell-level data for the next stage.
tempfile spells_with_meta
save `spells_with_meta'

/*===============================================================================
 PART 4: RESHAPE DATA FROM LONG TO WIDE
===============================================================================*/
// The data is currently "long," with one row for each employment spell.
// This section transforms it to be "wide," with one row per person, and columns
// for each of their spell dates (e.g., doj_first, dol_first, doj_second, etc.).

use `spells_with_meta', clear

// First, it tags the rows that correspond to the employee's ID at the start of the study.
gen byte study_tag = (tokenno == study_tokenno)

*--------------------------------------------------------------
* Spell indexing and status classification
*--------------------------------------------------------------

// Ensures spells are ordered chronologically by their start date.
bysort uid (doj): gen rep = _n
bysort uid:       gen repeat = _N

label var rep    "Spell number for this worker (chronological)"
label var repeat "Total spells for this worker"

// This `leftstatus` variable classifies the final employment status of a worker.
gen leftstatus = .
replace leftstatus = 0 if repeat == 1 & missing(dol)  // Has one spell and is still working.
replace leftstatus = 1 if repeat == 1 & !missing(dol) // Has one spell and has left.
replace leftstatus = 2 if repeat > 1 & rep == repeat & missing(dol) // Rejoined and is currently working.
replace leftstatus = 3 if repeat > 1 & rep == repeat & !missing(dol) // Rejoined and has since left again.

label define k 0 "working" 1 "left" 2 "left & rejoined" 3 "rejoined & quit"
label values leftstatus k

// A simple turnover indicator is created for the baseline employment spell.
gen turnover = .
replace turnover = 0 if study_tag==1 & inlist(leftstatus,0,2)
replace turnover = 1 if study_tag==1 & inlist(leftstatus,1,3)
label define turn_lbl 0 "Not left / working" 1 "Left"
label values turnover turn_lbl

*--------------------------------------------------------------
* Create wide variables for DOJ/DOL for each spell
*--------------------------------------------------------------

format doj %td
format dol %td

// Defines names for up to 15 employment spells.
local spellnames first second third fourth fifth sixth seventh eighth ninth tenth eleventh twelfth thirteenth fourteenth fifteenth

// This loop creates new variables for the start and end date of each of the 15 possible spells.
forvalues s = 1/15 {
    local spellname : word `s' of `spellnames'
    gen doj_`spellname' = .
    gen dol_`spellname' = .
    replace doj_`spellname' = doj if rep == `s'
    replace dol_`spellname' = dol if rep == `s'
    format doj_`spellname' %td
    format dol_`spellname' %td
}


// These spell dates are renamed to be more descriptive (e.g., "doj_rejoin").
capture rename doj_second      doj_rejoin
capture rename doj_third       doj_rejoin_twice
capture rename doj_fourth      doj_rejoin_thrice
capture rename doj_fifth       doj_rejoin_fourth
capture rename doj_sixth       doj_rejoin_fifth
capture rename doj_seventh     doj_rejoin_sixth
capture rename doj_eighth      doj_rejoin_seventh
capture rename doj_ninth       doj_rejoin_eighth
capture rename doj_tenth       doj_rejoin_ninth
capture rename doj_eleventh    doj_rejoin_tenth
capture rename doj_twelfth     doj_rejoin_eleventh
capture rename doj_thirteenth  doj_rejoin_twelfth
capture rename doj_fourteenth  doj_rejoin_thirteenth
capture rename doj_fifteenth   doj_rejoin_fourteenth


// After reshaping, only one row per person has the date. This step propagates
// that single date to all rows for that person, filling in the missing values.
local spell_vars doj_first dol_first dol_second dol_third dol_fourth dol_fifth dol_sixth ///
                 dol_seventh dol_eighth dol_ninth dol_tenth dol_eleventh dol_twelfth dol_thirteenth ///
                 dol_fourteenth dol_fifteenth ///
                 doj_rejoin doj_rejoin_twice doj_rejoin_thrice doj_rejoin_fourth doj_rejoin_fifth ///
                 doj_rejoin_sixth doj_rejoin_seventh doj_rejoin_eighth doj_rejoin_ninth doj_rejoin_tenth ///
                 doj_rejoin_eleventh doj_rejoin_twelfth doj_rejoin_thirteenth doj_rejoin_fourteenth
                
foreach v of local spell_vars {
    bysort uid: egen temp_var = max(`v')
    replace `v' = temp_var
    drop temp_var
    format `v' %td
}

// The wide-format data is saved to a temporary file.
tempfile spells_wide
save `spells_wide'


/*===============================================================================
 PART 5: CREATE THE PERSON-LEVEL ATTRITION FILE
===============================================================================*/
// This section takes the wide data and collapses it to one unique row per person,
// containing all their employment history in separate columns.

use `spells_wide', clear

// It keeps only the row corresponding to the worker's baseline employment spell.
keep if study_tag == 1

// It keeps only the necessary variables for the final person-level attrition file.
keep study_empcode uid current_tenure_days current_tenure_years total_tenure_days total_tenure_years leftstatus turnover ///
     doj_first dol_first dol_second dol_third dol_fourth dol_fifth dol_sixth ///
     dol_seventh dol_eighth dol_ninth dol_tenth dol_eleventh dol_twelfth dol_thirteenth ///
     dol_fourteenth dol_fifteenth ///
     doj_rejoin doj_rejoin_twice doj_rejoin_thrice doj_rejoin_fourth doj_rejoin_fifth ///
     doj_rejoin_sixth doj_rejoin_seventh doj_rejoin_eighth doj_rejoin_ninth doj_rejoin_tenth ///
     doj_rejoin_eleventh doj_rejoin_twelfth doj_rejoin_thirteenth doj_rejoin_fourteenth

// All date variables are correctly formatted.
format doj_first dol_first dol_second dol_third dol_fourth dol_fifth dol_sixth ///
       dol_seventh dol_eighth dol_ninth dol_tenth dol_eleventh dol_twelfth dol_thirteenth ///
       dol_fourteenth dol_fifteenth ///
       doj_rejoin doj_rejoin_twice doj_rejoin_thrice doj_rejoin_fourth doj_rejoin_fifth ///
       doj_rejoin_sixth doj_rejoin_seventh doj_rejoin_eighth doj_rejoin_ninth doj_rejoin_tenth ///
       doj_rejoin_eleventh doj_rejoin_twelfth doj_rejoin_thirteenth doj_rejoin_fourteenth %td

// Key variables are labeled for clarity.
label var uid "study id"
label var dol_first "First Date of leaving"
label var turnover "Turnover status"
label var leftstatus "Working/Left/rejoin status"
label var doj_first "First date of joining"

// This finds the latest leaving date across all spells for each person.
egen max_dol = rowmax(dol_*)
// It then keeps only the very last record, ensuring one unique row per person.
bysort uid (max_dol): keep if _n == _N
drop max_dol

// The final person-level attrition file is saved.
save "${attrition_cleaned}", replace


/*==============================================================================
  PART 6: LOADING AND STANDARDIZING MONTHLY ATTENDANCE DATA
------------------------------------------------------------------------------*/
// The goal of this section is to loop through each raw monthly attendance file,
// merge it with a stable employee identifier (uid), and save it as a
// standardized temporary file.

// First, load the file that maps various employee codes (`empcode`) to a single,
// unique person identifier (`uid`).
use "${uid_map}", clear
destring uid, force replace
tempfile uid_data
save `uid_data'

// This next part processes each monthly attendance file from Dec 2023 to Jun 2025.
// For each month, the script loads the data, attaches the stable UID, keeps the
// latest record, and saves it to a uniquely named tempfile.
use "${raw_attendance_dir}/eas_2023_12.dta", clear
merge 1:1 empcode using `uid_data'
keep if _m==3
drop _m
drop if missing(doj)
sort uid doj
bysort uid: keep if _n == _N
tempfile d_2023_12
save `d_2023_12'

forval i=1/9{
    use "${raw_attendance_dir}/eas_2024_0`i'.dta", clear
    merge 1:1 empcode using `uid_data'
    keep if _m==3
    drop _m
    drop if missing(doj)
    sort uid doj
    bysort uid: keep if _n == _N
    tempfile d_2024_0`i'
    save `d_2024_0`i''
}

foreach i in 10 11 12{
    use "${raw_attendance_dir}/eas_2024_`i'.dta", clear
    merge 1:1 empcode using `uid_data'
    keep if _m==3
    drop _m
    drop if missing(doj)
    sort uid doj
    bysort uid: keep if _n == _N
    tempfile d_2024_`i'
    save `d_2024_`i''
}

forval i=1/9{
    use "${raw_attendance_dir}/eas_2025_0`i'.dta", clear
    merge 1:1 empcode using `uid_data'
    keep if _m==3
    drop _m
    drop if missing(doj)
    sort uid doj
    bysort uid: keep if _n == _N
    tempfile d_2025_0`i'
    save `d_2025_0`i''
}

/*==============================================================================
  PART 7: COMBINING DATA INTO A WIDE FORMAT
------------------------------------------------------------------------------*/
// This section starts with the cleaned attrition file and merges each of the
// monthly tempfiles. This creates a "wide" dataset with one row per person.

use "${attrition_cleaned}", clear
keep uid study_empcode  dol* doj* leftstatus

// Sequentially merge each cleaned monthly attendance tempfile.
merge 1:m uid using `d_2023_12'
keep if _m != 2
rename _m dec23_merge
rename att_status* status_23_dec_*

merge 1:m uid using `d_2024_01'
keep if _m != 2
rename _m jan24_merge
rename att_status* status_24_jan_*

merge 1:m uid using `d_2024_02'
keep if _m != 2
rename _m feb24_merge
rename att_status* status_24_feb_*

merge 1:m uid using `d_2024_03'
keep if _m != 2
rename _m mar24_merge
rename factory factory_mar24
rename att_status* status_24_mar_*

merge 1:m uid using `d_2024_04'
keep if _m != 2
rename _m apr24_merge
rename factory factory_apr24
rename att_status* status_24_apr_*

merge 1:m uid using `d_2024_05'
keep if _m != 2
rename _m may24_merge
rename factory factory_may24
rename att_status* status_24_may_*

merge 1:m uid using `d_2024_06'
keep if _m != 2
rename _m jun24_merge
rename factory factory_jun24
rename att_status* status_24_jun_*

merge 1:m uid using `d_2024_07'
keep if _m != 2
rename _m jul24_merge
rename factory factory_jul24
rename att_status* status_24_jul_*

merge 1:m uid using `d_2024_08'
keep if _m != 2
rename _m aug24_merge
rename factory factory_aug24
rename att_status* status_24_aug_*

merge 1:m uid using `d_2024_09'
keep if _m != 2
rename _m sep24_merge
rename factory factory_sep24
rename att_status* status_24_sep_*

merge 1:m uid using `d_2024_10'
keep if _m != 2
rename _m oct24_merge
rename factory factory_oct24
rename att_status* status_24_oct_*

merge 1:m uid using `d_2024_11'
keep if _m != 2
rename _m nov24_merge
rename factory factory_nov24
rename att_status* status_24_nov_*

merge 1:m uid using `d_2024_12'
keep if _m != 2
rename _m dec24_merge
rename factory factory_dec24
rename att_status* status_24_dec_*

merge 1:m uid using `d_2025_01'
keep if _m != 2
rename _m jan25_merge
rename att_status* status_25_jan_*

merge 1:m uid using `d_2025_02'
keep if _m != 2
rename _m feb25_merge
rename att_status* status_25_feb_*

merge 1:m uid using `d_2025_03'
keep if _m != 2
rename _m mar25_merge
rename factory factory_mar25
rename att_status* status_25_mar_*

merge 1:m uid using `d_2025_04'
keep if _m != 2
rename _m apr25_merge
rename factory factory_apr25
rename att_status* status_25_apr_*

merge 1:m uid using `d_2025_05'
keep if _m != 2
rename _m may25_merge
rename factory factory_may25
rename att_status* status_25_may_*

merge 1:m uid using `d_2025_06'
keep if _m != 2
rename _m jun25_merge
rename factory factory_jun25
rename att_status* status_25_jun_*

merge 1:m uid using `d_2025_07'
keep if _m != 2
rename _m jul25_merge
rename factory factory_jul25
rename att_status* status_25_jul_*

merge 1:m uid using `d_2025_08'
keep if _m != 2
rename _m aug25_merge
rename factory factory_aug25
rename att_status* status_25_aug_*

merge 1:m uid using `d_2025_09'
keep if _m != 2
rename _m sep25_merge
rename factory factory_sep25
rename att_status* status_25_sep_*



/*==============================================================================
  PART 8: RESHAPING DATA FROM WIDE TO LONG
------------------------------------------------------------------------------*/
// This section transforms the data into a "long" format, with one
// row for each person for each day.

keep uid doj* dol* leftstatus status* *merge

reshape long status_, i(uid) j(date,str)
rename status_ status
order *_merge, last
order uid

/*==============================================================================
  PART 9: CLEANING DATES AND STATUS CODES
------------------------------------------------------------------------------*/
// This section parses the string `date` variable created by reshape to create a
// proper Stata numeric date variable.

gen month_x = substr(date, 1, 6)
gen month=substr(date,4,3)
gen day=substr(date,8,.)
tab day
gen year=2025
replace year=2024 if month_x== "24_jan" | month_x== "24_feb" | month_x== "24_mar" | month_x== "24_apr" | month_x== "24_may" | month_x== "24_jun" |month_x== "24_jul" |month_x== "24_aug" |month_x== "24_sep" | month_x== "24_oct" | month_x== "24_nov" | month_x== "24_dec"
replace year=2023 if month_x== "23_dec"

gen month_num=9 if month=="sep"
replace month_num=10 if month=="oct"
replace month_num=11 if month=="nov"
replace month_num=12 if month=="dec"
replace month_num=1 if month=="jan"
replace month_num=2 if month=="feb"
replace month_num=3 if month=="mar"
replace month_num=4 if month=="apr"
replace month_num=5 if month=="may"
replace month_num=6 if month=="jun"
replace month_num=7 if month=="jul"
replace month_num=8 if month=="aug"

destring(day), replace
gen edate = mdy(month_num, day, year)
format %td edate
drop date month month_x day
rename edate date
tab date, sort
order *merge , last

// This intermediate reshaped data is saved to a tempfile.
tempfile attendance_midlife
save `attendance_midlife', replace


use `attendance_midlife', clear


// The raw attendance statuses are encoded into a numeric variable with labels.
label define leave 1 "P" 2 "A" 3 "EL" 4 "CL" 5 "SL" 6 "ES" 7 "PCL" 8 "ML" 9 "WP" 10 "W" 11 "IS" 12 "left_shahi" 13 "H" 14 "Missing data" ///
15 "A/P" 16 "A/EL" 17 "P/A" 18 "P/EL" 19 "EL/P" 20 "EL/A"
replace status =substr(status,1,4)
encode status, gen(status_num) label(leave)
drop status
rename status_num status

sort uid date
order uid doj* dol* date status

// A simple binary attendance variable is created.
gen attendance = (status == 1)
replace attendance = . if status == 12
replace attendance = . if status == 10
replace attendance = . if status == 13

/*==============================================================================
  PART 10: CORRECTING DAILY STATUS USING EMPLOYMENT SPELL DATA (EXTENDED FOR 15 SPELLS)
------------------------------------------------------------------------------*/
// This critical step uses the reliable DOJ/DOL information to correct the daily
// status, ensuring consistency between attendance records and official employment spells.

// --- Corrects status for periods BETWEEN employment spells ---
// This section identifies the "gaps" when a worker has left but will later rejoin.

replace status = 12 if date > dol_first & date < doj_rejoin & !missing(dol_first) & !missing(doj_rejoin)
replace attendance = . if date > dol_first & date < doj_rejoin & !missing(dol_first) & !missing(doj_rejoin)

replace status = 12 if date > dol_second & date < doj_rejoin_twice & !missing(dol_second) & !missing(doj_rejoin_twice)
replace attendance = . if date > dol_second & date < doj_rejoin_twice & !missing(dol_second) & !missing(doj_rejoin_twice)

replace status = 12 if date > dol_third & date < doj_rejoin_thrice & !missing(dol_third) & !missing(doj_rejoin_thrice)
replace attendance = . if date > dol_third & date < doj_rejoin_thrice & !missing(dol_third) & !missing(doj_rejoin_thrice)

replace status = 12 if date > dol_fourth & date < doj_rejoin_fourth & !missing(dol_fourth) & !missing(doj_rejoin_fourth)
replace attendance = . if date > dol_fourth & date < doj_rejoin_fourth & !missing(dol_fourth) & !missing(doj_rejoin_fourth)

replace status = 12 if date > dol_fifth & date < doj_rejoin_fifth & !missing(dol_fifth) & !missing(doj_rejoin_fifth)
replace attendance = . if date > dol_fifth & date < doj_rejoin_fifth & !missing(dol_fifth) & !missing(doj_rejoin_fifth)

replace status = 12 if date > dol_sixth & date < doj_rejoin_sixth & !missing(dol_sixth) & !missing(doj_rejoin_sixth)
replace attendance = . if date > dol_sixth & date < doj_rejoin_sixth & !missing(dol_sixth) & !missing(doj_rejoin_sixth)

replace status = 12 if date > dol_seventh & date < doj_rejoin_seventh & !missing(dol_seventh) & !missing(doj_rejoin_seventh)
replace attendance = . if date > dol_seventh & date < doj_rejoin_seventh & !missing(dol_seventh) & !missing(doj_rejoin_seventh)

replace status = 12 if date > dol_eighth & date < doj_rejoin_eighth & !missing(dol_eighth) & !missing(doj_rejoin_eighth)
replace attendance = . if date > dol_eighth & date < doj_rejoin_eighth & !missing(dol_eighth) & !missing(doj_rejoin_eighth)

replace status = 12 if date > dol_ninth & date < doj_rejoin_ninth & !missing(dol_ninth) & !missing(doj_rejoin_ninth)
replace attendance = . if date > dol_ninth & date < doj_rejoin_ninth & !missing(dol_ninth) & !missing(doj_rejoin_ninth)

replace status = 12 if date > dol_tenth & date < doj_rejoin_tenth & !missing(dol_tenth) & !missing(doj_rejoin_tenth)
replace attendance = . if date > dol_tenth & date < doj_rejoin_tenth & !missing(dol_tenth) & !missing(doj_rejoin_tenth)

replace status = 12 if date > dol_eleventh & date < doj_rejoin_eleventh & !missing(dol_eleventh) & !missing(doj_rejoin_eleventh)
replace attendance = . if date > dol_eleventh & date < doj_rejoin_eleventh & !missing(dol_eleventh) & !missing(doj_rejoin_eleventh)

replace status = 12 if date > dol_twelfth & date < doj_rejoin_twelfth & !missing(dol_twelfth) & !missing(doj_rejoin_twelfth)
replace attendance = . if date > dol_twelfth & date < doj_rejoin_twelfth & !missing(dol_twelfth) & !missing(doj_rejoin_twelfth)

replace status = 12 if date > dol_thirteenth & date < doj_rejoin_thirteenth & !missing(dol_thirteenth) & !missing(doj_rejoin_thirteenth)
replace attendance = . if date > dol_thirteenth & date < doj_rejoin_thirteenth & !missing(dol_thirteenth) & !missing(doj_rejoin_thirteenth)

replace status = 12 if date > dol_fourteenth & date < doj_rejoin_fourteenth & !missing(dol_fourteenth) & !missing(doj_rejoin_fourteenth)
replace attendance = . if date > dol_fourteenth & date < doj_rejoin_fourteenth & !missing(dol_fourteenth) & !missing(doj_rejoin_fourteenth)

// --- Corrects status for periods AFTER the FINAL exit ---
// This section handles cases where a worker leaves and does NOT return.

replace status = 12 if date >= dol_first & missing(doj_rejoin) & !missing(dol_first)
replace attendance = . if date >= dol_first & missing(doj_rejoin) & !missing(dol_first)

replace status = 12 if date >= dol_second & missing(doj_rejoin_twice) & !missing(dol_second)
replace attendance = . if date >= dol_second & missing(doj_rejoin_twice) & !missing(dol_second)

replace status = 12 if date >= dol_third & missing(doj_rejoin_thrice) & !missing(dol_third)
replace attendance = . if date >= dol_third & missing(doj_rejoin_thrice) & !missing(dol_third)

replace status = 12 if date >= dol_fourth & missing(doj_rejoin_fourth) & !missing(dol_fourth)
replace attendance = . if date >= dol_fourth & missing(doj_rejoin_fourth) & !missing(dol_fourth)

replace status = 12 if date >= dol_fifth & missing(doj_rejoin_fifth) & !missing(dol_fifth)
replace attendance = . if date >= dol_fifth & missing(doj_rejoin_fifth) & !missing(dol_fifth)

replace status = 12 if date >= dol_sixth & missing(doj_rejoin_sixth) & !missing(dol_sixth)
replace attendance = . if date >= dol_sixth & missing(doj_rejoin_sixth) & !missing(dol_sixth)

replace status = 12 if date >= dol_seventh & missing(doj_rejoin_seventh) & !missing(dol_seventh)
replace attendance = . if date >= dol_seventh & missing(doj_rejoin_seventh) & !missing(dol_seventh)

replace status = 12 if date >= dol_eighth & missing(doj_rejoin_eighth) & !missing(dol_eighth)
replace attendance = . if date >= dol_eighth & missing(doj_rejoin_eighth) & !missing(dol_eighth)

replace status = 12 if date >= dol_ninth & missing(doj_rejoin_ninth) & !missing(dol_ninth)
replace attendance = . if date >= dol_ninth & missing(doj_rejoin_ninth) & !missing(dol_ninth)

replace status = 12 if date >= dol_tenth & missing(doj_rejoin_tenth) & !missing(dol_tenth)
replace attendance = . if date >= dol_tenth & missing(doj_rejoin_tenth) & !missing(dol_tenth)

replace status = 12 if date >= dol_eleventh & missing(doj_rejoin_eleventh) & !missing(dol_eleventh)
replace attendance = . if date >= dol_eleventh & missing(doj_rejoin_eleventh) & !missing(dol_eleventh)

replace status = 12 if date >= dol_twelfth & missing(doj_rejoin_twelfth) & !missing(dol_twelfth)
replace attendance = . if date >= dol_twelfth & missing(doj_rejoin_twelfth) & !missing(dol_twelfth)

replace status = 12 if date >= dol_thirteenth & missing(doj_rejoin_thirteenth) & !missing(dol_thirteenth)
replace attendance = . if date >= dol_thirteenth & missing(doj_rejoin_thirteenth) & !missing(dol_thirteenth)

replace status = 12 if date >= dol_fourteenth & missing(doj_rejoin_fourteenth) & !missing(dol_fourteenth)
replace attendance = . if date >= dol_fourteenth & missing(doj_rejoin_fourteenth) & !missing(dol_fourteenth)

// After the 15th (and final possible) spell, any exit is permanent.
replace status = 12 if date >= dol_fifteenth & !missing(dol_fifteenth)
replace attendance = . if date >= dol_fifteenth & !missing(dol_fifteenth)


/*==============================================================================
  PART 11: NECESSARY VARIABLE CREATION AND DATA ASSEMBLY
------------------------------------------------------------------------------*/
// Creates a sequential month index for time-series analysis.
gen mdate = ym(year, month_num)
gen sequential_month = mdate - ym(2023, 11)
label define sequential_month_lbl 1 "Dec2023" 2 "Jan2024" 3 "Feb2024" 4 "Mar2024" ///
                                5 "Apr2024" 6 "May2024" 7 "Jun2024" 8 "Jul2024" ///
                                9 "Aug2024" 10 "Sep2024" 11 "Oct2024" 12 "Nov2024" ///
                                13 "Dec2024" 14 "Jan2025" 15 "Feb2025" 16 "Mar2025" ///
                                17 "Apr2025" 18 "May2025" 19 "Jun2025" 20 "Jul2025"
label values sequential_month sequential_month_lbl
rename sequential_month month

// Cleans up the dataset.
order leftstatus month year, after(status)
order uid  doj dol_first doj_rejoin dol_second doj_rejoin_twice date status
drop *_merge leftstatus
sort uid date

// Labels key variables.
label var date "Date"
label var doj "(irrelevant)First joining date"
label var dol_first "First DOL"
label var doj_rejoin "First REJOIN date"
label var dol_second "Second and last DOL"
label var doj_rejoin_twice "Second REJOIN date"
label var status "Attendance Status "

// Saves the first main output: the cleaned daily attendance panel.
save "${out_cleaned_attendance}", replace


// --- Assembling the final analysis dataset ---
// Starts with the baseline and treatment status data.
use "${treatstat_data}", clear
merge 1:1 uid using "${attrition_cleaned}", force
keep if _merge == 3
drop _merge
merge 1:m uid using "${out_cleaned_attendance}"
keep if _merge == 3
drop _merge

* Labelling control variables in Baseline data
lab def remittance 0 "Doesn't Remit" 1 "Remits (BL)"
lab val remit remittance
lab var remit "Remittance at BL"
lab def educ_g10 0 "Educ < G10" 1 "Educ = G10" 2 "Educ > G10"
lab val educ_g10 educ_g10 
lab var educ_g10 "Education (less, more, equal to G10)"
lab def hh_head_resp 0 "Not the HH Head" 1 "Sole/co HH Head"
lab val hh_head_resp hh_head_resp 
lab def sphone 0 "No Smartphone" 1 "Owns and uses a smartphone (BL)"
lab val own_phone sphone
lab var own_phone "Owns and uses a smartphone (BL)"
lab def knowsupi 0 "Doesn't Know UPI" 1 "Knows UPI use"
lab val knows_upi knowsupi
rename anysavings any_blsavings
lab def anysave 0 "No Savings" 1 "Any Savings at BL"
lab val any_blsavings anysave
lab var any_blsavings "Any Savings at Baseline"
rename bank_savings any_bnksavings 
lab def anybank 0 "No Bank Savings" 1 "Any Bank Savings at BL"
lab val any_bnksavings anybank
lab var any_bnksavings "Any Bank Savings at Baseline"
lab var age "Age"
lab var asset_index "Asset Index (BL)"
rename any_loans any_blloans
lab def any_blloans 0 "No Loans" 1 "Any Loans at BL"
lab val any_blloans any_blloans
lab var any_blloans "Any Loans at Baseline"
lab var log_total_savings "Log of Total Savings (BL)"
lab var log_total_loans "Log of Total Loans (BL)"
lab var tenure_months "BL Tenure (Months)"

* Labelling treatment variables in baseline data
rename treat treatment1 
lab def treatment1 0 "Pure Control" 1 "Treatment 1" 2 "Treatment 2" 3 "Treatment 3" 4 "Treatment 4"
lab val treatment1 treatment1
lab var treatment1 "Treatment"

rename ntreat treatment2 
lab def pcontrol 0 "Pure Control" 1 "Treatment Arms"
lab val treatment2 pcontrol 
lab var treatment2 "Treatment (Control vs Treatment)"
recode treatment1 (0 = .), gen(treatment3)
lab def treat4g 1 "Treatment 1" 2 "Treatment 2" 3 "Treatment 3" 4 "Treatment 4"
lab val treatment3 treat4g
lab var treatment3 "Treatment (4G)"
recode treatment1 (0 1 = 0) (2 3 4 = 1), gen(treatment4)
lab def autoded 0 "T-01" 1 "Auto-Deductions (T-234)"
lab val treatment4 autoded 
lab var treatment4 "Treatment (along automatic deductions)"
recode treatment1 (0 1 2 = 0) (3 4 = 1), gen(treatment5)
lab def rewarms 0 "No Rewards (T-012)" 1 "Rewards (T-34)"
lab val treatment5 rewarms
lab var treatment5 "Treatment (along rewards)"

// Manually corrects a known data error.
replace tenure_months = 54/30 if uid == 8627574798
xtset uid date

// Drops old enrollment date before merging the clean version.
drop enrol_date
tempfile attendance_with_bl
save `attendance_with_bl'

// Merges implementation data (containing enrollment dates).
use "${implementation_data}", clear
tempfile implementation
save `implementation'
use `attendance_with_bl', clear
merge m:1 uid using `implementation', force
tab _merge
destring new_tknno, replace
recast long enrol_date
drop _merge
tempfile attendance_with_impl
save `attendance_with_impl'

// Merges control group cutoff date information.
import delimited "${control_csv}", clear
ren pctoken new_tknno
generate temp_date = date(pc_date, "DMY")
format temp_date %td
drop pc_date
rename temp_date enrol_date2
recast long enrol_date2
keep new_tknno enrol_date2
tempfile control_cutoff
save `control_cutoff'

use `attendance_with_impl', clear
merge m:1 new_tknno using `control_cutoff', force
drop if _merge == 2
drop _merge

// Creates various outcome variables.
gen abs = status == 2
gen abs_attr = status == 2| status == 12
gen attr1 = status == 12
label var abs "Absent"
label var abs_attr "Absent or left the factory"
label var attr1 "Left the factory"

tostring new_tknno, replace force

// Saves the final, fully assembled analysis dataset.
save "${out_final_prepost}", replace


/*===============================================================================
  PART 12: MERGING DATA SOURCES TO CREATE THE DAILY PANEL
===============================================================================*/
// This part of the script brings together all the different datasets (treatment
// status, attendance, implementation details, etc.) to build the final daily
// panel dataset for analysis.

// STEP 0: Prepare implementation data with a numeric uid.
use "${implementation_data}", clear
save "${implementation_data}", replace

// STEP 1: Load treatment status data and merge the attrition file.
use "${treatstat_data}", clear
rename doj treatstat_doj // Renames doj to avoid conflicts.
merge 1:1 uid using "${attrition_cleaned}", keep(match using)
drop if _merge == 2
drop _merge

// STEP 2: Attach the latest employee ID (`new_tknno`) from attendance data.
// This is necessary for linking with other datasets.
capture confirm variable new_tknno
if _rc {
    preserve
        use "${out_final_prepost}", clear
        keep uid new_tknno
        destring uid, force replace
        drop if missing(uid) | missing(new_tknno)
        bysort uid (new_tknno): keep if _n==1
        tempfile tkn_map
        save `tkn_map'
    restore
    merge 1:1 uid using `tkn_map', nogen
}

// STEP 3: Merge the implementation data.
merge 1:1 uid using "${implementation_data}", force
drop if _merge==2
drop _merge

// STEP 4: Merge with the control group data from the CSV file.
tempfile main
save `main' // Saves the current state of the main dataset.


// Prepare the attendance data
use "${out_final_prepost}", clear
tostring new_tknno, replace force
tempfile att
save `att'

use `main', clear // Load the main dataset
drop date
// Now, merge the attendance data into the already-expanded dataset
merge 1:m uid using `att'
keep if _merge == 3 // Keeps only records that successfully merged
drop _merge         // Clean up after the final merge


/*===============================================================================
  PART 13: CORRECTING DAILY STATUS BASED ON EMPLOYMENT SPELLS
===============================================================================*/
// The raw attendance data can sometimes be inconsistent with a worker's actual
// employment spell. This section uses the reliable DOJ/DOL information to clean
// up the daily `status' and `attendance' variables.

// First, create a numeric indicator for recorded presence.
capture confirm variable recorded_present
if _rc {
    gen byte recorded_present = (attendance==1)
}

// 1) Handle "Rejoin Gaps": For the days between leaving and rejoining,
//    the status is set to "Left" (12) and attendance is marked as missing.
gen byte rj1 = (!missing(dol_first) & date > dol_first & (!missing(doj_rejoin) & date < doj_rejoin))
replace status = 12 if rj1
replace attendance = . if rj1
gen byte rj2 = (!missing(dol_second) & date > dol_second & (!missing(doj_rejoin_twice) & date < doj_rejoin_twice))
replace status = 12 if rj2
replace attendance = . if rj2
label var rj1 "Between first DOL and first rejoin"
label var rj2 "Between second DOL and second rejoin"

// 2) Identify Active Spells: Flags are created for each day that falls within a valid employment spell.
gen byte active_spell1 = !missing(doj_first)  & date >= doj_first  & (missing(dol_first)  | date <= dol_first)
gen byte active_spell2 = !missing(doj_rejoin) & date >= doj_rejoin & (missing(dol_second) | date <= dol_second)
gen byte active_spell3 = !missing(doj_rejoin_twice) & date >= doj_rejoin_twice & (missing(dol_third) | date <= dol_third)

// This logic ensures that attendance is only marked as "Present" (1) if there is a
// record of their presence *and* the day falls within one of their active employment spells.
replace attendance = 1 if active_spell1 & recorded_present==1
replace status     = 1 if active_spell1 & attendance==1
replace attendance = 1 if active_spell2 & recorded_present==1
replace status     = 1 if active_spell2 & attendance==1
replace attendance = 1 if active_spell3 & recorded_present==1
replace status     = 1 if active_spell3 & attendance==1

// Clean up the helper variables.
drop active_spell* rj1 rj2

/*===============================================================================
 PART 14: CONSTRUCT THE DAILY TURNOVER VARIABLE
===============================================================================*/
// This creates a `turnover_daily` variable, which is `0` if
// the person is employed on a given day and `1` if they are not. This is built
// by checking if the date falls between any of the DOJ/DOL pairs.

// Make sure the daily date variable is formatted correctly.
capture confirm variable date
if _rc {
    di as err "daily date variable 'date' not found. Rename accordingly."
    exit 199
}
format date %td

gen turnover_daily = .

// The logic is applied sequentially for each of the 15 possible spells.
// Spell 1: Employed (0) during the spell, Left (1) after.
replace turnover_daily = 0 if !missing(doj_first) & date >= doj_first & (date < dol_first | missing(dol_first))
replace turnover_daily = 1 if !missing(dol_first) & date >= dol_first & (date < doj_rejoin | missing(doj_rejoin))

// Spell 2
replace turnover_daily = 0 if !missing(doj_rejoin) & date >= doj_rejoin & (date < dol_second | missing(dol_second))
replace turnover_daily = 1 if !missing(dol_second) & date >= dol_second & (date < doj_rejoin_twice | missing(doj_rejoin_twice))

// Spell 3
replace turnover_daily = 0 if !missing(doj_rejoin_twice) & date >= doj_rejoin_twice & (date < dol_third | missing(dol_third))
replace turnover_daily = 1 if !missing(dol_third) & date >= dol_third & (date < doj_rejoin_thrice | missing(doj_rejoin_thrice))

// Spell 4
replace turnover_daily = 0 if !missing(doj_rejoin_thrice) & date >= doj_rejoin_thrice & (date < dol_fourth | missing(dol_fourth))
replace turnover_daily = 1 if !missing(dol_fourth) & date >= dol_fourth & (date < doj_rejoin_fourth | missing(doj_rejoin_fourth))

// Spell 5
replace turnover_daily = 0 if !missing(doj_rejoin_fourth) & date >= doj_rejoin_fourth & (date < dol_fifth | missing(dol_fifth))
replace turnover_daily = 1 if !missing(dol_fifth) & date >= dol_fifth & (date < doj_rejoin_fifth | missing(doj_rejoin_fifth))

// Spell 6
replace turnover_daily = 0 if !missing(doj_rejoin_fifth) & date >= doj_rejoin_fifth & (date < dol_sixth | missing(dol_sixth))
replace turnover_daily = 1 if !missing(dol_sixth) & date >= dol_sixth & (date < doj_rejoin_sixth | missing(doj_rejoin_sixth))

// Spell 7
replace turnover_daily = 0 if !missing(doj_rejoin_sixth) & date >= doj_rejoin_sixth & (date < dol_seventh | missing(dol_seventh))
replace turnover_daily = 1 if !missing(dol_seventh) & date >= dol_seventh & (date < doj_rejoin_seventh | missing(doj_rejoin_seventh))

// Spell 8
replace turnover_daily = 0 if !missing(doj_rejoin_seventh) & date >= doj_rejoin_seventh & (date < dol_eighth | missing(dol_eighth))
replace turnover_daily = 1 if !missing(dol_eighth) & date >= dol_eighth & (date < doj_rejoin_eighth | missing(doj_rejoin_eighth))

// Spell 9
replace turnover_daily = 0 if !missing(doj_rejoin_eighth) & date >= doj_rejoin_eighth & (date < dol_ninth | missing(dol_ninth))
replace turnover_daily = 1 if !missing(dol_ninth) & date >= dol_ninth & (date < doj_rejoin_ninth | missing(doj_rejoin_ninth))

// Spell 10
replace turnover_daily = 0 if !missing(doj_rejoin_ninth) & date >= doj_rejoin_ninth & (date < dol_tenth | missing(dol_tenth))
replace turnover_daily = 1 if !missing(dol_tenth) & date >= dol_tenth & (date < doj_rejoin_tenth | missing(doj_rejoin_tenth))

// Spell 11
replace turnover_daily = 0 if !missing(doj_rejoin_tenth) & date >= doj_rejoin_tenth & (date < dol_eleventh | missing(dol_eleventh))
replace turnover_daily = 1 if !missing(dol_eleventh) & date >= dol_eleventh & (date < doj_rejoin_eleventh | missing(doj_rejoin_eleventh))

// Spell 12
replace turnover_daily = 0 if !missing(doj_rejoin_eleventh) & date >= doj_rejoin_eleventh & (date < dol_twelfth | missing(dol_twelfth))
replace turnover_daily = 1 if !missing(dol_twelfth) & date >= dol_twelfth & (date < doj_rejoin_twelfth | missing(doj_rejoin_twelfth))

// Spell 13
replace turnover_daily = 0 if !missing(doj_rejoin_twelfth) & date >= doj_rejoin_twelfth & (date < dol_thirteenth | missing(dol_thirteenth))
replace turnover_daily = 1 if !missing(dol_thirteenth) & date >= dol_thirteenth & (date < doj_rejoin_thirteenth | missing(doj_rejoin_thirteenth))

// Spell 14
replace turnover_daily = 0 if !missing(doj_rejoin_thirteenth) & date >= doj_rejoin_thirteenth & (date < dol_fourteenth | missing(dol_fourteenth))
replace turnover_daily = 1 if !missing(dol_fourteenth) & date >= dol_fourteenth & (date < doj_rejoin_fourteenth | missing(doj_rejoin_fourteenth))

// Spell 15
replace turnover_daily = 0 if !missing(doj_rejoin_fourteenth) & date >= doj_rejoin_fourteenth & (date < dol_fifteenth | missing(dol_fifteenth))
replace turnover_daily = 1 if !missing(dol_fifteenth) & date >= dol_fifteenth

replace turnover_daily = . if missing(turnover_daily)
label var turnover_daily "Turnover (Original DOL)"


/*===============================================================================
 PART 15: FINAL DATA CLEANING AND RECONSTRUCTION
===============================================================================*/
// This final section performs several data cleaning and consistency checks
// to ensure the final daily panel is accurate and ready for analysis.

tempfile main_daily
save `main_daily'

// This block prepares the data for a robust reconstruction of employment status.
// It creates a temporary "long" dataset with one row per spell.
use `main_daily', clear
keep uid doj_first dol_first dol_second dol_third dol_fourth dol_fifth dol_sixth ///
         dol_seventh dol_eighth dol_ninth dol_tenth dol_eleventh dol_twelfth dol_thirteenth ///
         dol_fourteenth dol_fifteenth ///
         doj_rejoin doj_rejoin_twice doj_rejoin_thrice doj_rejoin_fourth doj_rejoin_fifth ///
         doj_rejoin_sixth doj_rejoin_seventh doj_rejoin_eighth doj_rejoin_ninth doj_rejoin_tenth ///
         doj_rejoin_eleventh doj_rejoin_twelfth doj_rejoin_thirteenth doj_rejoin_fourteenth
bys uid: keep if _n==1

// Renames variables to a consistent format (`doj_1`, `dol_1`, etc.).
rename doj_first            doj_1
rename dol_first            dol_1
rename doj_rejoin           doj_2
rename dol_second           dol_2
rename doj_rejoin_twice     doj_3
rename dol_third            dol_3
rename doj_rejoin_thrice    doj_4
rename dol_fourth           dol_4
rename doj_rejoin_fourth    doj_5
rename dol_fifth            dol_5
rename doj_rejoin_fifth     doj_6
rename dol_sixth            dol_6
rename doj_rejoin_sixth     doj_7
rename dol_seventh          dol_7
rename doj_rejoin_seventh   doj_8
rename dol_eighth           dol_8
rename doj_rejoin_eighth    doj_9
rename dol_ninth            dol_9
rename doj_rejoin_ninth     doj_10
rename dol_tenth            dol_10
rename doj_rejoin_tenth     doj_11
rename dol_eleventh         dol_11
rename doj_rejoin_eleventh  doj_12
rename dol_twelfth          dol_12
rename doj_rejoin_twelfth   doj_13
rename dol_thirteenth       dol_13
rename doj_rejoin_thirteenth doj_14
rename dol_fourteenth       dol_14
rename doj_rejoin_fourteenth doj_15
rename dol_fifteenth        dol_15

reshape long doj_ dol_, i(uid) j(spell_num)
drop if missing(doj_)
rename doj_ spell_start
rename dol_ spell_end
format spell_start %td
format spell_end   %td

// This creates an "events" file: a +1 on the day a spell starts, and a -1 on the day after it ends.
tempfile events
preserve
    keep uid spell_start
    gen date = spell_start
    gen byte event_type = 1
    keep uid date event_type
    save `events'
restore

preserve
    keep uid spell_end
    drop if missing(spell_end)
    gen date = spell_end + 1
    gen byte event_type = -1
    keep uid date event_type
    append using `events'
    save `events', replace
restore

// These events are then merged back into the main daily panel.
use `main_daily', clear
append using `events'
sort uid date event_type

// By calculating a cumulative sum of the event markers, the script can robustly determine
// if a person is employed on any given day. A sum > 0 means they are employed.
by uid: gen is_employed_sum = sum(event_type)
by uid (date): replace is_employed_sum = is_employed_sum[_n-1] if missing(event_type)

// Patches an edge case where turnover was incorrect on the exact day of leaving.
replace turnover_daily = 1 if date == dol_first
replace turnover_daily = 1 if date == dol_second
replace turnover_daily = 1 if date == dol_third
replace turnover_daily = 1 if date == dol_fourth
replace turnover_daily = 1 if date == dol_fifth
replace turnover_daily = 1 if date == dol_sixth
replace turnover_daily = 1 if date == dol_seventh
replace turnover_daily = 1 if date == dol_eighth

// The temporary event rows are dropped, leaving only the original daily data.
keep if missing(event_type)
drop event_type is_employed_sum is_employed
format date %td
drop if uid == .

// Using the reliable spell dates, a definitive `should_be_working` flag is created.
gen byte should_be_working = 0
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

// This flag is then used to correct the `status` variable.
// If someone should be working, their status cannot be 'Left'.
replace status = 1 if should_be_working == 1 & status == 12

// A series of final data patches to correct known inconsistencies in the source data.
replace turnover_daily = 1 if status == 12 & attendance == .
replace attendance = . if turnover_daily == 1
replace status = 12 if turnover_daily == 1
drop should_be_working

// This fixes an issue for workers who joined after the baseline start date,
// ensuring they are correctly marked as "not employed" before their start date.
local cutoff_date = td(01dec2023)
local condition "if doj_first > `cutoff_date' & date < doj_first"
replace status         = 12 `condition'
replace attendance     = .  `condition'
replace turnover_daily = 1  `condition'

// Tenure variables are converted from days to months for easier interpretation.
gen total_tenure_months = total_tenure_days / 30.4375
label var total_tenure_months "Total Tenure (Months)"
gen current_tenure_months = current_tenure_days / 30.4375
label var current_tenure_months "Current Tenure (Months)"

// This block contains manual data fixes for a specific participant (Sunil P) with known admin data errors.
sort uid date
order date status attendance
replace attendance = 0 in 135747
replace attendance = 1 in 135748
replace attendance = 1 in 135749
replace attendance = 1 in 135750
replace attendance = 1 in 135751
replace attendance = 1 in 135752
replace attendance = 1 in 135753
replace attendance = 1 in 135755
replace attendance = 1 in 135756
replace attendance = 0 in 135757
replace attendance = 1 in 135758
replace attendance = 1 in 135759
replace attendance = 1 in 135760
replace attendance = 1 in 135762
replace attendance = 1 in 135763
replace attendance = 1 in 135764
replace attendance = 1 in 135765
replace attendance = 1 in 135766
replace attendance = 1 in 135767
replace attendance = 1 in 135769
replace attendance = 1 in 135770
replace attendance = 1 in 135771
replace attendance = 1 in 135772
replace attendance = 1 in 135773
replace attendance = 1 in 135775
replace attendance = 1 in 135777

drop enrol_date

// Merges in the final study sample information.
tostring uid, replace format(%12.0f)
merge m:1 uid using "${study_sample}", force
keep if _merge == 3


// This block contains more manual data fixes for a specific participant (Sunil P) with known admin data errors.
replace attendance = . in 133941
replace attendance = . in 133948
replace attendance = 1 in 133949
replace attendance = . in 133934
replace attendance = . in 133927
replace attendance = . in 133920
replace attendance = . in 133955
replace attendance = . in 133962
replace attendance = . in 133969
replace attendance = . in 133976
replace attendance = 1 in 133951
replace attendance = 1 in 133952
replace attendance = 1 in 133953
replace attendance = 1 in 133954
replace attendance = 1 in 133956
replace attendance = 1 in 133957
replace attendance = 1 in 133958
replace attendance = 1 in 133959
replace attendance = 1 in 133960
replace attendance = 1 in 133961
replace attendance = 1 in 133963
replace attendance = 1 in 133964
replace attendance = 1 in 133965
replace attendance = 1 in 133966
replace attendance = 1 in 133967
replace attendance = 1 in 133968
replace attendance = 1 in 133970
replace attendance = 1 in 133971
replace attendance = 1 in 133972
replace attendance = 1 in 133973
replace attendance = 1 in 133974
replace attendance = 1 in 133975
replace attendance = 1 in 133977
replace attendance = 1 in 133978
replace attendance = 1 in 133979
replace attendance = 1 in 133980


drop if date == .

gen treatment_start_date = enrol_date
replace treatment_start_date = mdy(11, 20, 2024) if missing(treatment_start_date)
format treatment_start_date %td


gen retained = 0 if turnover_daily == 1
replace retained = 1 if turnover_daily == 0

*Creating prepost var for analysis
gen prepost = (date >= enrol_date) if !missing(enrol_date)


// Saves the final, cleaned daily panel dataset.
save "${out_daily}", replace



egen latest_doj = rowmax(doj_*)
egen latest_dol = rowmax(dol_*)

gen byte truly_left = !missing(latest_dol) & (latest_dol >= latest_doj)

format uid %12.0f

bysort uid: gen is_first_obs = (_n == 1)

preserve
keep if is_first_obs == 1 
list uid name if truly_left == 1 & latest_dol < treatment_start_date
restore


