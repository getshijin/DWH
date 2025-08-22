//create user & create compute wahrehouse

USE ROLE SYSADMIN;

//create DB & schemas
create database if not exists Lending_club;
create or replace schema Lending_club.land;
create or replace schema Lending_club.raw;
create or replace schema Lending_club.clean;
create or replace schema Lending_club.consumption;

show schemas in database Lending_club;

//Create file Format
create or replace file format Lending_club.land.my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'   -- allow quotes around values
ESCAPE_UNENCLOSED_FIELD = NONE
SKIP_HEADER = 1; 
    
//describe file format
desc file format Lending_club.land.my_csv_format;
ALTER FILE FORMAT Lending_club.land.my_csv_format
SET SKIP_HEADER = 1;

GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SYSADMIN;

//Stprage intergration
CREATE or replace STORAGE INTEGRATION my_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::112779684945:role/Snowflake_aws_access'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakes3bucketshijin123/csv/lendingclub/');

//create Stage for accesing the structured or unstructured or semistructured files
create or replace stage Lending_club.land.my_s3_stage
 URL = 's3://snowflakes3bucketshijin123/csv/lendingclub/'
    STORAGE_INTEGRATION = my_s3_integration
    FILE_FORMAT = Lending_club.land.my_csv_format;

//Desc Integration
DESC INTEGRATION my_s3_integration;

//Show stage
list @Lending_club.land.my_s3_stage;


Select $1,$2,$3,$4 from  @Lending_club.land.my_s3_stage;

//create table
CREATE OR REPLACE TABLE Lending_club.land.lending_club_raw (
    id STRING,
    member_id STRING,
    loan_amnt STRING,
    funded_amnt STRING,
    funded_amnt_inv STRING,
    term STRING,
    int_rate STRING,
    installment STRING,
    grade STRING,
    sub_grade STRING,
    emp_title STRING,
    emp_length STRING,
    home_ownership STRING,
    annual_inc STRING,
    verification_status STRING,
    issue_d STRING,
    loan_status STRING,
    pymnt_plan STRING,
    url STRING,
    desc STRING,
    purpose STRING,
    title STRING,
    zip_code STRING,
    addr_state STRING,
    dti STRING,
    delinq_2yrs STRING,
    earliest_cr_line STRING,
    fico_range_low STRING,
    fico_range_high STRING,
    inq_last_6mths STRING,
    mths_since_last_delinq STRING,
    mths_since_last_record STRING,
    open_acc STRING,
    pub_rec STRING,
    revol_bal STRING,
    revol_util STRING,
    total_acc STRING,
    initial_list_status STRING,
    out_prncp STRING,
    out_prncp_inv STRING,
    total_pymnt STRING,
    total_pymnt_inv STRING,
    total_rec_prncp STRING,
    total_rec_int STRING,
    total_rec_late_fee STRING,
    recoveries STRING,
    collection_recovery_fee STRING,
    last_pymnt_d STRING,
    last_pymnt_amnt STRING,
    next_pymnt_d STRING,
    last_credit_pull_d STRING,
    last_fico_range_high STRING,
    last_fico_range_low STRING,
    collections_12_mths_ex_med STRING,
    mths_since_last_major_derog STRING,
    policy_code STRING,
    application_type STRING,
    annual_inc_joint STRING,
    dti_joint STRING,
    verification_status_joint STRING,
    acc_now_delinq STRING,
    tot_coll_amt STRING,
    tot_cur_bal STRING,
    open_acc_6m STRING,
    open_act_il STRING,
    open_il_12m STRING,
    open_il_24m STRING,
    mths_since_rcnt_il STRING,
    total_bal_il STRING,
    il_util STRING,
    open_rv_12m STRING,
    open_rv_24m STRING,
    max_bal_bc STRING,
    all_util STRING,
    total_rev_hi_lim STRING,
    inq_fi STRING,
    total_cu_tl STRING,
    inq_last_12m STRING,
    acc_open_past_24mths STRING,
    avg_cur_bal STRING,
    bc_open_to_buy STRING,
    bc_util STRING,
    chargeoff_within_12_mths STRING,
    delinq_amnt STRING,
    mo_sin_old_il_acct STRING,
    mo_sin_old_rev_tl_op STRING,
    mo_sin_rcnt_rev_tl_op STRING,
    mo_sin_rcnt_tl STRING,
    mort_acc STRING,
    mths_since_recent_bc STRING,
    mths_since_recent_bc_dlq STRING,
    mths_since_recent_inq STRING,
    mths_since_recent_revol_delinq STRING,
    num_accts_ever_120_pd STRING,
    num_actv_bc_tl STRING,
    num_actv_rev_tl STRING,
    num_bc_sats STRING,
    num_bc_tl STRING,
    num_il_tl STRING,
    num_op_rev_tl STRING,
    num_rev_accts STRING,
    num_rev_tl_bal_gt_0 STRING,
    num_sats STRING,
    num_tl_120dpd_2m STRING,
    num_tl_30dpd STRING,
    num_tl_90g_dpd_24m STRING,
    num_tl_op_past_12m STRING,
    pct_tl_nvr_dlq STRING,
    percent_bc_gt_75 STRING,
    pub_rec_bankruptcies STRING,
    tax_liens STRING,
    tot_hi_cred_lim STRING,
    total_bal_ex_mort STRING,
    total_bc_limit STRING,
    total_il_high_credit_limit STRING,
    revol_bal_joint STRING,
    sec_app_fico_range_low STRING,
    sec_app_fico_range_high STRING,
    sec_app_earliest_cr_line STRING,
    sec_app_inq_last_6mths STRING,
    sec_app_mort_acc STRING,
    sec_app_open_acc STRING,
    sec_app_revol_util STRING,
    sec_app_open_act_il STRING,
    sec_app_num_rev_accts STRING,
    sec_app_chargeoff_within_12_mths STRING,
    sec_app_collections_12_mths_ex_med STRING,
    sec_app_mths_since_last_major_derog STRING,
    hardship_flag STRING,
    hardship_type STRING,
    hardship_reason STRING,
    hardship_status STRING,
    deferral_term STRING,
    hardship_amount STRING,
    hardship_start_date STRING,
    hardship_end_date STRING,
    payment_plan_start_date STRING,
    hardship_length STRING,
    hardship_dpd STRING,
    hardship_loan_status STRING,
    orig_projected_additional_accrued_interest STRING,
    hardship_payoff_balance_amount STRING,
    hardship_last_payment_amount STRING,
    disbursement_method STRING,
    debt_settlement_flag STRING,
    debt_settlement_flag_date STRING,
    settlement_status STRING,
    settlement_date STRING,
    settlement_amount STRING,
    settlement_percentage STRING,
    settlement_term STRING
);

// Use Copy command       
COPY INTO Lending_club.land.lending_club_raw
    FROM @Lending_club.land.my_s3_stage;

COPY INTO Lending_club.land.lending_club_raw
FROM @Lending_club.land.my_s3_stage
FILE_FORMAT = Lending_club.land.my_csv_format
;

select * from Lending_club.land.lending_club_raw limit 100;
group by id
order by count_1 desc;

create or replace table Lending_club.land.lending_club_raw_1 as
SELECT
    MD5( CONCAT_WS('|',
        COALESCE(application_type, ''),
        COALESCE(addr_state, ''),
        COALESCE(zip_code, ''),
        COALESCE(earliest_cr_line, ''),
        COALESCE(CAST(annual_inc AS STRING), '')
    )) AS member_id_1,
    *
FROM Lending_club.land.lending_club_raw;



CREATE OR REPLACE TABLE Lending_club.land.borrowers AS
SELECT DISTINCT
    member_id_1 AS member_id,
    application_type,
    addr_state,
    zip_code,
    earliest_cr_line,
    annual_inc
FROM lending_club.land.lending_club_raw_1;


----
CREATE OR REPLACE TABLE Lending_club.land.loans AS
SELECT
    id,
    member_id_1 AS member_id,
    loan_amnt,
    term,
    int_rate,
    installment,
    grade,
    sub_grade,
    issue_d,
    loan_status,
    purpose,
    title
FROM lending_club.land.lending_club_raw_1;


---
CREATE OR REPLACE TABLE Lending_club.land.payments AS
SELECT
    id,
    member_id_1 AS member_id,
    total_pymnt,
    total_rec_prncp,
    total_rec_int,
    total_rec_late_fee,
    recoveries,
    collection_recovery_fee,
    last_pymnt_d,
    last_pymnt_amnt
FROM lending_club.land.lending_club_raw_1;


--Edit
CREATE OR REPLACE TABLE Lending_club.land.credit_history AS
SELECT DISTINCT
    member_id_1 AS member_id,
    open_acc,
    pub_rec,
    revol_bal,
    revol_util,
    total_acc,
    delinq_2yrs,
    inq_last_6mths,
    mths_since_last_delinq,
    mths_since_last_record
FROM lending_club.land.lending_club_raw_1;

select * from Lending_club.land.borrowers;
select * from Lending_club.land.credit_history;
select * from Lending_club.land.loans;
select * from Lending_club.land.payments;

CREATE OR REPLACE table Lending_club.clean.borrowers AS
SELECT DISTINCT
    member_id:: STRING as member_id,
    application_type :: STRING as application_type,
    addr_state:: STRING as addr_state,
    zip_code:: STRING as zip_code,
    earliest_cr_line:: STRING as earliest_cr_line,
    annual_inc:: Number as annual_inc
FROM lending_club.land.borrowers;
select * from Lending_club.clean.borrowers;


