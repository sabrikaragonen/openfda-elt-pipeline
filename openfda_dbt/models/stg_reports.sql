SELECT 
safetyreportid,
receivedate as received_date,
serious,
patient__patientsex as patient_sex,
patient__patientonsetage as patient_onset_age       
FROM {{ source('openfda_raw', 'reports') }}