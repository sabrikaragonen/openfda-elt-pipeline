SELECT
r.safetyreportid,
d._dlt_list_idx as drug_index,
d.medicinalproduct
FROM {{source('openfda_raw', 'reports__patient__drug')}} as d 
INNER JOIN {{source('openfda_raw', 'reports')}} as r ON 
r._dlt_id=d._dlt_parent_id