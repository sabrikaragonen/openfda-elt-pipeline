SELECT 
r.safetyreportid,
rx.reactionmeddrapt,
rx._dlt_list_idx as reaction_index
FROM {{ source('openfda_raw', 'reports__patient__reaction') }} as rx
INNER JOIN  {{ source('openfda_raw', 'reports') }}  as r ON 
rx._dlt_parent_id=r._dlt_id
