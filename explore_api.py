import duckdb
import requests
def fetch_openfda_data():
    url = "https://api.fda.gov/drug/event.json"
    response = requests.get(url, params={"limit": 5})
    print(response.url)
    print(response.status_code)
    if response.status_code !=200:
        print("FDA server currently unavailable")
        raise SystemExit
    else:
        return response.json()


def flatten_openfda_reports(data):
    results=data.get("results",[])
    reports_list=[]
    reactions_list=[]
    drugs_list=[]
    for report in results:
        patient=report.get("patient", {})
        reactions=patient.get("reaction", [])
        drugs=patient.get("drug", [])
        flat_report={
            'safetyreportid': report.get("safetyreportid"),
            'receivedate': report.get("receivedate"),
            'serious': report.get("serious"),
            'patientsex': patient.get("patientsex"),
            'patientonsetage': patient.get("patientonsetage")}
        reports_list.append(flat_report) 
        for reaction in reactions:
            flat_reaction={
                'safetyreportid':report.get("safetyreportid"),
                'reactionmeddrapt':reaction.get("reactionmeddrapt")}
            reactions_list.append(flat_reaction)

        for drug in drugs:
            flat_drug={
                'safetyreportid':report.get("safetyreportid"),
                'medicinalproduct':drug.get("medicinalproduct")}
            drugs_list.append(flat_drug)
    return reports_list, reactions_list, drugs_list

            
def load_to_duckdb(reports_list, reactions_list, drugs_list):        
    conn = duckdb.connect("openfda.duckdb") 
    conn.execute("""
CREATE TABLE IF NOT EXISTS reports (
    safetyreportid VARCHAR,
    receivedate VARCHAR,
    serious VARCHAR,
    patientsex VARCHAR,
    patientonsetage VARCHAR
)
""")
    conn.execute("DELETE FROM reports")
    for report in reports_list:
        conn.execute(
        "INSERT INTO reports VALUES(?, ?, ?, ?, ?)",
        [report.get("safetyreportid"),
    report.get("receivedate"),
    report.get("serious"),
    report.get("patientsex"),
    report.get("patientonsetage"),]
    )
    conn.execute("""
CREATE TABLE IF NOT EXISTS reactions (
    safetyreportid VARCHAR,
    reactionmeddrapt VARCHAR   
)
""")
    conn.execute("DELETE FROM reactions")
    for reaction in reactions_list:
        conn.execute(
        "INSERT INTO reactions VALUES(?, ?)",
        [reaction.get("safetyreportid"),
         reaction.get("reactionmeddrapt"),
         ]
)
    conn.execute("""
CREATE TABLE IF NOT EXISTS drugs (
    safetyreportid VARCHAR,
    medicinalproduct VARCHAR
    
)
""")
    conn.execute("DELETE FROM drugs")
    for drug in drugs_list:
        conn.execute(
        "INSERT INTO drugs VALUES(?, ?)",
        [drug.get("safetyreportid"),
         drug.get("medicinalproduct"),
         ]
)         
    return conn

def validate_load(conn):
    reports_rows=conn.execute("""SELECT COUNT(*)  from reports""").fetchall()
    reactions_rows=conn.execute("""SELECT COUNT(*)  from reactions""").fetchall()
    drugs_rows=conn.execute("""SELECT COUNT(*)  from drugs""").fetchall()
    return reports_rows[0][0], reactions_rows[0][0], drugs_rows[0][0]

data=fetch_openfda_data()
reports_list, reactions_list, drugs_list =flatten_openfda_reports(data)
conn=load_to_duckdb(reports_list, reactions_list, drugs_list)        
reports_rows, reactions_rows, drugs_rows=validate_load(conn)

# joined_rows=(conn.execute("""SELECT
#     reports.safetyreportid,
#     reports.receivedate,
#     reactions.reactionmeddrapt
# FROM reports
# JOIN reactions
# ON reports.safetyreportid = reactions.safetyreportid 
# ORDER BY
#     reports.safetyreportid,
#     reactions.reactionmeddrapt""").fetchall())
      

    
print("reports_rows:")
print(reports_rows)
print("reactions_rows:")
print(reactions_rows)
print("drugs_rows:")
print(drugs_rows)