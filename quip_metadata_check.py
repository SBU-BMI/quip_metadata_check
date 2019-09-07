import json
import pandas as pd
import sys
import uuid
from os import path

required_columns = ["path","studyid","clinicaltrialsubjectid","imageid"]
def check_required_columns(pf):
    missing_columns = []
    for x in required_columns:
        if x not in pf.columns:
            missing_columns.append(x)
    return missing_columns

def check_rows_missing_values(pf):
    rows_missing_values = []
    for idx,row in pf.iterrows():
        for c in required_columns:
            if pd.isna(row[c]):
                rows_missing_values.append(idx+1)
                break;
    return rows_missing_values

def check_duplicate_rows(pf):
    duplicate_rows = []
    dr = pf.duplicated()
    for idx,row in dr.items():
        if row==True:
            duplicate_rows.append(idx+1)
    return duplicate_rows

def main(argv):
    inp_folder   = "/data/images"
    inp_manifest = "manifest.csv"
    out_error_json = "quip_error_log.json"
    if len(argv)==1:
       inp_manifest = argv[0]
    out_manifest = "quip_" + inp_manifest

    out_json = open(inp_folder + "/" + out_error_json,"w");
    all_log     = {}
    error_log   = []
    warning_log = []
    if not path.exists(inp_folder + "/" + inp_manifest):
        ierr = {}
        ierr["error_code"] = 1
        ierr["error_msg"] = "input manifest file does not exist."
        error_log.append(ierr)
        all_log["error_log"]   = error_log
        all_log["warning_log"] = warning_log
        json.dump(all_log,out_json)
        out_json.close()
        sys.exit(1)
    
    finp = open(inp_folder+"/"+inp_manifest)
    pf = pd.read_csv(finp,sep=',')
    
    pf["row_status"] = "ok"
    pf["file_uuid"]  = ""
    missing_columns = check_required_columns(pf)
    if len(missing_columns)!=0:
        ierr = {}
        ierr["error_code"] = 2
        ierr["error_msg"]  = "missing required columns."
        ierr["missing_columns"] = missing_columns
        error_log.append(ierr)
        all_log["error_log"]   = error_log
        all_log["warning_log"] = warning_log
        json.dump(all_log,out_json)
        out_json.close()
        sys.exit(2)
    
    rows_missing_values = check_rows_missing_values(pf)
    if len(rows_missing_values)!=0:
        iwarn = {}
        iwarn["warning_code"] = 1
        iwarn["warning_msg"]  = "rows missing values."
        iwarn["rows_missing_values"] = rows_missing_values
        warning_log.append(iwarn)
        all_log["warning_log"] = warning_log
    duplicate_rows = check_duplicate_rows(pf)
    if len(duplicate_rows)!=0:
        iwarn = {}
        iwarn["warning_code"] = 1
        iwarn["warning_msg"]  = "duplicate_rows"
        iwarn["duplicate_rows"] = duplicate_rows
        warning_log.append(iwarn)
        all_log["warning_log"] = warning_log
    for idx in rows_missing_values:
        pf.at[idx-1,"row_status"] = "missing_value"
    for idx in duplicate_rows:
        pf.at[idx-1,"row_status"] = "duplicate_row"
    for idx, row in pf.iterrows():
        pf.at[idx,"file_uuid"] = str(uuid.uuid1())
    
    all_log["error_log"]   = error_log
    all_log["warning_log"] = warning_log
    json.dump(all_log,out_json)
    out_json.close()
    
    out_csv = open(inp_folder+"/"+out_manifest,mode="w")
    pf.to_csv(out_csv,index=False)
    out_csv.close()
    sys.exit(0)

if __name__ == "__main__":
   main(sys.argv[1:])

