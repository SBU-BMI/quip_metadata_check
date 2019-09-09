import json
import pandas as pd
import sys
import uuid
from os import path
import argparse

error_info = {}
error_info["no_error"] = { "code":"0", "msg":"no-error" }
error_info["missing_file"] = { "code":"101", "msg":"input-file-missing" }
error_info["missing_values"] = { "code":"102", "msg":"missing-values" }
error_info["duplicate_rows"] = { "code":"103", "msg":"duplicate-rows" }
error_info["file_format"] = { "code":"104", "msg":"file-format-error" }
error_info["missing_columns"] = { "code":"105", "msg":"missing-columns" }

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

parser = argparse.ArgumentParser(description="Metadata checker.")
parser.add_argument("--inpmeta",nargs="?",default="manifest.csv",type=str,help="input manifest (metadata) file.")
parser.add_argument("--outmeta",nargs="?",default="quip_manifest.csv",type=str,help="output manifest (metadata) file.")
parser.add_argument("--errfile",nargs="?",default="quip_manifest_error_log.json",type=str,help="error log file.")
parser.add_argument("--inpdir",nargs="?",default="/data/images",type=str,help="input folder.")

def main(args):
    inp_folder = args.inpdir 
    inp_manifest_fname = args.inpmeta 
    out_error_fname = args.errfile 
    out_manifest_fname = args.outmeta

    # error and warning log
    all_log = {}
    all_log["error"] = []
    all_log["warning"] = []
    out_error_fd = open(inp_folder + "/" + out_error_fname,mode="w");
    try:
        inp_metadata_fd = open(inp_folder+"/"+inp_manifest_fname,mode="r")
    except OSError:
        all_log["error"].append(error_info["missing_file"])
        json.dump(all_log,out_error_fd)
        out_error_fd.close()
        sys.exit(1)
    pf = pd.read_csv(inp_metadata_fd,sep=',')
   
    # Check if required columns are missing
    missing_columns = check_required_columns(pf)
    if len(missing_columns)!=0:
        ierr = error_info["missing_columns"];
        ierr["missing_columns"] = missing_columns 
        all_log["error"].append(ierr)
        json.dump(all_log,out_error_fd)
        out_error_fd.close()
        inp_metadata_fd.close()
        sys.exit(2)
   
    # Check rows missing values
    rows_missing_values = check_rows_missing_values(pf)
    if len(rows_missing_values)!=0:
        ierr = error_info["missing_values"];
        ierr["missing_values"] = rows_missing_values 
        all_log["error"].append(ierr)

    # Check for duplicate rows
    duplicate_rows = check_duplicate_rows(pf)
    if len(duplicate_rows)!=0: 
        ierr = error_info["duplicate_rows"];
        ierr["duplicate_rows"] = duplicate_rows
        all_log["warning"].append(ierr)

    # Store row wise status
    pf["error_code"] = error_info["no_error"]["code"]
    pf["error_msg"]  = error_info["no_error"]["msg"]
    pf["file_uuid"]  = ""
    for idx in rows_missing_values:
        pf.at[idx-1,"error_code"] = error_info["missing_values"]["code"]
        pf.at[idx-1,"error_msg"]  = error_info["missing_values"]["msg"]
    for idx in duplicate_rows: 
        if pf["error_code"][idx-1]==error_info["no_error"]["code"]: 
            pf.at[idx-1,"error_code"] = error_info["duplicate_rows"]["code"]
            pf.at[idx-1,"error_msg"]  = error_info["duplicate_rows"]["msg"]
        else: 
            pf.at[idx-1,"error_code"] = pf["error_code"][idx-1]+";"+error_info["duplicate_rows"]["code"]
            pf.at[idx-1,"error_msg"]  = pf["error_msg"][idx-1]+";"+error_info["duplicate_rows"]["msg"]
    for idx, row in pf.iterrows():
        filename, file_extension = path.splitext(row["path"])
        pf.at[idx,"file_uuid"] = str(uuid.uuid1()) + file_extension
    
    json.dump(all_log,out_error_fd)
    out_error_fd.close()
    
    out_metadata_fd = open(inp_folder+"/"+out_manifest_fname,mode="w")
    pf.to_csv(out_metadata_fd,index=False)

    inp_metadata_fd.close()
    out_error_fd.close()
    out_metadata_fd.close()

    sys.exit(0)

if __name__ == "__main__":
    args = parser.parse_args(sys.argv[1:]);
    main(args)
