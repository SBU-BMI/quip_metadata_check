import json
import pandas as pd
import sys
import uuid
import hashlib
from os import path
import argparse

error_info = {}
error_info["no_error"] = { "code":0, "msg":"no-error" }
error_info["missing_file"] = { "code":101, "msg":"input-file-missing" }
error_info["missing_values"] = { "code":102, "msg":"missing-values" }
error_info["duplicate_rows"] = { "code":103, "msg":"duplicate-rows" }
error_info["file_format"] = { "code":104, "msg":"file-format-error" }
error_info["missing_columns"] = { "code":105, "msg":"missing-columns" }
error_info["column_lengths"] = { "code":106, "msg":"column-lengths" }

def check_required_columns(pf,required_columns):
    missing_columns = []
    for x in required_columns:
        if x not in pf.columns:
            missing_columns.append(x)
    return missing_columns

def check_column_lengths(pf,required_columns,column_lengths): 
    problem_rows = []
    for idx,row in pf.iterrows():
        for c in required_columns:
            if column_lengths[c]>0 and pd.isna(row[c])==False: 
                if len(str(row[c]))>column_lengths[c]:
                    p = {}
                    p["column_name"] = c
                    p["row_id"] = idx+1
                    problem_rows.append(p)
    return problem_rows

def check_rows_missing_values(pf,required_columns):
    rows_missing_values = []
    for idx,row in pf.iterrows():
        for c in required_columns:
            if row[c].strip()=="" or pd.isna(row[c]):
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

def compute_md5(fname):
    md5h = hashlib.md5()
    with open(fname,"rb") as f:
         for byte_block in iter(lambda: f.read(4096),b""):
             md5h.update(byte_block)
    return md5h.hexdigest()

parser = argparse.ArgumentParser(description="Metadata checker.")
parser.add_argument("--inpmeta",nargs="?",default="manifest.csv",type=str,help="input manifest (metadata) file.")
parser.add_argument("--outmeta",nargs="?",default="quip_manifest.csv",type=str,help="output manifest (metadata) file.")
parser.add_argument("--errfile",nargs="?",default="quip_wsi_error_log.json",type=str,help="error log file.")
parser.add_argument("--inpdir",nargs="?",default="/data/images",type=str,help="input folder.")
parser.add_argument("--cfgfile",nargs="?",default="config.json",type=str,help="config for required columns, etc.")
parser.add_argument("--md5",nargs="?",default="y",type=str,help="compute md5 instead of uuid.")
parser.add_argument("--slide",nargs="?",default="",type=str,help="one slide to check.")

def process_manifest_file(args):
    inp_folder = args.inpdir 
    inp_manifest_fname = args.inpmeta 
    out_error_fname = args.errfile 
    out_manifest_fname = args.outmeta
    cfg_fname = args.cfgfile
    md5_check = args.md5

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

    # read config file
    try: 
        cfg_file_fd = open(cfg_fname,mode="r")
    except OSError:
        all_log["error"].append(error_info["missing_file"])
        json.dump(all_log,out_error_fd)
        out_error_fd.close()
        sys.exit(1)
    cfg_data = cfg_file_fd.read()
    cfg_json = json.loads(cfg_data)
    required_columns = cfg_json['required_columns']
    column_lens  = cfg_json['column_lengths']
    column_lengths = {}
    for i in range(len(required_columns)):
        column_lengths[required_columns[i]] = column_lens[i]

    pf = pd.read_csv(inp_metadata_fd,sep=',')
   
    # Check if required columns are missing
    missing_columns = check_required_columns(pf,required_columns)
    if len(missing_columns)!=0:
        ierr = error_info["missing_columns"];
        ierr["missing_columns"] = missing_columns 
        all_log["error"].append(ierr)
        json.dump(all_log,out_error_fd)
        out_error_fd.close()
        inp_metadata_fd.close()
        sys.exit(2)

    # Check if column values are shorter than max length allowed
    problem_rows = check_column_lengths(pf,required_columns,column_lengths)
    if len(problem_rows)!=0:
        ierr = error_info["column_lengths"]
        ierr["column_lengths"] = problem_rows
        all_log["error"].append(ierr)
   
    # Check rows missing values
    rows_missing_values = check_rows_missing_values(pf,required_columns)
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
    pf["manifest_error_code"] = str(error_info["no_error"]["code"])
    pf["manifest_error_msg"]  = error_info["no_error"]["msg"]
    pf["file_uuid"]  = ""
    pf["file_ext"]   = ""
    for idx in rows_missing_values:
        pf.at[idx-1,"manifest_error_code"] = str(error_info["missing_values"]["code"])
        pf.at[idx-1,"manifest_error_msg"]  = error_info["missing_values"]["msg"]
    for idx in range(len(problem_rows)):
        idx = problem_rows[idx]["row_id"];
        if str(pf["manifest_error_code"][idx-1])==str(error_info["no_error"]["code"]): 
            pf.at[idx-1,"manifest_error_code"] = str(error_info["column_lengths"]["code"]) 
            pf.at[idx-1,"manifest_error_msg"]  = error_info["column_lengths"]["msg"]
        else:
            pf.at[idx-1,"manifest_error_code"] = str(pf["manifest_error_code"][idx-1])+";"+str(error_info["collumn_lengths"]["code"])
            pf.at[idx-1,"manifest_error_msg"]  = pf["manifest_error_msg"][idx-1]+";"+error_info["collumn_lengths"]["msg"]
    for idx in duplicate_rows: 
        if str(pf["manifest_error_code"][idx-1])==str(error_info["no_error"]["code"]): 
            pf.at[idx-1,"manifest_error_code"] = str(error_info["duplicate_rows"]["code"])
            pf.at[idx-1,"manifest_error_msg"]  = error_info["duplicate_rows"]["msg"]
        else: 
            pf.at[idx-1,"manifest_error_code"] = str(pf["manifest_error_code"][idx-1])+";"+str(error_info["duplicate_rows"]["code"])
            pf.at[idx-1,"manifest_error_msg"]  = pf["manifest_error_msg"][idx-1]+";"+error_info["duplicate_rows"]["msg"]
    for idx, row in pf.iterrows():
        filename, file_extension = path.splitext(row["path"])
        if md5_check=='y':
            pf.at[idx,"file_uuid"] = compute_md5(inp_folder+"/"+row["path"])
        else:
            pf.at[idx,"file_uuid"] = str(uuid.uuid1()) 
        pf.at[idx,"file_ext"] = str(file_extension)
    
    json.dump(all_log,out_error_fd)
    out_error_fd.close()

    out_metadata_fd = open(inp_folder+"/"+out_manifest_fname,mode="w")
    pf.to_csv(out_metadata_fd,index=False)

    inp_metadata_fd.close()
    out_error_fd.close()
    out_metadata_fd.close()
    return 0

def process_single_slide(args):
    inp_folder = args.inpdir 
    inp_manifest_fname = args.inpmeta 
    out_error_fname = args.errfile 
    out_manifest_fname = args.outmeta
    cfg_fname = args.cfgfile
    md5_check = args.md5
    inp_slide = args.slide

    # error and warning log
    all_log = {}
    all_log["error"] = []
    all_log["warning"] = []
    return_msg = {}
    return_msg["status"] = all_log
    return_msg["output"] = {}

    # read config file
    try: 
        cfg_file_fd = open(cfg_fname,mode="r")
    except OSError:
        all_log["error"].append(error_info["missing_file"])
        return_msg["status"] = all_log
        print(return_msg)
        sys.exit(1)
    cfg_data = cfg_file_fd.read()
    cfg_json = json.loads(cfg_data)
    required_columns = cfg_json['required_columns']
    column_lens  = cfg_json['column_lengths']
    column_lengths = {}
    for i in range(len(required_columns)):
        column_lengths[required_columns[i]] = column_lens[i]

    inp_json = {} 
    r_json = json.loads(inp_slide)
    for item in r_json:
        inp_json[item] = [r_json[item]]
    pf = pd.DataFrame.from_dict(inp_json)
   
    # Check if required columns are missing
    missing_columns = check_required_columns(pf,required_columns)
    if len(missing_columns)!=0:
        ierr = error_info["missing_columns"];
        ierr["missing_columns"] = missing_columns 
        all_log["error"].append(ierr)
        return_msg["status"] = all_log
        print(return_msg)
        sys.exit(2)

    # Check if column values are shorter than max length allowed
    problem_rows = check_column_lengths(pf,required_columns,column_lengths)
    if len(problem_rows)!=0:
        ierr = error_info["column_lengths"]
        ierr["column_lengths"] = problem_rows
        all_log["error"].append(ierr)
   
    # Check rows missing values
    rows_missing_values = check_rows_missing_values(pf,required_columns)
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
    pf["manifest_error_code"] = str(error_info["no_error"]["code"])
    pf["manifest_error_msg"]  = error_info["no_error"]["msg"]
    pf["file_uuid"]  = ""
    pf["file_ext"]   = ""
    for idx in rows_missing_values:
        pf.at[idx-1,"manifest_error_code"] = str(error_info["missing_values"]["code"])
        pf.at[idx-1,"manifest_error_msg"]  = error_info["missing_values"]["msg"]
    for idx in range(len(problem_rows)):
        idx = problem_rows[idx]["row_id"];
        if str(pf["manifest_error_code"][idx-1])==str(error_info["no_error"]["code"]): 
            pf.at[idx-1,"manifest_error_code"] = str(error_info["column_lengths"]["code"]) 
            pf.at[idx-1,"manifest_error_msg"]  = error_info["column_lengths"]["msg"]
        else:
            pf.at[idx-1,"manifest_error_code"] = str(pf["manifest_error_code"][idx-1])+";"+str(error_info["collumn_lengths"]["code"])
            pf.at[idx-1,"manifest_error_msg"]  = pf["manifest_error_msg"][idx-1]+";"+error_info["collumn_lengths"]["msg"]
    for idx in duplicate_rows: 
        if str(pf["manifest_error_code"][idx-1])==str(error_info["no_error"]["code"]): 
            pf.at[idx-1,"manifest_error_code"] = str(error_info["duplicate_rows"]["code"])
            pf.at[idx-1,"manifest_error_msg"]  = error_info["duplicate_rows"]["msg"]
        else: 
            pf.at[idx-1,"manifest_error_code"] = str(pf["manifest_error_code"][idx-1])+";"+str(error_info["duplicate_rows"]["code"])
            pf.at[idx-1,"manifest_error_msg"]  = pf["manifest_error_msg"][idx-1]+";"+error_info["duplicate_rows"]["msg"]
    for idx, row in pf.iterrows():
        filename, file_extension = path.splitext(row["path"])
        if md5_check=='y':
            pf.at[idx,"file_uuid"] = compute_md5(inp_folder+"/"+row["path"])
        else:
            pf.at[idx,"file_uuid"] = str(uuid.uuid1()) 
        pf.at[idx,"file_ext"] = str(file_extension)
    
    return_msg["status"] = all_log
    return_msg["output"] = pf.to_dict(orient='records')
    print(return_msg)
    return 0

def main(args):
    if args.slide.strip()=="":
        process_manifest_file(args)
    else:
        process_single_slide(args)

    sys.exit(0)

if __name__ == "__main__":
    args = parser.parse_args(sys.argv[1:]);
    main(args)
