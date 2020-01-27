import json
import pandas as pd
import sys
import uuid
from os import path
import argparse

parser = argparse.ArgumentParser(description="Combine metadata.")
parser.add_argument("--inpmeta",nargs="?",default="quip_manifest.csv",type=str,help="input manifest (metadata) file.")
parser.add_argument("--outmeta",nargs="?",default="quip_combined_manifest.csv",type=str,help="combined manifest (metadata) file.")
parser.add_argument("--inpdir",nargs="?",default="/data/images",type=str,help="input folder.")

def main(args):
    inp_folder = args.inpdir 
    inp_manifest_fname = args.inpmeta 
    out_manifest_fname = args.outmeta

    images_manifest=inp_folder+"/"+'images/quip_manifest.csv';
    metadata_manifest=inp_folder+"/"+'metadata_extract/quip_manifest.csv';
    converted_manifest=inp_folder+"/"+'converted/quip_manifest.csv';
    quality_manifest=inp_folder+"/"+'quality_check/quip_manifest.csv';

    p_images = pd.read_csv(images_manifest);
    for idx, row in p_images.iterrows():
        fname = row["path"];
        p_images.at[idx,"path"] = "images/"+fname;

    p_metadata = pd.read_csv(metadata_manifest);
    for idx, row in p_metadata.iterrows():
        if str(row["label_img"])!="nan":
            fname = row["label_img"];
            p_metadata.at[idx,"label_img"] = "metadata_extract/"+fname;
        if row["macro_img"]!="nan":
            fname = row["macro_img"];
            p_metadata.at[idx,"macro_img"] = "metadata_extract/"+fname;
        if row["thumb_img"]!="nan":
            fname = row["thumb_img"];
            p_metadata.at[idx,"thumb_img"] = "metadata_extract/"+fname;
        if row["metadata_json"]!="nan":
            fname = row["metadata_json"];
            p_metadata.at[idx,"metadata_json"] = "metadata_extract/"+fname;

    p_converted = pd.read_csv(converted_manifest);
    for idx, row in p_converted.iterrows():
        fname = row["converted_filename"];
        p_converted.at[idx,"converted_filename"] = "converted/"+fname;

    p_quality = pd.read_csv(quality_manifest);

    p_out = pd.merge(p_images,p_metadata,how='left',left_on='file_uuid',right_on='file_uuid');
    p_out = pd.merge(p_out,p_converted,how='left',left_on='file_uuid',right_on='file_uuid');
    p_out = pd.merge(p_out,p_quality,how='left',left_on='file_uuid',right_on='file_uuid');

    p_out.to_csv(inp_folder+"/"+out_manifest_fname,mode="w",index=False);

    sys.exit(0)

if __name__ == "__main__":
    args = parser.parse_args(sys.argv[1:]);
    main(args)

