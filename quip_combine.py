import json
import pandas as pd
import sys
import uuid
from os import path
import argparse

p_images = pd.read_csv('images/quip_manifest.csv');
for idx, row in p_images.iterrows():
    fname = row["path"];
    p_images.at[idx,"path"] = "images/"+fname;

p_metadata = pd.read_csv('metadata_extract/quip_manifest.csv');
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

p_converted = pd.read_csv('converted/quip_manifest.csv');
for idx, row in p_converted.iterrows():
    fname = row["converted_filename"];
    p_converted.at[idx,"converted_filename"] = "converted/"+fname;

p_quality = pd.read_csv('quality_check/quip_manifest.csv');

p_out = pd.merge(p_images,p_metadata,how='left',left_on='file_uuid',right_on='file_uuid');
p_out = pd.merge(p_out,p_converted,how='left',left_on='file_uuid',right_on='file_uuid');
p_out = pd.merge(p_out,p_quality,how='left',left_on='file_uuid',right_on='file_uuid');

p_out.to_csv('quip_combined_manifest.csv',mode="w",index=False);

