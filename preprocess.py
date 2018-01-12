
# coding: utf-8

import boto3
import botocore
import pandas as pd
from io import BytesIO
from PIL import Image
import imghdr
import numpy as np
import concurrent.futures

# constants and globals
BUCKET_NAME = "nanonets-platform-task"
TARGET_SIZE = (300, 300)
TARGET_COLORS = (255,255,255)

s3 = boto3.client('s3', 'us-west-2')

def get_files():
    s3 = boto3.client('s3', 'us-west-2')
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket = BUCKET_NAME)
    images = []
    for page in page_iterator:
        for a in page['Contents']:
            category, filename = a['Key'].split('/', 1)
            if category == 'processed': # this is where we store processed images.
                continue
            if a['Key'].endswith('jpg'):
                image_format = 'jpg'
            elif a['Key'].endswith('png'):
                image_format = 'png'
            else:
                image_format = 'unknown'
            images.append({'category': category, 'name': filename, 's3key': a['Key'], 'format': image_format})

    return images

def download_image(filename):
    string_io = BytesIO()
    s3.download_fileobj(Bucket=BUCKET_NAME, Key=filename, Fileobj=string_io)
    return string_io

def get_image_format(dat):
    return imghdr.what(None, dat.getvalue())

def transform_image(dat):
    img_format = get_image_format(dat)
    if img_format == None:
        return "Invalid"
    img_dat = Image.open(dat)
    if img_format != 'jpeg':
        rgb_img_dat = img_dat.convert('RGB')
        resized = rgb_img_dat.resize(TARGET_SIZE)
    else:
        resized = img_dat.resize(TARGET_SIZE)
    out_bytes = BytesIO()
    resized.save(out_bytes, format='jpeg')
    out_bytes.seek(0)
    return out_bytes

def row_pipeline(row, split):
    image_dat = download_image(row['s3key'])
    resized_image = transform_image(image_dat)
    if resized_image == "Invalid":
        return "Invalid"
    upload_img_to_s3(resized_image, row['category'], row['name'], split)
    return "Done" 

def upload_img_to_s3(img, category, filename, dataSplit=""):
    s3key = 'processed/{0}/{1}/{2}.jpg'.format(category,
                                                  dataSplit,
                                                  filename)
    print('Uploading file:{0}'.format(s3key))
    s3.upload_fileobj(Bucket=BUCKET_NAME, Fileobj=img, Key=s3key, Callback=lambda x: print(x))

def process_df(df_list, splits=[]):
    pool = concurrent.futures.ThreadPoolExecutor(10)
    futures = []
    for df, split in zip(df_list, splits):
        for index, row in df.iterrows():
            futures.append(pool.submit(row_pipeline, row, split))
    
    print(concurrent.futures.wait(futures))
        
if __name__ == "__main__":
    print("Getting all files present in the the bucket: {0}".format(BUCKET_NAME))
    images = get_files()
    print("Loaded metadata from S3 for {0} files".format(len(images)))
    categories = {image['category'] for image in images}
    print("There are {0} categories in the configured bucket: {1}".format(len(categories), categories))

    unknown = [a for a in images if a['format'] == 'unknown']
    print("There are {0} files with unknown formats".format(len(unknown)))

    print("Creating a dataframe with image metadata")
    images_df = pd.DataFrame(images)
    images_df = images_df[images_df['format'] != 'unknown'] # drop unknown file types

    print("Generating data splits")
    train, test, validate = np.split(images_df.sample(frac=1), [int(.7*len(images_df)), int(.9*len(images_df))])

    print("Running pipeline")
                           
    process_df([train, test, validate], ["train", "test", "validate"])
    
    #train['resize_status'] = train.apply(row_pipeline, axis=1, args=("train",))
    #test['resize_status'] = test.apply(row_pipeline, axis=1, args=("test",))
    #validate['resize_status'] = validate.apply(row_pipeline, axis=1, args=("validate",))
    print("Successfully preprocessed all images")
    
