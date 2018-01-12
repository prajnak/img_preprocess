
# coding: utf-8

import boto3
import botocore
import pandas as pd
from io import BytesIO
from PIL import Image
import imghdr
import numpy as np

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

def download_image(row):
    string_io = BytesIO()
    s3.download_fileobj(Bucket=BUCKET_NAME, Key=row['s3key'], Fileobj=string_io)
    return string_io

def get_image_format(dat):
    return imghdr.what(None, dat.getvalue())

def transform_image(row):
    img_format = get_image_format(row['rawdata'])
    if img_format == None:
        return "Invalid"
    img_dat = Image.open(row['rawdata'])
    if img_format != 'jpeg':
        rgb_img_dat = img_dat.convert('RGB')
        resized = rgb_img_dat.resize(TARGET_SIZE)
    else:
        resized = img_dat.resize(TARGET_SIZE)
    out_bytes = BytesIO()
    resized.save(out_bytes, format='jpeg')
    out_bytes.seek(0)
    return out_bytes

def upload_df_to_s3(df, split=""):
    print('Uploading to bucket:{0}'.format(BUCKET_NAME))
    for index, row in df.iterrows():
        category_tracker[row['category']] += 1
        s3key = 'processed/{0}/{1}/img{2}.jpg'.format(row['category'],
                                                      split,
                                                      category_tracker[row['category']])
        print('Uploading file:{0}'.format(s3key))
        s3.upload_fileobj(Bucket=BUCKET_NAME, Fileobj=row['resized'], Key=s3key, Callback=lambda x: print(x))

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
    print("Dimensions of created image dataframe: {0}".format(images_df.shape))

    print("Downloading image data from S3")
    images_df['rawdata'] = images_df.apply(download_image, axis=1) # this will take a while
    print("Successfully downloaded all images into memory")
    print("Resizing and converting all images to JPEG")
    images_df['resized'] = images_df.apply(transform_image, axis=1)
    print("Successfully preprocessed all images")
    print("There are {0} invalid images".format(images_df[images_df['resized'] == "Invalid"].shape))

    print("Generating data splits")
    train, validate, test = np.split(images_df.sample(frac=1), [int(.7*len(images_df)), int(.9*len(images_df))])

    print("Validate: {0}, Train: {1}, Test: {2}".format(validate.shape, test.shape, train.shape))

    category_tracker = dict(zip(categories, [0]*len(categories)))
    upload_df_to_s3(train, "train")
    upload_df_to_s3(test, "test")
    upload_df_to_s3(validate, "validate")
    print("Finished uploading all preprocessed files to S3")
