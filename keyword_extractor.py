from autoextract.sync import request_raw
import json
import pandas as pd
import numpy as np
import nltk
import re
import yake
from nltk.corpus import stopwords
import boto3
import botocore
import dotenv
import os
import io
from botocore.exceptions import ClientError

dotenv.load_dotenv()

key = 'dataLocationProtocol'
value = os.getenv(key)
filename = os.getenv('inputdatalocation')


def return_prod_list(urls):
    #function to get all the product details in a list from webscrapping

    prod_dtls_list = []
    for url in urls:
        query = [{
        'url': url,
        'pageType': 'product'
        }]
        results = request_raw(query, api_key='put_api_key_here')
        prod = results[0]['product']
        print(list(prod.values())[0])
        prod_dtls_list.append(list(prod.values())[0])
    return prod_dtls_list

def remove_stopwords(text):
    """function to remove the stopwords"""
    return " ".join([word for word in str(text).split() if word not in stop_words])

def text_clean(prod_dtls_list):
    #text cleaning
    for i in range(len(prod_dtls_list)):
        prod_dtls_list[i] = prod_dtls_list[i].lower().strip()
        prod_dtls_list[i] = re.sub(re.compile("[^\w\s]"), "", prod_dtls_list[i])#remove special charecter
        prod_dtls_list[i] = re.sub(r"\d+", "", prod_dtls_list[i]) # remove numericals
        #removing stopwords from each row of the datframe
        prod_dtls_list[i] = remove_stopwords(prod_dtls_list[i])

    prod_dtls_merged = ' '.join(prod_dtls_list) #joined all the product names
    return prod_dtls_merged

def keyword_extraction(prod_dtls_merged):
    #keyword extraction with yake 

    kw_extractor = yake.KeywordExtractor()
    language = "en"
    max_ngram_size = 1
    deduplication_threshold = 0.3
    numOfKeywords = 7
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(prod_dtls_merged)

    list_of_words = [x[0] for x in keywords]  # extracted all the keywords

    print(list_of_words)

    list_of_words.insert(0,'product_name')
    keyword_df = pd.DataFrame(columns=list_of_words)
    prod_name = ['Farm-Honey','Indigenous-Honey','HONEY-SHOP'] #this can be taken from UI
    keyword_df['product_name'] = prod_name 


    for col in keyword_df.columns:
        if col == 'product_name':
            continue
        for i in range(len(prod_dtls_list)):
            counter=False
            if col in prod_dtls_list[i].split():
                counter=True
                keyword_df[col].iloc[i]='✓'
            if counter == False:
                keyword_df[col].iloc[i]='X'

    # keyword_df.to_excel('keyword.xlsx',index=None)
    return keyword_df

def write_file_s3(df_write,bucket_name, object_name):
    #this function write to s3 backet
    #df_write = dataframe which will be converted to .csv
    #bucket_name = s3 bucket name
    #object_name = s3 file location

    if (os.getenv(key) == "s3"):
        try:
            s3 = boto3.client('s3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name='us-west-2')
            with io.BytesIO() as output:
              with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                  df_write.to_excel(writer)
              data = output.getvalue()
              s3.put_object(Bucket=os.getenv(bucket_name), Key=os.getenv(object_name), Body=data)
                  
        except ClientError as e:
            logging.error(e)

#Execution starts from here
if __name__ == '__main__':
    
    # nltk.download('stopwords') #download if necessary 

    stop_words = set(stopwords.words('english'))

    urls = ['https://www.amazon.in/Farm-Honey-Wild-Unprocessed-250/dp/B0762P81W7/ref=sr_1_1?dchild=1&keywords=Farm+Honey&qid=1635155116&sr=8-1',
    'https://www.amazon.in/Indigenous-Unprocessed-Unfiltered-Unpasteurized-Disorders/dp/B07H5PVCH7/ref=sr_1_1_sspa?dchild=1&keywords=INDIGENOUS+HONEY&qid=1635153847&sr=8-1-spons&psc=1&spLa=ZW5jcnlwdGVkUXVhbGlmaWVyPUEzTjdPWE5MMTJFUzhMJmVuY3J5cHRlZElkPUEwMTU2MDM4MkROR0lCQ0xKT0MwNCZlbmNyeXB0ZWRBZElkPUEwMTk3Mjg4MkdOQTE4U1A4VUU5SyZ3aWRnZXROYW1lPXNwX2F0ZiZhY3Rpb249Y2xpY2tSZWRpcmVjdCZkb05vdExvZ0NsaWNrPXRydWU=',
    'https://www.amazon.in/HONEY-SHOP-Forest-collected-Natural/dp/B075ZVMGDC/ref=sr_1_1_sspa?crid=2S3O2LFT4ORQP&dchild=1&keywords=the+honey+shop&qid=1635155831&sprefix=the+honey+sh%2Caps%2C301&sr=8-1-spons&psc=1&spLa=ZW5jcnlwdGVkUXVhbGlmaWVyPUEyTEdWR0U2VjczSTFUJmVuY3J5cHRlZElkPUEwMjExNjkxM0swRlRTRDJZVDY1MCZlbmNyeXB0ZWRBZElkPUEwNDcyMTUyM0lTRU84Tk5IVjdTMiZ3aWRnZXROYW1lPXNwX2F0ZiZhY3Rpb249Y2xpY2tSZWRpcmVjdCZkb05vdExvZ0NsaWNrPXRydWU=']

    prod_dtls_list = return_prod_list(urls)
    prod_dtls_merged = text_clean(prod_dtls_list)
    keyword_df = keyword_extraction(prod_dtls_merged)
    write_file_s3(df_write=keyword_df,bucket_name='outputs3bucket',object_name='Outputs3object')
    
