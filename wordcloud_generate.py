import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS

import nltk
import re

from gensim.models import Word2Vec
from nltk.tokenize import word_tokenize

import warnings
warnings.filterwarnings("ignore")

from nltk.corpus import stopwords

def word_transform(df):
#   function for cleaning the review statements

    content_df = df[['reviewBody','reviewRating']]
    #removing special charecters
    content_df['reviewBody'] = content_df['reviewBody'].apply(lambda x: re.sub(re.compile("[^\w\s]"), "", x))
    #removing space from front and back of the string
    content_df['reviewBody']= content_df['reviewBody'].apply(lambda x : x.strip())
    # Converting data into lower
    content_df['reviewBody']= content_df['reviewBody'].apply(lambda x : x.lower())
    return content_df

def remove_stopwords(text):
    """function to remove the stopwords"""
    return " ".join([word for word in str(text).split() if word not in stop_words])

def generate_image(content_df):
    comment_words = ''
    stopwords = set(STOPWORDS)
 
    # iterate through the csv file
    for val in content_df['reviewBody']:
     
        # typecaste each val to string
        val = str(val)
 
        # split the value
        tokens = val.split()
     
        # Converts each token into lowercase
        for i in range(len(tokens)):
            tokens[i] = tokens[i].lower()
     
        comment_words += " ".join(tokens)+" "
 
    wordcloud = WordCloud(width = 800, height = 800,
                background_color ='white',
                stopwords = stopwords,
                min_font_size = 10).generate(comment_words)
 
    # plot the WordCloud image                      
    plt.figure(figsize = (8, 8), facecolor = None)
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.tight_layout(pad = 0)
    #set the image details here
    plt.savefig('wordcloud.png', dpi=300, bbox_inches='tight')
 

#Execution starts from here
if __name__ == '__main__':
    #get the stopwords
    stop_words = set(stopwords.words('english')) 

    ## Read the file 
    df = pd.read_csv('vitamin_c.csv') ## mention the file name here
    df.head()

    df = df.iloc[::-2][::-1] ##removing even index because they are all 5 star

    #separate dataframe with ratings >4.0 (+ve) and <3.0 (-ve)
    df_pos = df[df['reviewRating']>=4.0]
    df_neg = df[df['reviewRating']<3.0]

    #change the df_pos/df_neg for generating +ve or -ve wordcloud
    content_df = word_transform(df_neg)

    #removing stopwords from each row of the datframe
    content_df['reviewBody'] =  content_df['reviewBody'].apply(lambda x : remove_stopwords(x))
    #removing numerics from the dataframe 
    content_df['reviewBody'] =  content_df['reviewBody'].apply(lambda x : re.sub(r"\d+", "", x))

    #tokenize the words
    content_df['reviewBody'] =  content_df['reviewBody'].apply(lambda x : word_tokenize(x))

    #Generate the image
    generate_image(content_df)
