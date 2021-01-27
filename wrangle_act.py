#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import tweepy
import re
import os
import json
import time
import warnings
import matplotlib.pyplot as plt


# **Gathering data**

# In[2]:


# reading csv as a Pandas DataFrame
twitter_archive_enhanced = pd.read_csv('twitter-archive-enhanced.csv')


# In[3]:


# Downloading and saving data using Requests
url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)

with open('image-predictions.tsv', mode ='wb') as file:
    file.write(response.content)

#Reading TSV file
image_prediction = pd.read_csv('image-predictions.tsv', sep='\t' )


# In[ ]:


''''# Query Twitter API for each tweet in the Twitter archive and save JSON in a text file
# These are hidden to comply with Twitter's API terms and conditions
consumer_key = 'HIDDEN'
consumer_secret = 'HIDDEN'
access_token = 'HIDDEN'
access_secret = 'HIDDEN'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)'''


# In[ ]:


''''# NOTE TO STUDENT WITH MOBILE VERIFICATION ISSUES:
# df_1 is a DataFrame with the twitter_archive_enhanced.csv file. You may have to
# change line 17 to match the name of your DataFrame with twitter_archive_enhanced.csv
# NOTE TO REVIEWER: this student had mobile verification issues so the following
# Twitter API code was sent to this student from a Udacity instructor
# Tweet IDs for which to gather additional data via Twitter's API
tweet_ids = df_1.tweet_id.values
len(tweet_ids)'''


# In[ ]:


''''# Query Twitter's API for JSON data for each tweet ID in the Twitter archive
count = 0
fails_dict = {}
start = timer()
# Save each tweet's returned JSON as a new line in a .txt file
with open('tweet_json.txt', 'w') as outfile:
    # This loop will likely take 20-30 minutes to run because of Twitter's rate limit
    for tweet_id in tweet_ids:
        count += 1
        print(str(count) + ": " + str(tweet_id))
        try:
            tweet = api.get_status(tweet_id, tweet_mode='extended')
            print("Success")
            json.dump(tweet._json, outfile)
            outfile.write('\n')
        except tweepy.TweepError as e:
            print("Fail")
            fails_dict[tweet_id] = e
            pass
end = timer()
print(end - start)
print(fails_dict)'''


# In[4]:


# Read text file line by line to create dataframe
tweets_data = []
with open('tweet_json.txt') as file:
    for line in file:
        try:
            tweet = json.loads(line)
            tweets_data.append(tweet)
        except:
            continue
df_api = pd.DataFrame(tweets_data, columns=list(tweets_data[0].keys()))


# In[5]:


df_json = df_api[['id', 'retweet_count', 'favorite_count']]


# **ِAssess data**

# In[6]:


twitter_archive_enhanced


# In[7]:


image_prediction


# In[8]:


df_json


# In[9]:


twitter_archive_enhanced.loc[:, ['tweet_id']].nunique()


# In[10]:


image_prediction.loc[:, ['tweet_id']].nunique()


# In[11]:


df_json.loc[:, ['id']].nunique()


# In[12]:


image_prediction.loc[:, ['jpg_url']].nunique()


# In[13]:


twitter_archive_enhanced.loc[:, ['expanded_urls']].nunique()


# In[14]:


twitter_archive_enhanced.info()


# In[15]:


#twitter_archive_enh['pupper'].duplicated()
twitter_archive_enhanced['name'].describe()


# In[16]:


filteredList = list(filter(lambda x : len(x)  <5, twitter_archive_enhanced.name))
print('Filtered List : ', filteredList)


# In[ ]:





# 

# In[17]:


missing_value =(twitter_archive_enhanced.isnull().sum()/twitter_archive_enhanced.shape[0]).sort_values(ascending=False)*100


# In[18]:


missing_value = twitter_archive_enhanced.isna().mean().round(4)*100


# In[19]:


# precentage of missing value.
missing_value.plot.barh(figsize=(10,20));
plt.ylabel('Column name');
plt.xlabel('Percentage of nulls')


# **in_reply_to_status_id, in_reply_to_user_id, retweeted_status_id, retweeted_status_user_id & retweeted_status_timestamp are missing too much data and should be dropped**

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# 

# **Observation**
# 
# I notice there is missing some ID since in twitter arch is: 2356, image prediction is:2075 & json is 2110.
# 
# On the other hand, 'in_reply_to_status_id',  'in_reply_to_user_id',  'retweeted_status_id',  'retweeted_status_user_id',  'retweeted_status_timestamp', are missing more than 80% of data. 

# **Quality:**
# 
# 1-	source need to be enhanced since it is <a /a>
# 
# 2-	timestamp should be converted to datetime && tweet id to str
# 
# 3-	Delete 'in_reply_to_status_id', 'in_reply_to_user_id', 'retweeted_status_id', 'retweeted_status_user_id', 'retweeted_status_timestamp', since its missing a lot of data
# 
# 4-	There is many names that is not corret such as (a,as..etc) and it should be renames to non
# 
# 5-	Drop duplicate image url
# 
# 6-	Combine p1, p2 & p3 and create dog breed
# 
# 7-	Rating not always correct 
# 
# 8-	rename id in json to  tweet_id
# 
# **Tidiness:**
# 
# 1-	Dog stage
# 
# 2-	Create one master sheet
# 

# **Cleaning**
# 
# First, make a copy of the data

# In[ ]:





# In[20]:


Master_sheet = twitter_archive_enhanced.copy()
clean_image_prediction = image_prediction.copy()
clean_df_json = df_json.copy()


# **Define**: rename id to tweet id in json
# 
# **code**

# In[21]:


clean_df_json.rename(columns = {'id':'tweet_id'}, inplace = True)
clean_df_json.rename(columns = {'retweet_count':'retweet'}, inplace = True)
clean_df_json.rename(columns = {'favorite_count':'favorite'}, inplace = True)


# **Test**

# In[22]:


clean_df_json


# **Define** create on master sheet.
# 
# **code**

# In[23]:


Master_sheet =pd.merge(left=Master_sheet,
                                 right=clean_image_prediction, how='inner')
Master_sheet = Master_sheet.merge(clean_df_json, how='inner')


# **Test**

# In[24]:


Master_sheet.info()


# **Define**: Clean and enhance the source.
# 
# **Code**

# In[25]:


Master_sheet['source'] = Master_sheet['source'].apply(lambda x: re.findall(r'>(.*)<', x)[0])


# **Test**

# In[26]:


Master_sheet.head()


# **Define**: convert the data type
# 
# **Code**

# In[27]:


Master_sheet['tweet_id'] = Master_sheet['tweet_id'].astype('str')
Master_sheet['timestamp'] = pd.to_datetime(Master_sheet.timestamp)


# **Test**

# In[28]:


Master_sheet.info()


# **Define**: drop 'in_reply_to_status_id', 'in_reply_to_user_id', 'retweeted_status_id', 'retweeted_status_user_id', 'retweeted_status_timestamp'
# 
# **Code**

# In[29]:


Master_sheet.drop(['in_reply_to_status_id', 'in_reply_to_user_id', 'retweeted_status_id', 'retweeted_status_user_id', 'retweeted_status_timestamp'], axis=1, inplace=True)


# **Test**

# In[30]:


Master_sheet.info()


# **Define**: change the unmeaning names to None
# 
# **Code**

# In[31]:


Master_sheet["name"].replace({"a": None, "an": None,"this":None,"The":None, "O": None, "AI": None, "his":None, "such":None}, inplace=True)


# **Test**

# In[32]:


Master_sheet


# **Define:** Drop duplicate image url
# 
# **Code**

# In[33]:


Master_sheet = Master_sheet.drop_duplicates(subset=['jpg_url'], keep='last')


# **Test**

# In[34]:


sum(Master_sheet['jpg_url'].duplicated())


# **Define:** Combine p1,p2& p3
# 
# **Code**

# In[35]:


def dog_breed(row):
    if row['p1_dog']:
        return row['p1']
    elif row['p2_dog']:
        return row['p2']
    elif row['p3_dog']:
        return row['p3']
    else:
        return np.nan

Master_sheet['dog_breed'] = Master_sheet.apply(dog_breed, axis = 1)

# Drop extra columns
Master_sheet = Master_sheet.drop(['img_num', 'p1', 'p1_conf', 'p1_dog', 'p2', 'p2_conf', 'p2_dog', 'p3', 'p3_conf', 'p3_dog'], 1)


# **Test**

# In[36]:


Master_sheet.info()


# **Define:** drop all rating that more than 10
# 
# **Code**

# In[37]:


Master_sheet.rating_denominator= Master_sheet['rating_denominator']=10


# **Test**

# In[38]:


Master_sheet.head()


# **Define:** dog style
# 
# **Code**

# In[ ]:





# In[40]:


#Master_sheet['dog_stage'] = Master_sheet['text'].str.extract('(puppo|pupper|floofer|doggo)', expand=True)

# Drop 'doggo', 'floofer', 'pupper', 'puppo' columns
#Master_sheet = Master_sheet.drop(columns = ['doggo', 'floofer', 'pupper', 'puppo'])
#stage = []
#def dog_stage(row):

#    if row['doggo'] == 'doggo':
#        stage.append('doggo')
#    if row['floofer'] == 'floofer':
#        stage.append('floofer')
#    if row['pupper'] == 'pupper':
#        stage.append('pupper')
#    if row['puppo'] == 'puppo':
#        stage.append('puppo')
    
#    if not stage:
#        return "None"
#    else:
#        return ','.join(stage)
    
#Master_sheet['dog_stage'] = Master_sheet.apply(lambda row: dog_stage(row), axis=1)

# drop the columns
#Master_sheet.drop(["doggo", "floofer", "pupper", "puppo"], axis=1, inplace=True)

Master_sheet.loc[Master_sheet['doggo'] == 'doggo', 'dog_stage'] = 'doggo'
Master_sheet.loc[Master_sheet['floofer'] == 'floofer', 'dog_stage'] = 'floofer'
Master_sheet.loc[Master_sheet['pupper'] == 'pupper', 'dog_stage'] = 'pupper'
Master_sheet.loc[Master_sheet['puppo'] == 'puppo', 'dog_stage'] = 'puppo'

# dropping unneded doggo, floofer, pupper or poppo columns
Master_sheet = Master_sheet.drop(['doggo', 'floofer', 'pupper', 'puppo'], axis = 1)


# **Test**

# In[41]:


pd.value_counts(Master_sheet['dog_stage'])


# In[42]:


Master_sheet


# In[43]:


Master_sheet.info()


# **Storing, Analyzing & Visualization**

# In[44]:


Master_sheet.to_csv('twitter_archive_master.csv') 


# In[45]:


Master_sheet.head()


# In[47]:


figure_1 = Master_sheet.dog_stage.value_counts()

figure_1.plot(kind='bar', color='red', title= '', alpha =.75, figsize=(8,5))

# graph labels
plt.xlabel('dog_stage')
plt.ylabel('Number of dogs');


# In[48]:


figure_2 = Master_sheet.dog_breed.value_counts()[4::-1]

figure_2.plot(kind='bar', color='red', title= '', alpha =.75, figsize=(8,5))

# graph labels
plt.xlabel('dog_breed')
plt.ylabel('Number of dogs');


# In[49]:


figure_3 = Master_sheet.query('retweet').groupby("dog_stage")["dog_breed"].count().sort_values(ascending=False)

figure_3.plot(kind='bar', color='red', title= 'most dogs that got retweet', alpha =.75, figsize=(8,5))

# graph labels
plt.xlabel('dog stage')
plt.ylabel('Number of retweet');


# In[50]:


figure_4 = Master_sheet.query('favorite').groupby("dog_stage")["dog_breed"].count().sort_values(ascending=False)

figure_4.plot(kind='bar', color='red', title= 'most dogs that got favorite', alpha =.75, figsize=(8,5))
# graph labels
plt.xlabel('dog stage')
plt.ylabel('Number of favorite');


# In[ ]:





# In[ ]:




