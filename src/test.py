import pandas as pd 
import random
import time
from utils.moderator import get_score
from utils.reddit_db import reddit_DB
from reddit import Client
client = Client()
db = reddit_DB()
crawl_list =['Conservatives','moderatepolitics','Ask_Politics','PoliticalDiscussion','anime_titties','PoliticsPeopleTwitter']
for i in crawl_list:
    #df = client.get_posts_by_timestamp('PoliticsPeopleTwitter','1698811200','1700110800')
    start_time = time.time()
    query_count = 0
    df = client.get_posts_by_timestamp(i,'1700110800','1701147600')
    for pid in df['id']:
            print(i)
            comms = pd.DataFrame()
            elapsed_time = time.time() - start_time
            if elapsed_time < 60 and query_count > 9:
                # Wait for the next minute before making more queries
                time.sleep(60 - elapsed_time)
                query_count = 0
                start_time = time.time()
                
            try : 
                comms = client.get_comments(i,pid,limit=100)
                if not comms.empty:
                    hate = [get_score(comm) for comm in comms['body']]
                    comms['hate_speech'] = hate
                    db.insert_posts_dataframe(comms,table_name='comments')
            except Exception as e :
                print("Error getting comments",e)
                time.sleep(60)
            query_count+=1
    else :
        print('Haha ya screwed')
    db.insert_posts_dataframe(df)
    time.sleep(random.randint(1,10))
    print(df)