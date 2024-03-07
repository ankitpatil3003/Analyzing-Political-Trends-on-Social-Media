import faktory
import logging
import pandas as pd
import time
from faktory import Worker
from reddit import Client
from datetime import datetime, timedelta
from utils.moderator import get_score
from utils.reddit_db import reddit_DB
from utils.red_stat import DataCollectionStats
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
df = pd.DataFrame()
post_ids = set()  # Use a set to store post IDs to prevent duplicates
comment_ids = set()
db = reddit_DB()
def get_old_comments(subreddit:str):
    """Get old post from the database for a particular subreddit and fetches top 100 comments
        Args:
            subreddit (str): required subreddit to mine eg. 'Python' 

        Returns:
            pandas.Dataframe: returns dataframe containing post from given subreddit
    """
    client = Client()
    start_time = time.time()
    query_count = 0
    df = db.get_posts_dataframe(subreddit)
    if not df.empty:
        post_ids = list(df['id'])
        for pid in post_ids:
            comms = pd.DataFrame()
            elapsed_time = time.time() - start_time
            if elapsed_time < 60 and query_count > 9:
                # Wait for the next minute before making more queries
                time.sleep(60 - elapsed_time)
                query_count = 0
                start_time = time.time()
            try : 
                comms = client.get_comments(subreddit,pid,limit=100)
                if not comms.empty:
                    hate = [get_score(comm) for comm in comms['body']]
                    comms['hate_speech'] = hate
                    db.insert_posts_dataframe(comms,table_name='comments')
            except Exception as e :
                print("Error getting comments",e)
                time.sleep(10)
            query_count+=1
    else :
        print('Haha ya screwed')


def get_new_comments(subreddit:str, new_post_ids):   
    """ Fetches new comments for freshely polled posts 
        Args:
            subreddit (str): required subreddit to mine eg. 'Python' 
            new_post_ids (list of str): List of ids of newly posted posts

        Returns:
            None
    """
    client = Client()
    start_time = time.time()
    query_count = 0
    for i in new_post_ids:
        try :
            comms = pd.DataFrame()
            elapsed_time = time.time() - start_time
            if elapsed_time < 60 and query_count > 5:
                # Wait for the next minute before making more queries
                time.sleep(60 - elapsed_time)
                query_count = 0
                start_time = time.time()
            comms = client.poll_comments(comms,subreddit,i,limit=50)
            if not comms.empty:
                logging.info(f'Retrieved new comments {subreddit}:{len(comms)}')
                hate = [get_score(comm) for comm in comms['body']]
                comms['hate_speech'] = hate
                db.insert_posts_dataframe(comms,table_name='comments')
            query_count +=1 
        except Exception as e :
            print("Error getting comments",e)
            time.sleep(60)
    
   
def get_posts(subreddit: str):
    """
    Fetch new posts (Main driver function) and saves to database 
    Args:
            subreddit (str): required subreddit to mine eg. 'Python' 

        Returns:
            None
    """
    global df, post_ids
    client = Client()
    randomizer = 0
    data_collection_stats = DataCollectionStats()  # Initialize DataCollectionStats
    try:
        # Poll for new posts and update the DataFrame
        df = client.poll(df, subreddit)

        # Get new post IDs
        new_post_ids = set(df['id'])

        # Identify and remove duplicate post IDs
        unique_new_post_ids = new_post_ids.difference(post_ids)

        logging.info(f'New posts in subreddit {subreddit}: {len(unique_new_post_ids)}')
    
        # Check if there are new posts to process
        if unique_new_post_ids:
            # Update the post_ids set with the new post IDs
            post_ids.update(unique_new_post_ids)
            new_posts = df[~df['id'].isin(unique_new_post_ids)]

            # Save the updated DataFrame to a CSV file
            # client.dump_to_csv(df, file_path="temp.csv", append=False)
            db.insert_posts_dataframe(new_posts)

            # Collect and analyze data
            #data_collection_stats.collect_and_analyze(client, subreddit, interval_minutes=5, analysis_interval=5)

    except Exception as e :
        logging.error(f"Trouble parse posts due to : {e}" )
        randomizer = random.randint(0,60)
        
    run_at = datetime.utcnow() + timedelta(minutes=(5+randomizer))
    run_at = run_at.isoformat()[:-7] + "Z"
    run_atc = datetime.utcnow() + timedelta(minutes=(60 + randomizer))
    run_atc = run_atc.isoformat()[:-7] + "Z"
    with faktory.connection() as client:
        client.queue("get_posts", args=(subreddit,), queue="get_posts", at=run_at)
        if unique_new_post_ids:
            client.queue("get_new_comments",args=(subreddit,list(unique_new_post_ids)),queue="get_new_comments",at = run_atc)
            


if __name__ == "__main__":
    # Main worker registration 
    w = Worker(queues=["get_posts","get_new_comments"])
    w.register("get_posts",get_posts)
    w.register("get_new_comments",get_new_comments)
    w.run()
    
