import logging
import time
import faktory
import reddit_crawler
import reddit
from datetime import datetime, timedelta
from faktory import Worker

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
# def crawler():
#     client = Client()
#     #get_posts= client.get_reddit_posts(subreddit=subreddit)

    
#     #temp = 
#     #logging.info(f'posts right now : {get_posts['id']}')
#     crawl_list =['politics','AmericanPolitics','political','Conservatives','moderatepolitics']
#     for sub in crawl_list:
#         with faktory.connection() as client :#"tcp://:testpassword@localhost:7419") as client:
#             client.queue("get_posts",args=(sub),queue='get_posts',reserve_for=50)
#         get_posts(sub)
with faktory.connection() as client:
    run_at = datetime.utcnow() + timedelta(minutes=5)
    run_at = run_at.isoformat()[:-7] + "Z"
    logging.info(f'run_at: {run_at}')
    crawl_list =['politics','AmericanPolitics','political','Conservatives','moderatepolitics','Ask_Politics','PoliticalDiscussion','anime_titties','PoliticsPeopleTwitter']
    client.queue("crawl-catalog", args=('pol',), queue="crawl-catalogs", reserve_for=60, at=run_at)
   
        #time.sleep(5*60)
    for sub in crawl_list:
        client.queue("get_posts",args=(sub,),queue='get_posts',reserve_for=50)
        #client.queue("crawl-catalog", args=('pol',), queue="crawl-catalogs", reserve_for=60, at=run_at)
        # time.sleep(300)