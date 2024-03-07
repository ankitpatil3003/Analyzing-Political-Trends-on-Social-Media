import faktory
import logging
from faktory import Worker
from chan_client import Client
from datetime import datetime, timedelta
import pandas as pd 
from utils.reddit_db import reddit_DB
import psycopg2

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
db= reddit_DB()
thread_numbers = set()
main_df = pd.DataFrame()
def extract_thread_data(board,thread):
    """
    Extract data from a given thread and save it to the database
    Args:
        board (str) : board name 
        thread (int) : thread number
    Returns :
        None 
    """
    thread_data_df = pd.DataFrame(columns=["board", "thread_number", "post_number", "comments"])
    board_name = board#thread["board"]
    thread_number = thread["no"]
    post_number = thread["replies"]
    comments = thread["com"]
    params = db.conn_params
    conn =None 
    try :
        thread_number = thread["no"]
        post_number = thread["replies"]
        comments = thread["com"]
        params = db.conn_params
        thread_data_df.loc[len(thread_data_df)] = [board_name, thread_number, post_number, comments]
        if main_df.empty:
            main_df = thread_data_df

        else:
            thread_data_df = thread_data_df[~thread_data_df['no'].isin(main_df['no'])]
            if not thread_data_df.empty:
                main_df  = pd.concat([main_df,thread_data_df],ignore_index = True)
        
        new_thread = set(main_df['no'])
        unique_new_thread_no = new_thread.difference(thread_numbers)

        if unique_new_thread_no:
            thread_numbers.update(unique_new_thread_no)
            db.insert_posts_dataframe(thread_data_df,table_name= 'chan_tb')

        logging.info(f"Data for /{board_name}/{thread_number} inserted into the database.")
    except (Exception) as error :#, psycopg2.DatabaseError) as error:
        logging.error(f"Error: {error}")
    # finally:
    #     if conn is not None:
    #         conn.close()
    

def thread_numbers_from_catalog(board,catalog):
    """ Get thread numbers from catalog 
        Args:
            board (str) : board name 
            catalog (dict) : catalog dictionary
        Returns:
            thread numbers
    """
    global thread_numbers

    for page in catalog:
        page_number = page["page"]
        #print(page_number)
        # now let's get thread numbers
        for thread in page["threads"]:
            thread_number = thread["no"]
            #print(f'Thread nubmber: {thread_number}')
            #extract_thread_data(board,thread)
            thread_numbers.add(thread_number)

    return thread_numbers

def find_dead_threads(old_thread_numbers, new_thread_numbers):
    """ Find dead threads"""
    dead_threads = old_thread_numbers.difference(new_thread_numbers)
    return dead_threads

def crawl_thread(board, thread_number):
    client = Client()
    thread = client.get_thread(board, thread_number)
    extract_thread_data(thread)
    #logging.info(f'/{board}/{thread_number}: {thread}')

def crawl_catalog(board, old_thread_numbers=[]):
    # make a new 4chan client
    client = Client()
    catalog = client.get_catalog(board)
    # we have a current catalog
    # we need to figure out which threads died so we can collect them
    new_thread_numbers = thread_numbers_from_catalog(board,catalog)
    #logging.info(f'catalog thread numbers: {new_thread_numbers}')

    # now we need to figure out which threads have died, and issue a crawl
    # job for them
    dead_thread_numbers = find_dead_threads(set(old_thread_numbers), new_thread_numbers)

    for dead_thread_number in dead_thread_numbers:
        # enqueue a new job to collect the dead thread number
        with faktory.connection() as client:
            client.queue("crawl-thread", args=(board, dead_thread_number), queue="crawl-threads", reserve_for=60)

    # now we need to schedule to crawl the catalog again in 5 minutes
    run_at = datetime.utcnow() + timedelta(minutes=5)
    run_at = run_at.isoformat()[:-7] + "Z"
    #logging.info(f'scheduling a new catalog crawl to run at: {run_at}')
    
    with faktory.connection() as client:
        client.queue("crawl-catalog", args=(board,list(new_thread_numbers)), queue="crawl-catalogs", reserve_for=60, at=run_at)




if __name__ == "__main__":
    w = Worker(queues=["crawl-catalogs", "crawl-threads"])
    w.register("crawl-catalog", crawl_catalog)
    w.register("crawl-thread", crawl_thread)
    w.run()