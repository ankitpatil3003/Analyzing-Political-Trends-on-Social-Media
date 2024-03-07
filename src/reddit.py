import requests
from utils.reddit_db import reddit_DB
import time
import requests.auth
import random
import pandas as pd
from utils.credentials import CLIENT_ID,CLIENT_SECRET,USERNAME,PASSWORD
import logging
from datetime import datetime
import os

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
db = reddit_DB()
class Client:
    """Client class for reddit api 
    """
    def __init__(self) -> None:
        OAUTH_ENDPOINT = 'https://oauth.reddit.com'
        

    def get_auth(self):
        "'Authentication function for generating token'"
        client_auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
        post_data = {'grant_type': 'password', 'username': USERNAME, 'password': PASSWORD}
        headers={
        'User-Agent':'testApp vs 0.01'
        }
        # Get an access token
        TOKEN_ACCESS_ENDPOINT = 'https://www.reddit.com/api/v1/access_token'
        response = requests.post(TOKEN_ACCESS_ENDPOINT, data=post_data, headers=headers, auth=client_auth)
        
        if response.status_code != 200:
            print("Failed to obtain access token.")
            return None
        token_id = response.json()['access_token']
        #get_param = {'limit': limit}
        headers_get={
        'User-Agent': 'testApp vs 0.01',
        'Authorization': 'Bearer ' + token_id
        }
        return headers_get

    def get_reddit_posts(self, subreddit, before_key=None,headers_get=None,limit=100):
        """Retrive new posts for a specific subreddit

        Args:
            subreddit (str): required subreddit to mine eg. 'Python' 
            headers_get (dict, optional): headers containing token for oauth api use. Defaults to None.
            limit (int, optional):Number of post to retrive. Defaults to 100.

        Returns:
            pandas.Dataframe: returns dataframe containing post from given subreddit
        """
        # Get Reddit posts
        get_param = {'limit':limit}

        if before_key:
            get_param['before'] = before_key
        
        if headers_get is not None:
            response2 = requests.get(OAUTH_ENDPOINT + f'/r/{subreddit}/new/', headers=headers_get, params=get_param)
            data = response2.json()
        
        else :
            headers_get = {'User-Agent': 'testApp vs 0.01'}
            url = 'https://www.reddit.com/r/'+subreddit+'/new.json'
            response2= requests.get(url,headers=headers_get,params=get_param)
            if response2.status_code!= 200:
                return None
            data=response2.json()
        posts = data.get('data', {}).get('children', [])

        # Create a DataFrame from the posts
        post_data = []
        for post in posts:
            post_data.append({
                'id': post['data']['id'],
                'subreddit': post['data']['subreddit'],
                'title': post['data']['title'],
                'selftext': post['data']['selftext'],
                'upvote_ratio': post['data']['upvote_ratio'],
                'ups': post['data']['ups'],
                'downs': post['data']['downs'],
                'score': post['data']['score'],
                'created': post['data']['created_utc'],
            })

        df = pd.DataFrame(post_data)
        return df
        
    def get_comments(self,subreddit:str,post_id:str,headers_get=None,limit=50):
        """Function to retrive comments for a specific post

        Args:
            subreddit (str): subreddit whose post you want to retrieve 
            post_id (str): specific id whose comments are to be retried 
            headers_get (_type_, optional): headers for oauth api use. Defaults to None.
            limit (int, optional): Number of comments to retrieve . Defaults to 50.

        Returns:
            pandas.Dataframe: Dataframe containing comments for specified post
        """
        get_param={'limit':limit}
        if headers_get is not None:
            response2 = requests.get(OAUTH_ENDPOINT + f'/r/{subreddit}'+'/comments/'+post_id+'.json', headers=headers_get, params=get_param)      
        else :
            headers_get = {'User-Agent': 'testApp vs 0.01'}
            url = 'https://www.reddit.com/r/'+subreddit+'/comments/'+post_id+'.json?sort=confidence'
            response2= requests.get(url,headers=headers_get,params=get_param)
            if response2.status_code!= 200:
                logging.error(f'error! failed to retrieve comments cuz of code : {response2.status_code}')
                return None
        data = response2.json()
        #print(data)
        comment_data=[]
        if 'data' in data[1] and 'children' in data[1]['data']:
            comments = data[1]['data']['children']
            for comment in comments:
                if 'body' not in comment['data']:
                    comment['data']['body'] = ''
                try:
                    if 'permalink' not in comment['data']:
                        comment['data']['permalink']=' '
                    comment_data.append({
                        'comment_id':comment['data']['id'],
                        'post_id':post_id,
                        'subreddit':subreddit,
                        'name':comment['data']['name'],
                        'body':comment['data']['body'],
                        'url': 'https://www.reddit.com'+comment['data']['permalink'],
                    })
                except Exception as e:
                    print(e)   
                    pass
        df=pd.DataFrame(comment_data)
        return df

    def get_old_posts(self, subreddit, before_key=None, after_key= None,limit=100):
        """Retrieve older posts from a specific subreddit.

        Args:
            subreddit (str): The subreddit name.
            before_key (str): The Reddit 'before' key to fetch posts older than a specific post.
            limit (int): Number of posts to retrieve.

        Returns:
            pandas.DataFrame: DataFrame containing old posts from the subreddit.
        """
        get_params = {'limit': limit}#limit}
        # if before_key:
        #     get_params['before'] = before_key
        # if after_key:
        #     get_params['after'] = after_key
        if after_key is None: 
            after_key =''

        headers_get = {'User-Agent': 'testApp vs 0.01'}
        url = 'https://www.reddit.com/r/'+subreddit+'/new.json' + '?' +'sort=all&show=all&restrict_sr=on&t=month&after='+after_key
        #url = 'https://www.reddit.com/r/'+ subreddit+'top.json?sort=top&show=all&t=year&after={}'.format(after_key)
        response = requests.get(url, headers=headers_get, params=get_params)

        if response.status_code != 200:
            raise ValueError(f"Failed to retrieve old posts. Status code: {response.status_code}")

        data = response.json()
        #print(data)
        posts = data.get('data', {}).get('children', [])

        # Create a DataFrame from the posts
        post_data = []
        for post in posts:
            post_data.append({
                'id': post['data']['id'],
                'subreddit': post['data']['subreddit'],
                'title': post['data']['title'],
                'selftext': post['data']['selftext'],
                'upvote_ratio': post['data']['upvote_ratio'],
                'ups': post['data']['ups'],
                'downs': post['data']['downs'],
                'score': post['data']['score'],
                'created': post['data']['created_utc'],
            })
        
        # Extract 'after' key for pagination in get_posts_by_timestamp
        after_key = data.get('data', {}).get('after')
        #print(len(post_data))
        df = pd.DataFrame(post_data)
        return df, after_key

    def get_posts_by_timestamp(self, subreddit, start_timestamp, end_timestamp, limit=100):
        """ retrive all posts in a particular time range ( limited to 1000 post by rediit )
            Args:
                subreddit (str) : subreddit to be mined
                start_timestamp (str) : unix timestamp of starting date and time 
                end_timestamp (str) : unix timestamp of ending date and time
                limit (int) : number of posts to fetch per request Default : 100 (max)
            
            Returns:
                pandas.DataFrame : dataframe contatining posts 
        """
        posts = pd.DataFrame()
        after_key = None
        query_count = 0
        start_time = time.time()
        uni = set()
        while True:
            elapsed_time = time.time() - start_time

            # Check if query limit is exceeded
            if elapsed_time < 60 and query_count > 9:
                # Wait for the next minute before making more queries
                time.sleep(60 - elapsed_time)
                query_count = 0
                start_time = time.time()
                break

            # Get new posts
            try:
                new_posts, after_key = self.get_old_posts(subreddit, after_key=after_key, limit=limit)
                #print(new_posts)
                if new_posts.empty and after_key == None:
                    print(f"No new posts retrieved. Exiting.")
                    break
                #filtered_posts = new_posts.query('created'> float(start_timestamp))
                print(datetime.utcfromtimestamp(new_posts['created'].iloc[-1]).strftime('%Y-%m-%d %H:%M:%S'))
                if new_posts['created'].iloc[-1] > float(start_timestamp):
                    filtered_posts = new_posts[(new_posts['created'] >= float(start_timestamp)) & (new_posts['created'] <= float(end_timestamp))]
                    #print(filtered_posts)
                    if posts.empty:
                        posts = pd.concat([posts,filtered_posts], ignore_index=True)
                        new_post_id = set(posts['id'])
                        unique_new_post_ids = new_post_id.difference(uni)
                        if unique_new_post_ids:
                            uni.update(unique_new_post_ids)
                            temp = posts[~posts['id'].isin(unique_new_post_ids)]
                            #db.insert_posts_dataframe(temp)
                            
                    else:
                        #filtered_posts = filtered_posts[~filtered_posts['id'].isin(posts['id'])]
                        posts = pd.concat([posts,filtered_posts], ignore_index=True)
                        new_post_id = set(posts['id'])
                        unique_new_post_ids = new_post_id.difference(uni)
                        if unique_new_post_ids:
                            uni.update(unique_new_post_ids)
                            temp = posts[~posts['id'].isin(unique_new_post_ids)]
                            #db.insert_posts_dataframe(temp)
                    query_count += 1
                    continue
                else :
                
                    # Filter posts based on timestamp range
                    #filtered_posts = new_posts[(new_posts['created'] >= float(start_timestamp)) & (new_posts['created'] <= float(end_timestamp))]#[new_posts['created'] >= float(start_timestamp))]
                    if posts.empty:
                        posts = pd.concat([posts,new_posts],ignore_index=True)#filtered_posts], ignore_index=True)
                        new_post_id = set(posts['id'])
                        unique_new_post_ids = new_post_id.difference(uni)
                        if unique_new_post_ids:
                            uni.update(unique_new_post_ids)
                            temp = posts[~posts['id'].isin(unique_new_post_ids)]
                            #db.insert_posts_dataframe(temp)
                    else:
                        new_posts = new_posts[~new_posts['id'].isin(posts['id'])]
                        posts = pd.concat([posts,new_posts],ignore_index=True)#filtered_posts], ignore_index=True)
                        new_post_id = set(posts['id'])
                        unique_new_post_ids = new_post_id.difference(uni)
                        if unique_new_post_ids:
                            uni.update(unique_new_post_ids)
                            temp = posts[~posts['id'].isin(unique_new_post_ids)]
                            #db.insert_posts_dataframe(temp)
                    #print(posts)
                # print(new_posts)
                    if new_posts['created'].iloc[-1] < float(end_timestamp):
                        break
                    
                    if filtered_posts.empty:
                        print(f"No posts in the specified timestamp range. Exiting.")
                        break

                    
                    #before_key = after_key
                    print(posts)
                    query_count += 1
            except Exception as e:
                print(f"Exception occurred while getting old posts {e}")
                if not posts.empty:
                    new_post_id = set(posts['id'])
                    unique_new_post_ids = new_post_id.difference(uni)
                    if unique_new_post_ids:
                        uni.update(unique_new_post_ids)
                        temp = posts[~posts['id'].isin(unique_new_post_ids)]
                        #db.insert_posts_dataframe(temp)
                time.sleep(60)
                pass 
            

        # Combine filtered posts into a single DataFrame
        if  posts.empty:
            print("No posts to concatenate. Returning None.")
            return None
        else:
            new_post_id = set(posts['id'])
            unique_new_post_ids = new_post_id.difference(uni)
            if unique_new_post_ids:
                uni.update(unique_new_post_ids)
                temp = posts[~posts['id'].isin(unique_new_post_ids)]
               # db.insert_posts_dataframe(temp,table_name='reddit_tb')
            return posts
            

    def poll(self,df,subreddit_name, limit=100):
        # Get new posts from the specified subreddit
        """ Get new posts from specified subreddit 
            Args:
                df (pandas.DataFrame) : dataframe containing posts
                subreddit_name (str) : name of the subreddit
                limit (int) : number of posts to fetch per request
            Returns :
            pandas.DataFrame : updated dataframe with newly fetched posts"""

        new_posts = self.get_reddit_posts(subreddit_name)

        if new_posts is None:
            print("Failed to retrieve new posts.")
            return None

        # Check if the DataFrame is empty
        if df.empty:
            # If the DataFrame is empty, append all new posts to it
            df = new_posts
        else:
            # Identify the new posts that are not already in the DataFrame
            new_posts = new_posts[~new_posts['id'].isin(df['id'])]

            if not new_posts.empty:
                # Append the new posts to the DataFrame
                df = pd.concat([df, new_posts], ignore_index=True)

        return df

    def poll_comments(self,comm,subreddit,post_id,limit=100):
        """ Get comments for new posts
            Args:
                comm (pandas.DataFrame) : Dataframe for comments
                subreddit (str) : Name of the subreddit
                post_id (str) : ID of the post
                limit (int) : Number of comments to fetch per request
            
            Returns :
                pandas.DataFrame : updated DataFrame with comments
        """
        try : 
            new_comments = self.get_comments(subreddit,post_id)
            if comm is None or new_comments is None:
                return comm 
            if comm.empty:
                comm = new_comments
            else:
                new_comments = new_comments[~new_comments['comment_id'].isin(comm['comment_id'])]
                if not new_comments.empty:
                    comm = pd.concat([comm, new_comments], ignore_index=True)
        except Exception as e :
            logging.error(f'Error due to :{e}')
        return comm 
        

