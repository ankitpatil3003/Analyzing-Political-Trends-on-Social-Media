import pandas as pd 
import psycopg2
import logging
from sqlalchemy import create_engine, MetaData

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
class reddit_DB:
    def __init__(self)->None:
        self.conn_params={
            "database": "politics_db",
            "user": "pmarath1",
            "password": '1234',
            "host": "localhost",
            "port": "5432", 
        }
        self.conn = self.connect_db()

    def connect_db(self):
        """
        Connect to the database and return a connection object if successful.
        If not successful, print an error message and exit program.
        """
        try:
            
            temp =  create_engine("postgresql+psycopg2://pmarath1:1234@localhost:5432/politics_db")#psycopg2.connect(**self.conn_params)
            return temp
        except Exception as e :
            logging.error(f'Error connecting to Postgre:{e}')
            raise e

    def insert_posts_dataframe(self, dataframe, table_name='reddit_tb'):
        """ Inserts Dataframe into specified table in database
        Args:
            dataframe (pandas.Dataframe): Dataframe to be inserted into database. Follows specific database column rules. Make sure to verify 
            table_name (str) : Table name to insert data into . Default : 'reddit_tb'

        Returns:
           None
         """
        try:
            with self.conn.begin() as c:
                dataframe.to_sql(table_name, c,schema='public', if_exists='append', index=False)
        except Exception as e:
            logging.error(f'Error inserting data: {e}')
            raise e 

    def get_posts_dataframe(self,subreddit = None,table_name='reddit_tb'):
        """ Fetches posts from a specific subreddit and returns it as pandas dataframe
        Args:
            subreddit (str) : Subreddit name. Default : None 
            table_name (str) : table to query from . Default : 'reddit_tb'
        Returns :
            pandas.DataFrame : Dataframe containing requied data 
        """
        try:
            with self.conn.begin() as c:
                df = pd.DataFrame()
                if subreddit is None :
                    query = f"select * from {table_name}"
                    df = pd.read_sql(query,c)
                else : 
                    query = f"select * from {table_name} where subreddit = '{subreddit}'"
                    df = pd.read_sql(query,c)
                return df
        except Exception as e :
            print(e)
        
    def run_query(self,query:str):
        """ Runs specific query 
            Args :
                query (str) : Specific query to be executed. eg. select * from reddit_tb;
        """
        try:
            if len(query)<1:
                raise "Empty query please try again"
            with self.conn.begin() as c :
                df = pd.DataFrame()
                df = pd.read_sql(query,c)
                return df
        except Exception as e :
            logging.error(e)
            raise e
                
    def close_connection(self):
        self.conn.close()

