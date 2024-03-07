import time
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

class DataCollectionStats:
    def __init__(self):
        self.collection_history = []
        self.storage_requirements = []

    def collect_and_analyze(self, client, subreddit_name, interval_minutes, analysis_interval):
        # Collect data for the specified time intervals
        start_time = datetime.now()
        end_time = start_time + pd.Timedelta(minutes=analysis_interval)
        while datetime.now() < end_time:
            df = pd.DataFrame()
            data_collection_start_time = datetime.now()
            client.poll(df,subreddit_name)
            data_collection_end_time = datetime.now()
            data_collection_duration = (data_collection_end_time - data_collection_start_time).total_seconds()

            # Append the data collection time to the history
            self.collection_history.append(data_collection_duration)

            time.sleep(interval_minutes * 60)  # Sleep for the specified interval

        # Calculate basic data collection statistics
        avg_collection_time = sum(self.collection_history) / len(self.collection_history)
        max_collection_time = max(self.collection_history)
        min_collection_time = min(self.collection_history)

        # Generate and display basic data analysis
        print(f"Average Collection Time: {avg_collection_time} seconds")
        print(f"Maximum Collection Time: {max_collection_time} seconds")
        print(f"Minimum Collection Time: {min_collection_time} seconds")

        # Generate a time series plot of data collection times
        self.plot_time_series()

        # Estimate data storage requirements
        self.estimate_storage_requirements(subreddit_name, analysis_interval)

    def plot_time_series(self):
        plt.figure(figsize=(10, 6))
        plt.plot(self.collection_history)
        plt.title("Data Collection Time Series")
        plt.xlabel("Collection Iteration")
        plt.ylabel("Collection Time (seconds)")
        plt.show()
        plt.savefig('stat.jpg')

    def estimate_storage_requirements(self, subreddit_name, analysis_interval):
        # Estimate data storage requirements based on analysis_interval
        df = pd.DataFrame({'Collection Time': self.collection_history})
        avg_collection_time = df['Collection Time'].mean()
        posts_per_second = 1 / avg_collection_time
        total_posts = posts_per_second * analysis_interval
        storage_requirements = total_posts * 100  # Assuming 100 bytes per post
        self.storage_requirements.append(storage_requirements)
        print(f"Estimated Storage Requirements for {subreddit_name}: {storage_requirements} bytes")

if __name__ == "__main__":
    subreddit_name = 'Python'  # Change to the desired subreddit
    data_collection_stats = DataCollectionStats()

    client = Client()
    # Specify the data collection interval and analysis interval (in minutes)
    data_collection_stats.collect_and_analyze(client, subreddit_name, interval_minutes=5, analysis_interval=30)
