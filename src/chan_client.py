import requests
class Client:
    BASE_API_URL = "https://a.4cdn.org"
    
    def get_threads(self, board):
        # https://a.4cdn.org/pol/threads.json
        # create the end point url by joining parameters, etc.
        # endpoint_url = '/'.join([base_api_url, board, 'threads.json'])
        endpoint_url = self.build_request([board, 'threads.json'])
        # could also do something like: f'{base_api_url}/{board}/threads.json'
    
        # make our request
        return self.execute(endpoint_url)

    def get_boards(self):
        # https://a.4cdn.org/boards.json
        endpoint_url = self.build_request(["boards.json"])

        return self.execute(endpoint_url)
    
    def get_catalog(self, board):
        # https://a.4cdn.org/BOARD/catalog.json
        endpoint_url = self.build_request([board, 'catalog.json'])
    
        return self.execute(endpoint_url)

    def get_thread(self, board, thread_number):
        # https://boards.4channel.org/v/thread/648833274
        # https://boards.4channel.org/BOARD/thread/THREAD_NUMBER.json
        endpoint_url = self.build_request([board, "thread", f'{thread_number}.json'])

        return self.execute(endpoint_url)
    
    def execute(self, endpoint_url):
        return requests.get(endpoint_url).json()

    def build_request(self, endpoint_pieces):
        # put the base_api_url and any parameters into a single list
        endpoint_url = [self.BASE_API_URL] + endpoint_pieces
        endpoint_url = '/'.join(endpoint_url)
    
        print(endpoint_url)  
        return endpoint_url 