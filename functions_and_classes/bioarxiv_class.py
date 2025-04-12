import urllib3
import json 


class bioarxiv_api: 

    def __init__(self):
        self.http = urllib3.PoolManager()

    def request_papers(self , call_type:str , year_range:str):
        endpoint = f"https://api.biorxiv.org/pubs/biorxiv/{year_range}"
        response = self.http.request(call_type ,endpoint)
        decoded_response = response.data.decode("utf-8")
        parsed_response = json.loads(decoded_response)
        metadata = parsed_response['collection']
        return metadata
    
