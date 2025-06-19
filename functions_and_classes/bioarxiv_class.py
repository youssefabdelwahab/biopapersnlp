import urllib3
import json
import logging


class bioarxiv_api: 

    def __init__(self):
        self.http = urllib3.PoolManager()

    def request_papers(self , call_type:str , year_range:str , cursor:str):
        endpoint = f"https://api.biorxiv.org/pubs/biorxiv/{year_range}"
        if cursor: 
            endpoint += f"/{cursor}"
        response = self.http.request(call_type ,endpoint)
        decoded_response = response.data.decode("utf-8")
        return json.loads(decoded_response)
    
    def get_all_papers(self , year_range:str , limit: int): 

        all_metadata = []
        if not limit:
            raise ValueError("You must specify a limit when simulating cursor manually.")

        pages = (limit + 99) //100
        
        for i in range(pages):
            cursor = str(i*100)

            data = self.request_papers("GET" , year_range , cursor)
            batch = data.get('collection' , [])

            if not batch:
                logging.info(f"Successfully extracted {len(all_metadata)} papers (limit={limit}).") 
                break 

            all_metadata.extend(batch)

            if len(all_metadata) >= limit:
                break

        return all_metadata
    
    def request_specific_preprint(self,preprint:bool, doi:str):
        if preprint: 
            endpoint = f"https://api.biorxiv.org/details/biorxiv/{doi}"
        else: 
            endpoint = f"https://api.biorxiv.org/pubs/biorxiv/{doi}"
        response = self.http.request("GET", endpoint)
        decoded_response = response.data.decode("utf-8")
        return json.loads(decoded_response)