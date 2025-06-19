from openai import OpenAI
import os
import requests
from dotenv import load_dotenv
import asyncio
load_dotenv()

base_url = os.getenv('url')
api_key = os.getenv('OPENAI_API_KEY')

class LLMAgent: 

    def __init__(self, 
                 model_name,
                 base_url = os.getenv('url'),
                 api_key=None):

        if api_key is None: 
            api_key = os.getenv('OPENAI_API_KEY')
        
        self.model_name = model_name
        self.base_url = base_url
        self.api_key = api_key

        #creating cliient 

        self.client = OpenAI(
            base_url= self.base_url,
            api_key= self.api_key,

        )



    def unload_and_load_model(self, model_name = None): 

        if model_name is None: 
            model_name = self.model_name

        # Check if the current model is the correct one
        url = f"{self.base_url}/model"
        headers = {
            "Authorization": f"Bearer {self.api_key}",  
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        current_model = response.json()
        print(current_model)

        if current_model == model_name:
            return 0
        else:
            # Unload the existing model
            url = f"{base_url}/model/unload"
            requests.post(url, headers=headers)

            # Load the model we are using
            url = f"{base_url}/model/load"
            requests.post(url, headers=headers)

            return 0
        

    
    def one_turn(self, 
                system_prompt, 
                user_prompt,
                temperature=0.7,
                stop=None
                ):
    # Create a chat completion
        if not stop:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=temperature,
            )
        else:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=temperature,
                stop=stop,
            )
        return response.choices[0].message.content
    
    
    def batch_one_turn(self,
                    system_prompt,
                    user_prompts,
                    temperature=0.7,
                    stop=None):
        """
        Synchronous batch: calls one_turn sequentially for each prompt.
        """
        results = []
        for prompt in user_prompts:
            res = self.one_turn(
                system_prompt=system_prompt,
                user_prompt=prompt,
                temperature=temperature,
                stop=stop
            )
            results.append(res)
        return results
    
    async def one_turn_async(self,
                            system_prompt,
                            user_prompt,
                            temperature=0.7,
                            stop=None):
        """
        Async wrapper for one_turn using a thread executor.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.one_turn,
            system_prompt,
            user_prompt,
            temperature,
            stop
        )

    async def batch_one_turn_async(self,
                                    system_prompt,
                                    user_prompts,
                                    temperature=0.7,
                                    stop=None):
        """
        True async batch: schedules one_turn_async calls concurrently.
        """
        tasks = [
            asyncio.create_task(
                self.one_turn_async(system_prompt, prompt, temperature, stop)
            )
            for prompt in user_prompts
        ]
        return await asyncio.gather(*tasks)