import sys
import os
from dotenv import load_dotenv
import asyncio
import requests

cwd = os.path.dirname(os.path.abspath(__file__))
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
if parent_folder not in sys.path:
    sys.path.append(parent_folder)


from functions_and_classes import bioarxiv_reader
from modules.pdf_resolver import PDFResolver
from LLM_Agent import inline_llm_template
load_dotenv()
                
biorxiv_api = bioarxiv_reader.bioarxiv_api()

async def retry_biorxiv(doi: str, preprint: bool):
    loop = asyncio.get_running_loop()
    for attempt in range(3):
        try:
            return await loop.run_in_executor(None, biorxiv_api.request_specific_preprint, preprint, doi)
        except requests.RequestException:
            await asyncio.sleep(1 * (2 ** attempt))
    return None

    
async def extract_text_with_pdf_resolver(doi: str, paper_id:str, selector_timeout:int) -> str: 
    """
        Resolve *doi* → PDF → text using the new async PDFResolver.

    • Tries all resolver logic (Springer, OUP, Wiley, Playwright fallback…)
    • Downloads the PDF URL it finds
    • Extracts text with your existing `pdf_parser.extract_text_from_pdf_url`
    • Returns *None* if nothing could be extracted
    """
    async with PDFResolver(selector_timeout= selector_timeout) as resolver: 
        try:
            pdf = await resolver.get_pdf(doi=doi, paper_id=paper_id)
            return pdf
        except resolver.CantDownload as exc: 
            #print 
            return {"url":exc.landing}



