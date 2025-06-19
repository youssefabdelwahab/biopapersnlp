import sys
import os
import json 
from dotenv import load_dotenv
import asyncio
import pdfplumber
import aiofiles
import requests




cwd = os.getcwd()
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
if parent_folder not in sys.path:
    sys.path.append(parent_folder)


from functions_and_classes import  functions
from functions_and_classes import bioarxiv_class
from functions_and_classes.paper_to_doi import get_article_info_from_title
from LLM_Agent.llm_template import LLMAgent
load_dotenv()

repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
extract_save_folder = os.path.join(repo_root, "papers", "extracted_papers")
unextracted_save_folder = os.path.join(repo_root, "papers", "unextracted_papers")
unknown_save_folder = os.path.join(repo_root, "papers", "unknown_papers")

for folder in (extract_save_folder, unextracted_save_folder, unknown_save_folder):
    os.makedirs(folder, exist_ok=True)

extract_file_name = os.path.join(extract_save_folder, "extracted_papers.jsonl")
unextracted_file_name = os.path.join(unextracted_save_folder, "unextracted_papers.jsonl")
unknown_file_name = os.path.join(unknown_save_folder, "unknown_papers.jsonl")

max_llm_concurrency = int(os.getenv("max_llm_concurrency", 8))
s3_preprint_path = os.getenv("s3_preprint_path")
cleaning_prompt = """
 The following text is a *partial excerpt* from a research paper. Your task is to:

                        - Clean it up
                        - Keep only the **main body content**
                        - Remove footnotes, references, citations, figure captions, and legal disclaimers.
                        - Ensure proper paragraph structure and readability.
                        - Preserve the logical order within this chunk only.
                        - Do not include any commentary, analysis, or information not present in the chunk.
                        - Do not attempt to infer or hallucinate missing parts from previous or next sections.
                        - Do not include any commentary 

                        This is only one chunk of a longer paper. Treat each chunk independently unless otherwise told.
                        Return ONLY the cleaned and readable main body text from the input. DO NOT add any commentary, introduction, summary, or instructional text.
"""
title_prompt = """
        

            You will receive a partial excerpt of a scientific paper.

            Your task is to extract and return only the title of the paper, using the following rules:

            - If the text contains a line that starts with 'Title:', return only the text that follows it on that line.
            - If 'Title:' is not found, look for the very first non-empty line that appears before a line starting with
            'Authors' or 'Affiliations'. That is likely the title. Return that line as the title.
            - Do not reply with anything else except the title.
            - If no such title can be found, return: Title not found

            Return only the exact title as a plain string. Do not add any labels, formatting, or explanation.

            """
biorxiv_api = bioarxiv_class.bioarxiv_api()
api_key = os.getenv("API_KEY")
model_name = 'Llama-3-8B-Instruct-exl2'
agent = LLMAgent(model_name)

llm_semaphore = asyncio.Semaphore(max_llm_concurrency)

async def retry_biorxiv(doi: str, preprint: bool):
    loop = asyncio.get_running_loop()
    for attempt in range(3):
        try:
            return await loop.run_in_executor(None, biorxiv_api.request_specific_preprint, preprint, doi)
        except requests.RequestException:
            await asyncio.sleep(1 * (2 ** attempt))
    return None

async def call_llm(system_prompt: str, user_prompt: str) -> str:
    loop = asyncio.get_running_loop()
    async with llm_semaphore:
        return await loop.run_in_executor(None, agent.one_turn, system_prompt, user_prompt)
    
async def process_pdf(path: str):
    paper_chunks = []
    loop = asyncio.get_running_loop()
    try:
        with pdfplumber.open(path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                try:
                    page_text = page.extract_text() or ""
                except Exception:
                    continue
                if not page_text.strip():
                    continue
                page_chunks = functions.chunk_text_by_char_limit(page_text, limit=5000)
                page_chunks_cleaned = await asyncio.gather(*(call_llm(cleaning_prompt, c) for c in page_chunks))
                paper_chunks.append(" ".join(page_chunks_cleaned))
    except Exception:
        return []
    return paper_chunks
    
    
async def clean_chunk_text(loop, chunk: str) -> str:
    """Clean a single text chunk through the LLM with semaphore throttling."""
    async with llm_semaphore:
        return await loop.run_in_executor(None, agent.one_turn, cleaning_prompt, chunk)

async def get_title(loop, text: str) -> str:
    async with llm_semaphore:
        return await loop.run_in_executor(None, agent.one_turn, title_prompt, text)

counter = 0
counter_lock = asyncio.Lock()
async def extract_preprint_and_published_papers(extracted_q, unextracted_q, unknown_q):
    global counter
    loop = asyncio.get_event_loop()

    for paper in os.listdir(s3_preprint_path):
        if paper.endswith(".pdf"):
            paper_path = os.path.join(s3_preprint_path, paper)
        else:
            print(f"Skipping {paper}, not a PDF file.")
            continue
        print(f"Processing paper: {paper}")
        research_text_bucket = {
            "preprint_paper": None,
            "published_paper": None
                }
        paper_dict = {
            "preprint_doi": "",
            "published_doi": "",
            "published_journal": "",
            "preprint_title": "",
            "preprint_authors": "",
            "preprint_category": "",
            "preprint_date": "",
            "published_date": "",
            "preprint_author_corresponding": "",
            "preprint_author_corresponding_institution": "",
            "preprint_paper": "",
            "published_paper": "",
        }
        
        
        preprint_chunks = await process_pdf(paper_path) 
        if not preprint_chunks: 
            await unknown_q.put(paper_dict); continue   
        preprint_cleaned_text = " ".join(preprint_chunks)
        research_text_bucket.update({
            "preprint_paper": preprint_cleaned_text
        })
        intro_paragraph = str(preprint_chunks[0])
        
        paper_title  = await call_llm(title_prompt, intro_paragraph)

        if paper_title == "Title not found":
            print(f"Title not found for {paper_path}")
            #extract preprint and save it to unknown_preprints folder
            paper_dict.update({
                "preprint_doi": None,
                "published_doi":None,
                "published_journal": None,
                "preprint_title": paper_title,
                "preprint_authors": None,
                "preprint_category": None,
                "preprint_date": None,
                "published_date": None,
                "preprint_author_corresponding": None,
                "preprint_author_corresponding_institution": None,
                "preprint_paper": research_text_bucket["preprint_paper"],
                "published_paper": None
            })
            await unknown_q.put(paper_dict)
            continue
        
        print(f"Paper Title Found , moving on to extraction phase ")
        preprint_info = get_article_info_from_title(paper_title)
        
        if preprint_info['doi'] is None:
            print(f"Could not find preprint doi for {paper_title}")
            #save it to unknown_preprints folder
            paper_dict.update({
                "preprint_doi": None,
                "published_doi":None,
                "published_journal": None,
                "preprint_title": paper_title,
                "preprint_authors": None,
                "preprint_category": None,
                "preprint_date": None,
                "published_date": None,
                "preprint_author_corresponding": None,
                "preprint_author_corresponding_institution": None,
                "preprint_paper": research_text_bucket["preprint_paper"],
                "published_paper": None
            })
            await unknown_q.put(paper_dict)
            continue
        
        preprint_doi = preprint_info.get("doi")
        preprint_paper_metadata = await retry_biorxiv(preprint_doi, preprint=True)
        preprint_coll = (preprint_paper_metadata or {}).get("collection", [])
        if not preprint_coll:
            paper_dict.update({
                "preprint_doi": None,
                "published_doi":None,
                "published_journal": None,
                "preprint_title": paper_title,
                "preprint_authors": None,
                "preprint_category": None,
                "preprint_date": None,
                "published_date": None,
                "preprint_author_corresponding": None,
                "preprint_author_corresponding_institution": None,
                "preprint_paper": research_text_bucket["preprint_paper"],
                "published_paper": None
            })
            await unknown_q.put(paper_dict)
            continue
        latest_preprint = preprint_coll[-1]
        published_doi = latest_preprint.get('published')
        
        if published_doi == "NA":
            print("Preprint has not been published yet, storing preprint")
            #store its metadata information and save it to unextracted_papers
            paper_dict.update({
                "preprint_doi": latest_preprint.get('doi'),
                "published_doi":None,
                "published_journal": None,
                "preprint_title": paper_title,
                "preprint_authors": latest_preprint.get('authors'),
                "preprint_category": latest_preprint.get('category'),
                "preprint_date": latest_preprint.get('date'),
                "published_date": None,
                "preprint_author_corresponding": latest_preprint.get('author_corresponding'),
                "preprint_author_corresponding_institution": latest_preprint.get('author_corresponding_institution'),
                "preprint_paper": research_text_bucket["preprint_paper"],
                "published_paper": None
            })
            await unextracted_q.put(paper_dict)
            continue        
        
        published_paper_metadata = await retry_biorxiv(published_doi, preprint=False)
        published_coll = (published_paper_metadata or {}).get("collection", [])
        if not published_coll:
            paper_dict.update({
                "preprint_doi": latest_preprint.get('doi'),
                "published_doi":None,
                "published_journal": None,
                "preprint_title": paper_title,
                "preprint_authors": latest_preprint.get('authors'),
                "preprint_category": latest_preprint.get('category'),
                "preprint_date": latest_preprint.get('date'),
                "published_date": None,
                "preprint_author_corresponding": latest_preprint.get('author_corresponding'),
                "preprint_author_corresponding_institution": latest_preprint.get('author_corresponding_institution'),
                "preprint_paper": research_text_bucket["preprint_paper"],
                "published_paper": None
            })
            await unextracted_q.put(paper_dict)
            continue
        latest_pub = published_coll[0]
        confirmed_published_doi = latest_pub.get('published_doi')
        published_link = f"https://doi.org/{confirmed_published_doi}"
        
        published_text = None 
        success = False
        try:
            published_text = await functions.extract_text_from_pdf_via_browser(published_link)
            if published_text:
                print("Successfully extracted published paper")
                published_chunks = functions.chunk_text_by_char_limit(published_text, limit=5000)
            
                # published_tasks = [clean_chunk_text(loop, chunk) for chunk in published_chunks]
                
                published_cleaned_chunks = await asyncio.gather(*(call_llm(cleaning_prompt, c) for c in published_chunks))
                published_cleaned_text = " ".join(published_cleaned_chunks)
                research_text_bucket.update({
                    "published_paper" : published_cleaned_text
                })
                paper_dict.update({
                "preprint_doi": latest_pub.get('preprint_doi'),
                "published_doi": latest_pub.get('published_doi'),
                "published_journal": latest_pub.get('published_journal'),
                "preprint_title": paper_title,
                "preprint_authors": latest_pub.get('preprint_authors'),
                "preprint_category": latest_pub.get('preprint_category'),
                "preprint_date": latest_pub.get('preprint_date'),
                "published_date": latest_pub.get('published_date'),
                "preprint_author_corresponding": latest_pub.get('preprint_author_corresponding'),
                "preprint_author_corresponding_institution": latest_pub.get('preprint_author_corresponding_institution'),
                "preprint_paper": research_text_bucket["preprint_paper"],
                "published_paper": research_text_bucket["published_paper"]
                })
            
                await extracted_q.put(paper_dict)
                async with counter_lock:
                    counter += 1
                print(f"preprint and published extracted for {paper_title}")

                if counter == 1000: 
                    break
                success = True
        except asyncio.TimeoutError as e:
            print("Async timeout fetching published PDF for %s: %s", paper, e)
        except Exception as e:
            print(f"Error extracting published paper: {e}")
        if not success:
            print(f"Could not extract text from {published_link}, storing preprint only")
            paper_dict.update({
                "preprint_doi": latest_pub.get('preprint_doi'),
                "published_doi": latest_pub.get('published_doi'),
                "published_journal": latest_pub.get('published_journal'),
                "preprint_title": paper_title,
                "preprint_authors": latest_pub.get('preprint_authors'),
                "preprint_category": latest_pub.get('preprint_category'),
                "preprint_date": latest_pub.get('preprint_date'),
                "published_date": latest_pub.get('published_date'),
                "preprint_author_corresponding": latest_pub.get('preprint_author_corresponding'),
                "preprint_author_corresponding_institution": latest_pub.get('preprint_author_corresponding_institution'),
                "preprint_paper": research_text_bucket["preprint_paper"],
                "published_paper": None
                })
            await unextracted_q.put(paper_dict)
            continue 
            
        
async def writer(path, queue):
    async with aiofiles.open(path, 'a') as f:
        while True:
            item = await queue.get()
            if item is None:
                break
            await f.write(json.dumps(item) + "\n")

async def main():

    
    extracted_q = asyncio.Queue()
    unextracted_q = asyncio.Queue()
    unknown_q = asyncio.Queue()
    
    
    extract_task = asyncio.create_task(
        extract_preprint_and_published_papers(extracted_q, unextracted_q, unknown_q)
    )
    
    writer_task1 = asyncio.create_task(writer(extract_file_name, extracted_q))
    writer_task2 = asyncio.create_task(writer(unextracted_file_name, unextracted_q))
    writer_task3 = asyncio.create_task(writer(unknown_file_name, unknown_q))


    await extract_task
    
    await extracted_q.put(None)
    await unextracted_q.put(None)
    await unknown_q.put(None)
    
    
    await asyncio.gather(writer_task1, writer_task2, writer_task3)

    print(f"Completed extraction of {counter} papers.")
    
if __name__ == "__main__":
    asyncio.run(main())
    print("Extraction completed. Check the output files for results.")
