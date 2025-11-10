import os, asyncio,requests ,sys, aiofiles,json
from typing import List 
import logging

cwd = os.path.dirname(os.path.abspath(__file__))
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
if parent_folder not in sys.path:
    sys.path.append(parent_folder)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
from modules.paper_to_doi import get_article_info_from_title
from functions_and_classes import bioarxiv_reader
from functions_and_classes.regex_cleaning import remove_newlines , clean_title
from functions_and_classes.process_pdf import extract_pdf 
from functions_and_classes.pdf_resolver_call import extract_text_with_pdf_resolver
biorxiv_api = bioarxiv_reader.bioarxiv_api()


# title_prompt = """
#                 You are given a part of the introductory excerpt from a scientific paper.

#                 Your sole task is to extract and return the **exact title** of the paper, using the rules below. 
                
                
#                 The title will be found in the first non empty line of the text

#                 Task: from the text below, return **exactly** the paper title or **Title not found**

#                 Rules (highest priority first)
#                 1.return the first non-empty line that appears **before** any line that starts with `Authors` or `Affiliations`.
#                 3. If nothing matches, return: Title not found

#                 Output format (must follow **exactly**):
#                 <plain title>           ← no quotes, no label, no markdown
#                 """




async def agent_call(system_prompt: str, user_prompt: str) -> str:
    loop = asyncio.get_event_loop()
    async with llm_semaphore:
        return await loop.run_in_executor(None, call_llm, system_prompt, user_prompt)
    
async def retry_biorxiv(doi: str, preprint: bool):
    loop = asyncio.get_running_loop()
    for attempt in range(3):
        try:
            return await loop.run_in_executor(None, biorxiv_api.request_specific_preprint, preprint, doi)
        except requests.RequestException:
            logging.warning(f"Request failed for DOI {doi}, attempt {attempt + 1}")
            await asyncio.sleep(1 * (2 ** attempt))
    return None

async def extract_single_paper(paper_path: str , gpu_list:List) -> dict:
    
    paper_name = os.path.basename(paper_path)
    logging.info(f"Starting extraction for: {paper_name}")
    paper_dict = {
        "preprint_pdf_name": paper_name,
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
        "url": None
    }

    # --- Step 1: Extract preprint text
    preprint_paper = extract_pdf(paper_path)



    # --- Step 2: Infer title
    title_raw = await agent_call(system_prompt=title_prompt, user_prompt=remove_newlines(str(first_chunk)))
    title_cleaned = clean_title(title_raw)

    if title_cleaned == "Title not found":
        logging.warning(f"Title not found for {paper_name}")
        #log
        paper_dict.update({
                "preprint_pdf_name": paper_name,
                "preprint_doi": None,
                "published_doi":None,
                "published_journal": None,
                "preprint_title": title_cleaned,
                "preprint_authors": None,
                "preprint_category": None,
                "preprint_date": None,
                "published_date": None,
                "preprint_author_corresponding": None,
                "preprint_author_corresponding_institution": None,
                "preprint_paper": preprint_paper,
                "published_paper": None
            })
        return {"status": "unknown", "data": paper_dict}
    #log
    # --- Step 3: Lookup preprint metadata
    preprint_info = get_article_info_from_title(title_cleaned)

    if not preprint_info or not preprint_info.get("doi"):
        #log
        logging.warning(f"DOI not found for title: {title_cleaned}")
        paper_dict.update({
                'preprint_pdf_name': paper_name,
                "preprint_doi": None,
                "published_doi":None,
                "published_journal": None,
                "preprint_title": title_cleaned,
                "preprint_authors": None,
                "preprint_category": None,
                "preprint_date": None,
                "published_date": None,
                "preprint_author_corresponding": None,
                "preprint_author_corresponding_institution": None,
                "preprint_paper": preprint_paper,
                "published_paper": None
            })
        return {"status": "unknown", "data": paper_dict}

    
    # --- Step 4: Get preprint metadata from biorxiv
    #log
    preprint_meta = await retry_biorxiv(preprint_info["doi"], preprint=True)
    preprint_coll = (preprint_meta or {}).get("collection", [])

    if not preprint_coll:
        logging.warning(f"Preprint metadata not found for {title_cleaned}")
        print(f"preprint info was not found on biorxiv") #change to log
        paper_dict.update({
                'preprint_pdf_name': paper_name,
                "preprint_doi": None,
                "published_doi":None,
                "published_journal": None,
                "preprint_title": title_cleaned,
                "preprint_authors": None,
                "preprint_category": None,
                "preprint_date": None,
                "published_date": None,
                "preprint_author_corresponding": None,
                "preprint_author_corresponding_institution": None,
                "preprint_paper": preprint_paper,
                "published_paper": None
            })
        return {"status": "unknown", "data": paper_dict}

    #log
    latest_preprint = preprint_coll[-1]
    published_doi = latest_preprint.get('published')

    if published_doi == "NA":
            logging.info(f"{paper_name} has not been published. Storing preprint only.")
            print("Preprint has not been published yet, storing preprint")
            #store its metadata information and save it to unextracted_papers
            paper_dict.update({
                'preprint_pdf_name': paper_name,
                "preprint_doi": latest_preprint.get('doi'),
                "published_doi":None,
                "published_journal": None,
                "preprint_title": title_cleaned,
                "preprint_authors": latest_preprint.get('authors'),
                "preprint_category": latest_preprint.get('category'),
                "preprint_date": latest_preprint.get('date'),
                "published_date": None,
                "preprint_author_corresponding": latest_preprint.get('author_corresponding'),
                "preprint_author_corresponding_institution": latest_preprint.get('author_corresponding_institution'),
                "preprint_paper": preprint_paper,
                "published_paper": None
            })
            return {"status": "unextracted", "data": paper_dict}

    # --- Step 5: Extract published paper info
    published_meta = await retry_biorxiv(published_doi, preprint=False)
    published_coll = (published_meta or {}).get("collection", [])
     # --- Step 6: If no published version, return early

    if not published_coll:
        logging.warning(f"Published metadata not found for {title_cleaned}")
        print(f"published info was not found on biorxiv for {title_cleaned} storing preprint") # change to log
        paper_dict.update({
                'preprint_pdf_name': paper_name,
                "preprint_doi": latest_preprint.get('doi'),
                "published_doi":None,
                "published_journal": None,
                "preprint_title": title_cleaned,
                "preprint_authors": latest_preprint.get('authors'),
                "preprint_category": latest_preprint.get('category'),
                "preprint_date": latest_preprint.get('date'),
                "published_date": None,
                "preprint_author_corresponding": latest_preprint.get('author_corresponding'),
                "preprint_author_corresponding_institution": latest_preprint.get('author_corresponding_institution'),
                "preprint_paper": preprint_paper,
                "published_paper": None
            })
        return {"status": "unextracted", "data": paper_dict}
    
    #log found published info
    latest_pub = published_coll[0]
    confirmed_published_doi = latest_pub.get('published_doi')        
    published_text = None
    url = None
    # --- Step 7: Download and clean published paper

    try:
        published_result = await extract_text_with_pdf_resolver(doi = confirmed_published_doi, paper_id= confirmed_published_doi, selector_timeout=40_000 )
        if isinstance(published_result, dict) and 'url' in published_result: 
            #log failure
            published_text = None 
            url = published_result.get("url")
            paper_dict["url"] = url
            logging.warning(f"Failed to extract published PDF for {paper_name}, fallback URL used.")
    except asyncio.TimeoutError as e:
        logging.error(f"Timeout fetching published PDF for {paper_name}: {e}")
        print("Async timeout fetching published PDF for %s: %s", paper_name, e) # change to log
    except Exception as e:
        logging.error(f"Unexpected error extracting published paper for {paper_name}: {e}")
        print(f"Error extracting published paper: {e}") #change to log
            # --- Step 6: If no published version, return early
        paper_dict.update({
                'preprint_pdf_name': paper_name,
                "preprint_doi": latest_pub.get('preprint_doi'),
                "published_doi": latest_pub.get('published_doi'),
                "published_journal": latest_pub.get('published_journal'),
                "preprint_title": title_cleaned,
                "preprint_authors": latest_pub.get('preprint_authors'),
                "preprint_category": latest_pub.get('preprint_category'),
                "preprint_date": latest_pub.get('preprint_date'),
                "published_date": latest_pub.get('published_date'),
                "preprint_author_corresponding": latest_pub.get('preprint_author_corresponding'),
                "preprint_author_corresponding_institution": latest_pub.get('preprint_author_corresponding_institution'),
                "preprint_paper": preprint_paper,
                "published_paper": None, 
                "url": url
                })
        return {"status": "unextracted", "data": paper_dict}
    # --- Step 8: Store published info

    if published_text:
        # #bug here need to number the batches before sending for inference
        # published_chunks = chunk_text_by_char_limit(published_text, limit=7500)
        # published_chunks_dict = {paper_name: published_chunks}
        # # expected_pub_chunks = {paper_name: len(published_chunks)}
        # await dispatch_batches(paper_chunks=published_chunks_dict, available_gpus= gpu_list, system_prompt=cleaning_prompt, preprint=False)
        # # published_cleaned_chunks = await asyncio.gather(*(agent_call(system_prompt=cleaning_prompt, user_prompt=c) for c in published_chunks))
        # published_merged_text = merge_when_ready(num_expected_batches= len(published_chunks_dict[paper_name]), paper_id=paper_name, preprint=False)
        # published_cleaned_text = published_merged_text.get(paper_name, "")
        #log 
        paper_dict.update({
            'preprint_pdf_name': paper_name,
            "preprint_doi": latest_pub.get('preprint_doi'),
            "published_doi": latest_pub.get('published_doi'),
            "published_journal": latest_pub.get('published_journal'),
            "preprint_title": title_cleaned,
            "preprint_authors": latest_pub.get('preprint_authors'),
            "preprint_category": latest_pub.get('preprint_category'),
            "preprint_date": latest_pub.get('preprint_date'),
            "published_date": latest_pub.get('published_date'),
            "preprint_author_corresponding": latest_pub.get('preprint_author_corresponding'),
            "preprint_author_corresponding_institution": latest_pub.get('preprint_author_corresponding_institution'),
            "preprint_paper": preprint_paper,
            "published_paper": published_text
        })

    logging.info(f"Extraction complete for {paper_name}")
    return {"status": "extracted", "data": paper_dict}


async def writer(path: str, queue: asyncio.Queue):
    async with aiofiles.open(path, 'a') as f:
        while True:
            item = await queue.get()
            if item is None:
                break
            await f.write(json.dumps(item) + "\n")