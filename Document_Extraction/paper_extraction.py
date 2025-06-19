import os
import sys
cwd = os.getcwd()

parent_folder = os.path.abspath(os.path.join(cwd, ".."))
sys.path.append(parent_folder)


from dotenv import load_dotenv
import asyncio
import csv
from functions_and_classes.bioarxiv_class import *
from functions_and_classes.functions import *
from LLM_Agent.llm_template import LLMAgent
from selenium.common.exceptions import TimeoutException
import sys
import json

load_dotenv()

repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
extract_save_folder = os.path.join(repo_root, "papers", "extracted_papers")
unextract_save_folder = os.path.join(repo_root, "papers", "unextracted_papers")
os.makedirs(extract_save_folder, exist_ok=True)
os.makedirs(unextract_save_folder, exist_ok=True)

extract_file_name = os.path.join(extract_save_folder, "extracted_papers.jsonl")
unextract_file_name = os.path.join(unextract_save_folder, "unextracted_papers.jsonl")
year_range_str = "2017-06-01/2020-01-01"

api_key = os.getenv("OPENAI_API_KEY")
model_name = 'Llama-3-8B-Instruct-exl2'
agent = LLMAgent(model_name)

async def extract_all_papers(extracted_q, unextracted_q):
    api = bioarxiv_api()
    paper_metadata = api.get_all_papers(year_range_str , limit=3000)
    print(len(paper_metadata))

    # global paper_metadeta_approved_list, paper_metadeta_unextracted_list
    # paper_metadeta_approved_list = []
    # paper_metadeta_unextracted_list = []
    count = 0

    for paper in paper_metadata:
        doi_types = {
            'preprint_doi': f"https://www.biorxiv.org/content/{paper['preprint_doi']}v1",
            'published_doi': f"https://doi.org/{paper['published_doi']}"
        }

        research_text_bucket = {
            "preprint_doi": None,
            "published_doi": None
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

        for paper_key, paper_url in doi_types.items():
            try:
                print(f'Extracting {paper_key}')
                if paper_key == "preprint_doi":
                    relevant_text = await get_biorxiv_pdf_link(paper_url)
                else:
                    pdf_href = agent.one_turn(
                        system_prompt="""
                        You are an assistant that analyzes the HTML of academic paper webpages to extract the direct PDF download link.

                        Given the URL to a scientific paper, perform the following:
                        1. Follow any redirects to land on the final page hosting the full paper.
                        2. Analyze the HTML and look for anchor (`<a>`) tags that link to PDF documents.
                           - These usually contain "pdf" in the `href`
                           - May use a button or text like "Download PDF", "View PDF", etc.
                        3. Return only the absolute URL to the actual `.pdf` file — it must be a direct link to a downloadable PDF (not an HTML viewer or embedded viewer).
                        4. If a PDF link contains `download=true`, return the **same link with `download=false` instead**.
                        5. Do not include any commentary or analysis

                        Examples:
                        Input: https://doi.org/10.1111/bph.15505
                        Output: https://bpspubs.onlinelibrary.wiley.com/doi/pdfdirect/10.1111/bph.15505?download=true
                        Input: https://www.biorxiv.org/content/10.1101/2020.06.02.130062v2
                        Output: https://www.biorxiv.org/content/10.1101/2020.06.02.130062v2.full.pdf
                        Input: https://bpspubs.onlinelibrary.wiley.com/doi/pdfdirect/10.1111/bph.15505?download=true
                        Output: https://bpspubs.onlinelibrary.wiley.com/doi/pdfdirect/10.1111/bph.15505?download=False
                            """,
                        user_prompt=paper_url
                    )
                    clean_href = pdf_href.strip()
                    relevant_text = await extract_text_from_pdf_via_browser(clean_href)

                if relevant_text is None:
                    print(f"Could not Get {paper_key} Paper .. moving to next paper")
                    continue

                print(f"Extracted {paper_key}")
                chunk_text_storage = []
                chunks = chunk_text_by_char_limit(relevant_text)

                for chunk in chunks:
                    cleaned_chunk = agent.one_turn(
                        system_prompt="""
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
                    """,
                        user_prompt=chunk
                    )
                    chunk_text_storage.append(cleaned_chunk)

                cleaned_text = " ".join(chunk_text_storage)
                research_text_bucket[paper_key] = cleaned_text


            except TimeoutException:
                print('Time out while loading')
                continue
            except Exception as e:
                print(e)
                continue

        if research_text_bucket["preprint_doi"] and research_text_bucket["published_doi"]:
            paper_dict.update({
                "preprint_doi": paper["preprint_doi"],
                "published_doi": paper["published_doi"],
                "published_journal": paper["published_journal"],
                "preprint_title": paper["preprint_title"],
                "preprint_authors": paper["preprint_authors"],
                "preprint_category": paper["preprint_category"],
                "preprint_date": paper["preprint_date"],
                "published_date": paper["published_date"],
                "preprint_author_corresponding": paper["preprint_author_corresponding"],
                "preprint_author_corresponding_institution": paper["preprint_author_corresponding_institution"],
                "preprint_paper": research_text_bucket["preprint_doi"],
                "published_paper": research_text_bucket["published_doi"]
            })
            await extracted_q.put(paper_dict)
            # paper_metadeta_approved_list.append(paper_dict)
            count += 1
            print(f"Papers Extracted: {count}")
        else:
            paper_dict.update({
                "preprint_doi": paper["preprint_doi"],
                "published_doi": paper["published_doi"],
                "published_journal": paper["published_journal"],
                "preprint_title": paper["preprint_title"],
                "preprint_authors": paper["preprint_authors"],
                "preprint_category": paper["preprint_category"],
                "preprint_date": paper["preprint_date"],
                "published_date": paper["published_date"],
                "preprint_author_corresponding": paper["preprint_author_corresponding"],
                "preprint_author_corresponding_institution": paper["preprint_author_corresponding_institution"],
                "preprint_paper": research_text_bucket["preprint_doi"] or "N/A",
                "published_paper": research_text_bucket["published_doi"] or "N/A"
            })
            await unextracted_q.put(paper_dict)
            # paper_metadeta_unextracted_list.append(paper_dict)
            print("Could not Get Published and PrePrint Paper Pairs, stored paper info and moving on...")
        
        if count >= 500:
            print(f"Extracted {count} papers")
            break 
    await extracted_q.put(None)  # Signal end of extraction for the writer
    await unextracted_q.put(None)  # Signal end of extraction for the writer

async def json_writer(file_path, queue): 
    first_row = await queue.get()
    if first_row is None: 
        return 
    
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(json.dumps(first_row) + '\n')
        
        while True:
            item = await queue.get()
            if item is None:
                break
            file.write(json.dumps(item) + '\n')
            
            
async def main():
    
    
    
    extracted_queue = asyncio.Queue()
    unextracted_queue = asyncio.Queue()
    
    await asyncio.gather(
        extract_all_papers(extracted_queue, unextracted_queue), 
        json_writer(extract_file_name, extracted_queue), 
        json_writer(unextract_file_name, unextracted_queue)
    )
    print("All tasks completed. Exiting.")
    

    
    
            
if __name__ == "__main__":
    
    
    asyncio.run(main())
    # asyncio.run(extract_all_papers())
    
    
    # extracted_queue = asyncio.Queue()
    # unextracted_queue = asyncio.Queue()
    
    # await asyncio.gather(
    #     extract_all_papers(), 
    #     json_writer(extract_file_name, extracted_queue)
    # )

    # if paper_metadeta_approved_list:
    #     with open(extract_file_name, 'w', encoding='utf-8') as file:
    #         json.dump(paper_metadeta_approved_list, file, ensure_ascii=False, indent=2)
    #     print(f"Extract File saved to: {extract_file_name}")

    # if paper_metadeta_unextracted_list:
    #     with open(unextract_file_name, 'w', encoding='utf-8') as file:
    #         json.dump(paper_metadeta_unextracted_list, file, ensure_ascii=False, indent=2)
    #     print(f"Unextract File saved to: {unextract_file_name}")
