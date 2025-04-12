import os
from dotenv import load_dotenv
import asyncio
import csv
from functions_and_classes.bioarxiv_class import *
from functions_and_classes.functions import *
from LLM_Agent.llm_template import LLMAgent
from selenium.common.exceptions import TimeoutException

load_dotenv()

repo_root = os.path.dirname(os.path.abspath(__file__))
extract_save_folder = os.path.join(repo_root, "papers", "extracted_papers")
unextract_save_folder = os.path.join(repo_root, "papers", "unextracted_papers")
os.makedirs(extract_save_folder, exist_ok=True)
os.makedirs(unextract_save_folder, exist_ok=True)

extract_file_name = os.path.join(extract_save_folder, "extracted_papers.csv")
unextract_file_name = os.path.join(unextract_save_folder, "extracted_papers.csv")
year_range_str = "2020-01-01/2025-01-01/1"

api_key = os.getenv("OPENAI_API_KEY")
model_name = 'Llama-3-8B-Instruct-exl2'
agent = LLMAgent(model_name)

async def extract_all_papers():
    api = bioarxiv_api()
    paper_metadata = api.request_papers("GET" , year_range_str)

    global paper_metadeta_approved_list, paper_metadeta_unextracted_list
    paper_metadeta_approved_list = []
    paper_metadeta_unextracted_list = []
    count = 2000

    for paper in paper_metadata:
        doi_types = {
            'preprint_doi': f"https://www.biorxiv.org/content/{paper['preprint_doi']}",
            'published_doi': f"https://doi.org/{paper['published_doi']}"
        }

        research_text_bucket = []
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
                            ... (prompt omitted for brevity) ...
                        """,
                        user_prompt=paper_url
                    )
                    relevant_text = await extract_text_from_pdf_via_browser(pdf_href)

                if relevant_text is None:
                    print(f"Could not Get {paper_key} Paper .. moving to next paper")
                    continue

                print(f"Extracted {paper_key}")
                chunk_text_storage = []
                chunks = chunk_text_by_char_limit(relevant_text)

                for chunk in chunks:
                    cleaned_chunk = agent.one_turn(
                        system_prompt="""
                            The following text is extracted from a research paper. Your task is to:
                            ... (cleaning prompt omitted for brevity) ...
                        """,
                        user_prompt=chunk
                    )
                    chunk_text_storage.append(cleaned_chunk)

                cleaned_text = " ".join(chunk_text_storage)
                research_text_bucket.append(cleaned_text)

            except TimeoutException:
                print('Time out while loading')
                continue
            except Exception as e:
                print(e)
                continue

        if len(research_text_bucket) == 2:
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
                "preprint_paper": research_text_bucket[0],
                "published_paper": research_text_bucket[1]
            })
            paper_metadeta_approved_list.append(paper_dict)
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
                "preprint_paper": "N/A",
                "published_paper": 'N/A'
            })
            paper_metadeta_unextracted_list.append(paper_dict)
            print("Could not Get Published and PrePrint Paper Pairs, stored paper info and moving on...")

        if count >= 2000:
            break 
        print(f"Extracted {count} papers")

if __name__ == "__main__":
    asyncio.run(extract_all_papers())

    if paper_metadeta_approved_list:
        with open(extract_file_name, 'w', newline='', encoding='utf-8') as file:
            fieldnames = paper_metadeta_approved_list[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for full_metadata_dict in paper_metadeta_approved_list:
                writer.writerow(full_metadata_dict)
        print(f"Extract File saved to: {extract_save_folder}")

    if paper_metadeta_unextracted_list:
        with open(unextract_file_name, 'w', newline='', encoding='utf-8') as file:
            fieldnames = paper_metadeta_unextracted_list[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for full_metadata_dict in paper_metadeta_unextracted_list:
                writer.writerow(full_metadata_dict)
        print(f"Unextract File saved to: {unextract_save_folder}")
