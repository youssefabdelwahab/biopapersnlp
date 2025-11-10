import asyncio , pdfplumber
# from LLM_Agent.inline_llm_template import call_llm , llm_semaphore
from typing import List , Tuple , Dict
import os , sys
import pdfplumber , pytesseract
from pdf2image import convert_from_bytes

cwd = os.path.dirname(os.path.abspath(__file__))
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
if parent_folder not in sys.path:
    sys.path.append(parent_folder)

from  functions_and_classes.distribute_papers import dispatch_batches
    


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


# async def agent_call(system_prompt: str, user_prompt: str) -> str:
#     loop = asyncio.get_event_loop()
#     async with llm_semaphore:
#         return await loop.run_in_executor(None, call_llm, system_prompt, user_prompt)

#bug here need to number the batches before sending for inference
# def chunk_text_by_char_limit(text:str, limit:int):
#     return [(i // limit, text[i:i + limit]) for i in range(0, len(text), limit)]

# async def process_pdf(path: str , paper_id: str , available_gpus:List, preprint:bool) -> Tuple[List[str], str, Dict[str, int]]:
#     paper = []
#     loop = asyncio.get_running_loop()
#     try:
#         with pdfplumber.open(path) as pdf:
#             for page_num, page in enumerate(pdf.pages, 1):
#                 try:
#                     page_text = page.extract_text() or ""
#                     paper.append(page_text)
#                 except Exception:
#                     continue
#                 if not page_text.strip():
#                     continue
#             full_text = " ".join(paper)
#             paper_chunks = chunk_text_by_char_limit(full_text, limit=7500)
#             paper_chunks_dict = {paper_id: paper_chunks} 

#             # page_chunks_gen = await dispatch_batches(page_chunks_dict, available_gpus, system_prompt=cleaning_prompt, preprint=preprint)
#                 # paper_chunks.append(" ".join(page_chunks_cleaned))
#     except Exception:
#         return None
#     return paper_chunks_dict , paper_chunks[0]






def extract_pdf(pdf_path): 
     
    extracted_text = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
                for pdf_page in pdf.pages:
                    page_text = pdf_page.extract_text()
                    if page_text:
                        extracted_text.append(page_text)
    except Exception as e:
        print(f"pdfplumber failed: {e}")

    return "\n".join(extracted_text).strip() if extracted_text else None