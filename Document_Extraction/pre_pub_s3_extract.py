import os , asyncio,sys 
from dotenv import load_dotenv
import torch 
load_dotenv

cwd = os.path.dirname(os.path.abspath(__file__))

parent_folder = os.path.abspath(os.path.join(cwd, ".."))
if parent_folder not in sys.path:
    sys.path.append(parent_folder)
    
from functions_and_classes.main_script import extract_single_paper ,writer

output_main_dir = os.getenv("test_output")
test_dir = os.getenv('test_dir')


extract_save_folder = os.path.join(output_main_dir, "papers", "extracted_papers")
unextracted_save_folder = os.path.join(output_main_dir, "papers", "unextracted_papers")
unknown_save_folder = os.path.join(output_main_dir, "papers", "unknown_papers")

available_gpus = list(range(torch.cuda.device_count()))

for folder in (extract_save_folder, unextracted_save_folder, unknown_save_folder):
    os.makedirs(folder, exist_ok=True)


file_paths = {
    "extracted": os.path.join(extract_save_folder, "extracted_papers.jsonl"),
    "unextracted": os.path.join(unextracted_save_folder, "unextracted_papers.jsonl"),
    "unknown": os.path.join(unknown_save_folder, "unknown_papers.jsonl")
}

async def enqueue_pdfs_from_folder(task_queue: asyncio.Queue, folder_path: str, enqueued_files: set):
    for filename in os.listdir(folder_path):
        if filename.lower().endswith(".pdf"):
            full_path = os.path.join(folder_path, filename)
            if full_path not in enqueued_files:
                await task_queue.put(full_path)
                enqueued_files.add(full_path)



async def process_and_route(task_q, extracted_q, unextracted_q, unknown_q, worker_id:int):
    while True:
        paper = await task_q.get()
        if paper is None:  # Sentinel value to stop the worker
            print(f"[Worker {worker_id}] Finished Processing.")
            break
        try:
            result = await extract_single_paper(paper, available_gpus=available_gpus)
        except Exception as e:
            # Handle unexpected exceptions
            print(f"Error extracting published paper: {paper}")
            await unextracted_q.put(paper)
            continue
        
        status = result.get("status")
        data = result.get("data")

        if status == "extracted":
            await extracted_q.put(data)
        elif status == "unextracted":
            await unextracted_q.put(data)
        elif status == "unknown":
            await unknown_q.put(data)
        else:
            print(f"[WARN] Unknown status '{status}' for file {paper}") # change to log


async def main():
    
    extracted_q = asyncio.Queue()
    unextracted_q = asyncio.Queue()
    unknown_q = asyncio.Queue()
    
    task_q = asyncio.Queue()
    enqueued_files = set()
    
    await enqueue_pdfs_from_folder(task_q, test_dir, enqueued_files)
    num_workers = 5 
    
    for _ in range(num_workers):
        await task_q.put(None)  # Add a sentinel value to stop workers later
        
    extraction_tasks = [
        asyncio.create_task(process_and_route(task_q , extracted_q, unextracted_q, unknown_q, i))
        for i in range(num_workers)
    ]
    
    writer_task1 = asyncio.create_task(writer(file_paths["extracted"], extracted_q))
    writer_task2 = asyncio.create_task(writer(file_paths["unextracted"], unextracted_q))
    writer_task3 = asyncio.create_task(writer(file_paths["unknown"], unknown_q))

    # tasks = []
    # for paper in os.listdir(test_dir): 
    #     if paper.endswith(".pdf"): 
    #         paper_path = os.path.join(test_dir, paper)
    #         task = asyncio.create_task(process_and_route(
    #         paper_path, extracted_q, unextracted_q, unknown_q
    #     ))
    #     tasks.append(task)
            
    await asyncio.gather(*extraction_tasks)
    
    await extracted_q.put(None)
    await unextracted_q.put(None)
    await unknown_q.put(None)
    
    await asyncio.gather(writer_task1, writer_task2, writer_task3)

    
