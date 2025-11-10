from exllamav2 import ExLlamaV2, ExLlamaV2Tokenizer , ExLlamaV2Config , ExLlamaV2Cache 
from exllamav2.generator import ExLlamaV2DynamicGenerator , ExLlamaV2Sampler , ExLlamaV2DynamicJob
from exllamav2.architecture import ExLlamaV2ArchParams
from typing import Sequence, Optional, List, Dict, Any
import logging, os, sys, atexit, signal, json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


def truncate_each_to_limit_wordwise(items, limit: int):
    
    if limit < 0:
        raise ValueError("limit must be >= 0")

    out = []
    for s in items:
        # normalize to string, remove zero-width spaces, trim
        t = "" if s is None else str(s)
        t = t.replace("\u200b", "").strip()

        if len(t) <= limit:
            out.append(t)
            continue

        cut = t[:limit]
        pos = cut.rfind(" ")
        # if we found whitespace before the limit, cut there; otherwise hard cut
        out.append((cut[:pos].rstrip()) if pos != -1 else cut)
    return out

def make_llama3_chat( user_text: str) -> str:
    system_text = (
        "Given a medical research paper extract every clinical trial mention with its registration number"
        "(NCT, ISRCTN, EudraCT, CTRI, ChiCTR, ANZCTR, UMIN-CTR, jRCT). "
        "Output STRICT JSON: {\"trials\":[{\"name\":\"<trial name or null>\","
        "\"registration_number\":\"<exact id as written>\"}]}. "
        "Deduplicate by registration number. If none, return {\"trials\":[]}. "
        "Use only information present in the paper. Output ONLY the JSON.\n\n"

        )
    # return system_text
    user_block = (
            f"----- PAPER START -----\n{user_text}\----- PAPER END -----"
        )
        
    return (
        "<|begin_of_text|>"
        "<|start_header_id|>system<|end_header_id|>\n"
        f"{system_text}\n"
        "<|eot_id|>"
        "<|start_header_id|>user<|end_header_id|>\n"
        f"{user_block}\n"
        "<|eot_id|>"
        "<|start_header_id|>assistant<|end_header_id|>\n"
    )

def get_stop_conditions(prompt_format, tokenizer):
    """
    Stop when Llama-3 emits end-of-turn <|eot_id|>. Also include eos_token_id
    if the tokenizer defines one (harmless to have both).
    """
    if prompt_format == "llama3":
        stops = []
        try:
            eot_id = tokenizer.single_id("<|eot_id|>")
            if eot_id is not None and eot_id >= 0:
                stops.append(eot_id)
        except Exception:
            pass
        eos = getattr(tokenizer, "eos_token_id", None)
        if eos is not None and eos >= 0:
            stops.append(eos)
        # dedup while preserving order
        seen, uniq = set(), []
        for s in stops:
            if s not in seen:
                seen.add(s); uniq.append(s)
        return uniq

    elif prompt_format == "llama":
        return [tokenizer.eos_token_id]

    elif prompt_format == "granite":
        return [tokenizer.eos_token_id, "\n\nQuestion:"]

    elif prompt_format == "gemma":
        return [tokenizer.eos_token_id, "<end_of_turn>"]

    else:
        raise ValueError(f"Unknown prompt_format: {prompt_format!r}")


def batch_call_llm(generator, 
                   sampler, 
                   tokenizer, 
                   user_prompts: list, 
                   ids: list) -> str:
    prompt_format = 'llama3'

    if ids is None:
        ids = [str(i) for i in range(len(user_prompts))]
    assert len(ids) == len(user_prompts), "Length of ids must match length of prompts"

    jobs = []
    jobs_to_idx = {}

    for i, (pid, ptxt) in enumerate(zip(ids, user_prompts)): 
        usr_msg = make_llama3_chat(ptxt)

        input_ids = tokenizer.encode(usr_msg, encode_special_tokens = True)
        job = ExLlamaV2DynamicJob( 
            input_ids=input_ids,
            max_new_tokens = 128, 
            stop_conditions = get_stop_conditions(prompt_format, tokenizer), 
            add_bos = True,
            gen_settings = sampler,

        )
        jobs.append(job)
        jobs_to_idx[job] = i
    generator.enqueue(jobs)
    records: List[Optional[Dict[str, Any]]] = [None] * len(user_prompts)


    while generator.num_remaining_jobs(): 
        results = generator.iterate()
        for r in results: 
            if r['stage'] == 'streaming' and r["eos"]: 
                job = r['job']
                i = jobs_to_idx[job]

                rec = {
                "doi": ids[i],
                "full_completion": r.get('full_completion'),
                "new_tokens": r.get("new_tokens", 0),
                "prompt_tokens": r.get("prompt_tokens", 0),
                "cached_tokens": r.get("cached_tokens", 0),
                "time_enqueued_s": r.get("time_enqueued", 0.0),
                "time_prefill_s": r.get("time_prefill", 0.0),
                "time_generate_s": r.get("time_generate", 0.0),
                "eos_reason": r.get("eos_reason", "eos"),
            }
                records[i] = rec
                logging.info(f"Completed job for id {ids[i]}")
    logging.info("All jobs completed")
    return [rec for rec in records if rec is not None]