
from exllamav2 import ExLlamaV2, ExLlamaV2Tokenizer , ExLlamaV2Config , ExLlamaV2Cache 
from exllamav2.generator import ExLlamaV2DynamicGenerator , ExLlamaV2Sampler , ExLlamaV2DynamicJob
from exllamav2.architecture import ExLlamaV2ArchParams
from typing import Sequence, Optional, List, Dict, Any
import logging, os, sys, atexit, signal, json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


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




def inline_llm_call(generator , sampler, tokenizer , user_prompt, prompt_id): 

    prompt = make_llama3_chat(user_prompt)
    prompt_encoded = tokenizer.encode(prompt, add_bos=True, encode_special_tokens=True)

    result = generator.generate(
        prompt = prompt,
        gen_settings = sampler, 
        max_new_tokens = 128,
        stop_conditions = [tokenizer.eos_token_id] ,
        add_bos = True,
        return_tokens  = True
        )
    if isinstance(result, tuple):
        out_tokens = result[1]
    else:
        # fallback: no tokens returned → nothing to slice; just return text minus prompt
        out_text = result
        completion = out_text[len(prompt):].strip()
        return {"doi": prompt_id, "completion": completion}

    # decode ONLY the tokens generated after the prompt
    new_tokens = out_tokens[len(prompt_encoded):]
    completion = tokenizer.decode(new_tokens).strip()

    completion =  tokenizer.decode(out_tokens[len(prompt_encoded):]).strip()
    record = {
        "doi": prompt_id, 
        "completion": completion
    }

    return record 
