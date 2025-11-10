import atexit
import os
import sys
import time
from pathlib import Path
from typing import Optional
import requests
import json
import argparse
import math
import shlex, subprocess, sys


log_fh  = None



def parse_args():
    ap = argparse.ArgumentParser(description="Serve vLLM and run a pipeline")
    ap.add_argument("--model-name", required=True, help="HF id or local path to the model")
    ap.add_argument("--port", type=int, default=8000, help="Port to run the server on (default: 8000)")
    ap.add_argument("--max-model-len", type=int, default=None, help="Override max tokens; if omitted, model default is used")
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.9, help="Fraction of GPU memory to use (0–1, default: 0.9)")
    ap.add_argument("--out-dir", type=Path, required=True, help="Directory to write outputs/logs")
    ap.add_argument("--markdown", action="store_true", help="If set, save outputs in Markdown format")
    ap.add_argument("--input-dir", type=Path, required=True, help="Directory containing PDFs")
    ap.add_argument("--recursive", action="store_true", help="Recurse into subdirectories for PDFs")
    ap.add_argument("--pattern", default=None, help="Optional additional Path.match() pattern to filter PDFs")
    return ap.parse_args()


def start_vllm_server(model: str, port: int, max_model_len: Optional[int], gpu_mem: float):
    job_id = os.environ.get("SLURM_JOB_ID", "local")
    log_path = Path.home() / "biopapersnlp" / "shell_scripts" / "logs" / f"vllm_{job_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    gpu_mem = float(gpu_mem)
    if math.isnan(gpu_mem) or math.isinf(gpu_mem) or not (0.0 < gpu_mem <= 1.0):
        raise ValueError("--gpu-memory-utilization must be in (0, 1].")

    cmd = [ 
        "vllm", "serve", model, 
        "--host", "127.0.0.1", 
        "--port", str(port), 
        "--gpu-memory-utilization", str(gpu_mem), 
        "--enforce-eager"
        ]

    if max_model_len is not None:
        cmd += ["--max-model-len", str(max_model_len)]
    global log_fh
    log_fh = open(log_path, "w")

    # Launch in background, fully detached from this process’ stdio
    proc = subprocess.Popen(
        cmd,
        stdout=log_fh,
        stderr=subprocess.STDOUT,         
        stdin=subprocess.DEVNULL,          
        start_new_session=True,           
        text=True,                      
        bufsize=1                        
    )

    def _cleanup():
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=30)
            except Exception:
                proc.kill()
        try:
            log_fh.close()
        except Exception:
            pass
    atexit.register(_cleanup)

    print(f"[INFO] Started vLLM pid={proc.pid}, logging to {log_path}")
    return proc, log_fh
    

def wait_for_vllm_health(
    port: int,
    proc: Optional["subprocess.Popen"] = None,   # pass the Popen if you have it
    max_wait_min: int = 60,
    sleep_secs: float = 2.0,
        ) -> None:
    """Poll http://127.0.0.1:{port}/health until 200 OK, or fail on timeout/proc exit."""

    deadline = time.monotonic() + max_wait_min * 60
    base = f"http://127.0.0.1:{port}"
    print("[INFO] Waiting for vLLM to become healthy", end="", flush=True)

    while True:
        # Success case
        try:
            r = requests.get(f"{base}/health", timeout=2)
            if r.status_code == 200:
                print()  # newline after dots
                return
        except Exception:
            pass

        # Process died?
        if proc is not None and proc.poll() is not None:
            print("\n[ERROR] vLLM process is no longer running.", file=sys.stderr)
            raise RuntimeError("vLLM server died before becoming healthy")

        # Timeout?
        if time.monotonic() >= deadline:
            print(f"\n[ERROR] Timeout waiting for vLLM (>{max_wait_min} min).", file=sys.stderr)
            raise TimeoutError("Timed out waiting for vLLM /health")

        # Wait and print a dot
        time.sleep(sleep_secs)  
        print(".", end="", flush=True)



def api_call_check( 
    model_name:str, 
    port:int
    ) -> None: 
    base = f"http://127.0.0.1:{port}"

    try: 
        r = requests.get(f"{base}/v1/models", timeout = 5)
        r.raise_for_status()
    except requests.RequestException as e: 
        raise RuntimeError(f"Failed to reach vLLM at {base}:{e}")
    
    data = r.json()
    print(json.dumps(data, indent = 2))

    names = {m.get("id") for m in data.get("data", [])}
    if model_name not in names: 
        raise RuntimeError( 
            "Model '{model_name}' not listed by /v1/models. Found: {sorted(names)}"
        )
        

def run_olmocr_pipeline(port: int, input_dir: Path, out_dir: Path, markdown: bool, log_fh):
    server = f"http://127.0.0.1:{port}/v1"



    cmd_str = (
        f"{shlex.quote(sys.executable)} -m olmocr.pipeline "
        f"{shlex.quote(str(out_dir))} "
        + ("--markdown " if markdown else "")
        + f"--server {shlex.quote(server)} "
        f"--pdfs {shlex.quote(str(input_dir))}/*.pdf"
    )
 
    print("[INFO] Running:", cmd_str)

    ret = subprocess.run(
        cmd_str,
        shell=True,             
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if ret.returncode != 0:
        print("[ERROR] olmocr pipeline failed", file=sys.stderr)
        sys.exit(ret.returncode)
    print("[INFO] Pipeline finished successfully")
    print(f"[INFO] Done. Outputs in: {out_dir}")

def list_pdfs(base: Path, recursive: bool, pattern: str | None):
    if recursive:
        it = base.rglob("*.pdf")
        it2 = base.rglob("*.PDF")
    else:
        it = base.glob("*.pdf")
        it2 = base.glob("*.PDF")
    files = list(it) + list(it2)
    if pattern:
        # simple post-filter using Path.match
        files = [p for p in files if p.match(pattern)]
    return sorted(set(files))


def main(): 


    args = parse_args()

    proc, log_fh = start_vllm_server(
    model=args.model_name,
    port=args.port,
    max_model_len=args.max_model_len,
    gpu_mem=args.gpu_memory_utilization,
    )

    wait_for_vllm_health(port=args.port, proc=proc)


    api_call_check(model_name=args.model_name, port=args.port)



    pdfs = list_pdfs(args.input_dir, recursive=args.recursive, pattern=args.pattern)
    if not pdfs:
        print(f"[ERROR] No PDFs found in {args.input_dir}", file=sys.stderr)
        sys.exit(2)

    print("Found PDFS")
    print("Executing Olmocr Pipeline")

 

    try:
        run_olmocr_pipeline(
            port=args.port,
            input_dir= args.input_dir,  # or pass expanded list if olmocr supports multiple
            out_dir=args.out_dir,
            markdown=args.markdown,
            log_fh=log_fh,
        )
    finally:
        # Stop server explicitly; atexit also cleans up
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
        try:
            log_fh.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
    

    
