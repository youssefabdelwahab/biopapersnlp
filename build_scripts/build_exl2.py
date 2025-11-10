import os
import sys
import glob
import shutil
import importlib
from pathlib import Path

print("=== Build configuration ===")
print("Python:", sys.version)
try:
    import torch
    print("Torch:", torch.__version__)
except Exception as e:
    print("Torch import failed:", e)
    sys.exit(1)

CUDA_HOME = os.environ.get("CUDA_HOME")
EXT_DIR   = os.environ.get("TORCH_EXT_DIR") or str(Path.home()/".cache/torch_extensions")
ARCHS     = os.environ.get("TORCH_CUDA_ARCH_LIST", "")
print("CUDA_HOME:", CUDA_HOME)
print("TORCH_EXT_DIR:", EXT_DIR)
print("TORCH_CUDA_ARCH_LIST:", ARCHS)
print("===========================")

# 1) Ensure a truly fresh state inside the extensions root
#    (sbatch script already removed the whole root, but do an extra sweep)
for pat in ("exllamav2_ext", "**/exllamav2_ext"):
    for p in glob.glob(os.path.join(EXT_DIR, pat), recursive=True):
        shutil.rmtree(p, ignore_errors=True)

# 2) Importing exllamav2 triggers C++/CUDA build into TORCH_EXTENSIONS_DIR
print("Importing exllamav2 to trigger a fresh JIT build...")
import exllamav2
from exllamav2 import ext  # this forces the extension to compile/load

# 3) Report where the .so landed
print("\nBuild complete.")
print("Extension module path:", ext.__file__)

# 4) Sanity: confirm we can call a tiny symbol (doesn't execute kernels)
print("exllamav2 version:", getattr(exllamav2, "__version__", "unknown"))
