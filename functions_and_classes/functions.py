from pdf2image import convert_from_bytes
import pytesseract
import pdfplumber
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
import io
from playwright.async_api import async_playwright , Error as PlaywrightError
import random
import os
import httpx
from dotenv import load_dotenv
import logging
from tenacity import retry, stop_after_attempt, wait_exponential , retry_if_exception_type
from typing import Optional, List
from transformers import PreTrainedTokenizerFast


load_dotenv()

log_folder = os.path.join(os.path.dirname(__file__), "..", "Logs")
os.makedirs(log_folder, exist_ok=True)
log_file_path = os.path.join(log_folder, "extraction.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()  
    ]
)
logging.getLogger("pdfminer").setLevel(logging.ERROR)

USER_AGENTS = [
   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/125.0",
]

tokenizer = PreTrainedTokenizerFast.from_pretrained(
    "/home/longlab/tabbyAPI/models/Llama-3-8B-Instruct-exl2",
    local_files_only=True
)

def chunk_text_by_char_limit_tokens(text, chunk_size = 8000):
    input_ids = tokenizer.encode(text, add_special_tokens=False)
    chunks = [input_ids[i:i+chunk_size] for i in range(0, len(input_ids), chunk_size)]
    return [tokenizer.decode(chunk, skip_special_tokens=True) for chunk in chunks]

def chunk_text_by_char_limit(text, limit):
    return [text[i:i+limit] for i in range(0, len(text), limit)]


def extract_text_with_ocr(pdf_bytes):
    images = convert_from_bytes(pdf_bytes)
    full_text = []
    for i, img in enumerate(images):
        text = pytesseract.image_to_string(img)
        if text:
            full_text.append(text)
    return "\n".join(full_text).strip() if full_text else None


def extract_pdf(pdf_data): 
     
    extracted_text = []

    try:
        with pdfplumber.open(io.BytesIO(pdf_data)) as pdf:
                for pdf_page in pdf.pages:
                    page_text = pdf_page.extract_text()
                    if page_text:
                        extracted_text.append(page_text)
    except Exception as e:
        print(f"pdfplumber failed: {e}")

    if not extracted_text:
        print("Falling back to OCR...")
        ocr_text = extract_text_with_ocr(pdf_data)
        return ocr_text

    return "\n".join(extracted_text).strip() if extracted_text else None

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(4))
async def extract_text_from_pdf_via_browser(landing_url: str):
    try:
        login_url = 'https://login.ezproxy.lib.ucalgary.ca/login'
        ezproxy_prefix = 'https://ezproxy.lib.ucalgary.ca/login?url='
        proxied_url = ezproxy_prefix + landing_url
        random_ua = random.choice(USER_AGENTS)

        username = os.getenv('uni_username')
        password = os.getenv('uni_password')
        if not username or not password:
            raise ValueError("University EZProxy credentials not found in environment variables")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--no-sandbox",
                    "--disable-gpu"
                ]
            )
            context = await browser.new_context(
                user_agent=random_ua,
                locale='en-US',
                timezone_id='America/New_York'
            )
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """)
            page = await context.new_page()

            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                logging.warning("playwright_stealth not installed; continuing without stealth")

            print(f"Navigating to EZProxy URL: {proxied_url}")
            await page.goto(proxied_url, wait_until="networkidle" , timeout=50000)
            await page.wait_for_timeout(50000)

            current_url = page.url
            if current_url.lower().endswith(".pdf") or "pdf" in current_url.lower():
                print(f"Already on a PDF page: {current_url}")
                response = await context.request.get(current_url)
                if response.status == 200 and "pdf" in response.headers.get("content-type", "").lower():
                    print("PDF detected via URL.")
                    content = await response.body()
                    text = extract_pdf(content)
                    if text: return text

            try:
                pdf_href = await page.get_by_role("link", name="PDF").get_attribute("href")
                if pdf_href:
                    resolved_link = urljoin(page.url, pdf_href)
                    response = await context.request.get(resolved_link)
                    if response.status == 200 and "pdf" in response.headers.get("content-type", "").lower():
                        print("PDF extracted from PDF button.")
                        content = await response.body()
                        text = extract_pdf(content)
                        if text: return text
            except:
                pass

            print("🔍 Searching nested button group structure for download link...")
            try:
                download_group = page.locator("div.grouped.right")
                buttons = await download_group.locator("a.navbar-download.btn.btn--cta.roundedColored").all()
                for button in buttons:
                    href = await button.get_attribute("href")
                    if href and "pdf" in href:
                        pdf_link = urljoin(page.url, href)
                        print(f"📄 PDF found under nested class structure: {pdf_link}")
                        response = await context.request.get(pdf_link)
                        if response.status == 200 and "pdf" in response.headers.get("content-type", "").lower():
                            content = await response.body()
                            text = extract_pdf(content)
                            if text: return text
            except Exception as e:
                print(f"Failed extracting nested class-based PDF link: {e}")

            print("Attempting fallback reconstruction from epdf link...")
            epdf_url = landing_url
            if "/epdf/" in landing_url:
                try:
                    parsed = urlparse(epdf_url)
                    parts = parsed.path.split("/epdf/")
                    if len(parts) == 2:
                        prefix, suffix = parts[0], parts[1]
                        pdf_variants = ["pdf", "pdfdirect", "pdfdownload"]
                        cookies = await context.cookies()
                        await browser.close()

                        cookie_dict = {c["name"]: c["value"] for c in cookies}
                        headers = {
                            "User-Agent": random_ua,
                            "Accept": "application/pdf",
                            "Cookie": "; ".join(f"{k}={v}" for k, v in cookie_dict.items()),
                            "Referer": landing_url
                        }

                        for variant in pdf_variants:
                            reconstructed = f"{parsed.scheme}://{parsed.netloc}{prefix}/{variant}/{suffix}?download=False"
                            print(f"Trying reconstructed URL: {reconstructed}")
                            async with httpx.AsyncClient(follow_redirects=True) as client:
                                resp = await client.get(reconstructed, headers=headers)
                                print(f"ℹ️ Status: {resp.status_code}, Content-Type: {resp.headers.get('Content-Type')}")
                                if resp.status_code == 200:
                                    content_type = resp.headers.get("Content-Type", "").lower()
                                    if "pdf" in content_type:
                                        print("✅ PDF detected via reconstructed URL.")
                                        text = extract_pdf(resp.content)
                                        if text: return text
                except Exception as e:
                    print(f"Error during fallback reconstruction: {e}")

            cookies = await context.cookies()
            await browser.close()

        cookie_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
        headers = {
            "Cookie": "; ".join(f"{k}={v}" for k, v in cookie_dict.items()),
            "User-Agent": random_ua
        }

        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            response = await client.get(landing_url, headers=headers)
            if response.status_code == 200 and "pdf" in response.headers.get("content-type", "").lower():
                print("PDF fetched directly from landing URL.")
                return extract_pdf(response.content)

        logging.warning(f"All extraction methods failed for {landing_url}.")
        return None

    except Exception as e:
        logging.error(f"{landing_url} faced errors during extraction: {e}")
        return None

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=20)
)
async def get_biorxiv_pdf_link(article_url: str) -> Optional[str]:
    """Return the text extracted from the bioRxiv PDF at *article_url*.

    The function automatically appends the ``.full.pdf`` suffix if missing and
    tries the request with several header bundles to bypass Cloudflare’s 403
    filters.  If all bundles fail (or the response is not a PDF) Tenacity will
    retry up to 5 times with exponential back‑off.
    """
    _HEADERS_SETS: List[dict] = [
    {
        # Plain desktop Chrome UA – works for most PDF files
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        ),
    },
    {
        # + explicit Accept and benign Referer ‑ defeats Cloudflare bot check
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        ),
        "Accept": "application/pdf",
        "Referer": "https://www.biorxiv.org/",
    },
    {
        # XHR‑like headers as a last resort
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.biorxiv.org/",
    },
]
    pdf_url = article_url if article_url.endswith(".full.pdf") else f"{article_url}.full.pdf"

    last_exc: Optional[Exception] = None

    for hdr in _HEADERS_SETS:
        try:
            async with httpx.AsyncClient(headers=hdr, timeout=60, follow_redirects=True) as client:
                resp = await client.get(pdf_url.strip())
                resp.raise_for_status()

                if resp.headers.get("Content-Type", "").lower().startswith("application/pdf"):
                    return extract_pdf(resp.content)  # type: ignore[name-defined]

                # Not a PDF – break early, no need to try other headers
                print("⚠️  Not a PDF response (content‑type:", resp.headers.get("Content-Type"), ")")
                return None

        except httpx.HTTPStatusError as exc:
            # 403 / 429 etc – remember and try next header bundle
            last_exc = exc
            if exc.response.status_code in {403, 429}:
                continue  # try next header set
            # Other HTTP error – bail out immediately so Tenacity can retry
            raise
        except Exception as exc:
            # Network / TLS errors – keep for Tenacity
            last_exc = exc
            raise

    # All header bundles exhausted – propagate last exception so Tenacity sees a failure.
    if last_exc:
        raise last_exc

    return None
# async def get_biorxiv_pdf_link(article_url: str) -> str:
#     try:
#         if not article_url.endswith(".full.pdf"):
#             pdf_url = article_url + ".full.pdf"
#         else:
#             pdf_url = article_url

#         async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
#             response = await client.get(pdf_url.strip())
#             response.raise_for_status()

#             if "pdf" not in response.headers.get("Content-Type", "").lower():
#                 print("⚠️ Not a PDF response")
#                 return None

#             pdf_data = response.content
#             return extract_pdf(pdf_data)

#     except Exception as e:
#         print(f"❌ Failed to fetch PDF: {e}")
#         raise  # Let tenacity retry




# @retry(
#     stop=stop_after_attempt(5),
#     wait=wait_exponential(multiplier=1, min=2, max=20)
# )
# async def get_biorxiv_pdf_link(article_url: str) -> str:
#     try:
#         if not article_url.endswith(".full.pdf"):
#             pdf_url = article_url + ".full.pdf"
#         else:
#             pdf_url = article_url

#         async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
#             response = await client.get(pdf_url.strip())
#             response.raise_for_status()

#             if "pdf" not in response.headers.get("Content-Type", "").lower():
#                 print("⚠️ Not a PDF response")
#                 return None

#             pdf_data = response.content
#             return extract_pdf(pdf_data)

#     except Exception as e:
#         print(f"❌ Failed to fetch PDF: {e}")
#         raise  # Let tenacity retry






