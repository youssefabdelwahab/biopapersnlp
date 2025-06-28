"""
pdf_resolver.py
================

Asynchronous DOI / landing‑page → PDF resolver supporting many major
publishers (Springer, OUP, Wiley, F1000Research, Hindawi, etc.).

The class exposes an **async context‑manager** interface so HTTP resources
are cleaned up automatically:

```python
async with PDFResolver() as resolver:
    pdf_url = await resolver.get_pdf(doi, landing_url, paper_id)
```

If no PDF can be located it raises :class:`CantDownload`; if neither a DOI
nor a landing URL is supplied it raises :class:`MissingIdentifier`.
"""






import re 
import io
from bs4 import BeautifulSoup
from typing import Optional , AsyncIterator , List
import urllib.parse
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright , Playwright , Browser , TimeoutError as PWTimeoutError, Page
import asyncio , functools
import os
from typing import Optional
from urllib.parse import quote , urlparse , urljoin
import httpx
from httpx import Timeout, AsyncClient
import random
from playwright_stealth import stealth_async
import logging
import pdfplumber , pytesseract
from pdf2image import convert_from_bytes
import random
from playwright.async_api import BrowserContext 

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

def drop_www(host:str) -> str: 
    return host[4:] if host.startswith("www.") else host

login_selectors = {
            "user":   'input[name="user"], input[name="username"], #username',
            "pass":   'input[name="pass"], input[name="password"], #password',
            "submit": 'input[type="submit"], button[type="submit"]'

        }
JOURNAL_PDF_SELECTORS = {
        "bmj.com": 'a[title="Download this article as a PDF"]',
        "thelancet.com": 'a.download-pdf-link',
        "nejm.org": 'a[href*="pdf"]',
        "sciencedirect.com": 'a.pdf-download',
        "jamanetwork.com": 'a[href*="/pdf"]',
        "nature.com": 'a[data-track-action="download pdf"]',
        "springer.com": 'a[href$=".pdf"]',
        "oup.com": 'a[href$=".pdf"]',
        "frontiersin.org": 'a[href*="/pdf"]',
        "tandfonline.com": 'a[href*="/pdf"]',
        "sagepub.com": 'a.article-tools-download',
        "karger.com": 'a[href$=".pdf"]',
        # Add more as needed...
    }
class PDFResolver:
    playwright: Playwright | None = None
    browser : Browser| None = None
    lock: asyncio.Lock = asyncio.Lock()
    def __init__(self, *, selector_timeout: int  , client: Optional[AsyncClient] = None):
        self._client = client
        self.headers = {
    "User-Agent": random.choice([
        # realistic desktop browsers
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36",

        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Safari/605.1.15",

        "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0",
    ]),
    "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "application/pdf;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
}
        self.timeout = selector_timeout
        self.user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/125.0",
]
        self.selector_timeout = selector_timeout
        self._SPRINGER_HOST = "link.springer.com"
        self._OUP_HOST_RE = re.compile(r"^https?://academic\.oup\.com/")
        self._F1000_HOST_RE = re.compile(r"^https?://(?:f1000research|wellcomeopenresearch|gatesopenresearch)\.org/")
        self._HINDAWI_DOWNLOAD_RE = re.compile(r"^https?://downloads\.hindawi\.com/")
        self._HINDAWI_LANDING_RE = re.compile(r"^https?://(?:www\.)?hindawi\.com/")
        self._ANCHOR_HINT_RE = re.compile(r"""(?x)                        ]
        (
            pdf
            | full[\s_-]*text
            | full[\s_-]*article
            | view[\s_-]*article
            | view[\s_-]*pdf
            | read[\s_-]*(this[\s_-]*)?article
            | article[\s_-]*as[\s_-]*pdf
            | open[\s_-]*access
            | download
            | dl[\s_-]*(pdf|article)?
            | link[\s_-]*to[\s_-]*pdf
            | show[\s_-]*pdf
            | get[\s_-]*pdf
            | pdf[\s_-]*download
            | primary[\s_-]*document
            | main[\s_-]*article
            | publication[\s_-]*file
            | document[\s_-]*view
            | article[\s_-]*file
            | content[\s_-]*pdf
            | view[\s_-]*full[\s_-]*text
            | citation[\s_-]*pdf[\s_-]*url
        )
        """,
        re.I
    )
        self.wiley_token = os.getenv("wiley_api_token")

    class CantDownload(Exception):
        """
        Raised when the resolver failed to find a downloadable PDF.
        Carries the DOI and the final landing URL we ended up on.
        """
        def __init__(self, doi:str | None, landing:str):
            self.doi = doi
            self.landing = landing
            super().__init__(f"Could not download PDF for DOI {doi} from {landing}")
    
    class PDFResolverError(Exception): 
        """Base class for PDF resolver errors."""
        pass 
        
    class MissingIdentifier(PDFResolverError):
        """Neither DOI nor landing URL was supplied."""
        pass 
    
    class CantDownload(Exception):
        """
        Raised when the resolver failed to find a downloadable PDF.
        Carries the DOI and the final landing URL we ended up on.
        """
        def __init__(self, doi:str | None, landing:str):
            self.doi = doi
            self.landing = landing
            super().__init__(f"Could not download PDF for DOI {doi} from {landing}")

    async def __aenter__(self):
        self._client = httpx.AsyncClient(headers=self.headers, follow_redirects=True, timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._client:
            await self._client.aclose()
            self._client = None

    def _client_required(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers=self.headers, 
                follow_redirects=True, 
                timeout=Timeout(15.0, read=30.0), 
                http2=True)
        return self._client
    
    async def context_required(self) -> BrowserContext: 
        async with PDFResolver.lock:
            if PDFResolver.browser is None:
                PDFResolver.playwright = await async_playwright().start()
                PDFResolver.browser = await PDFResolver.playwright.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--disable-web-security",
                        "--no-sandbox",
                        "--disable-gpu",
                    ],
                )

        context = await PDFResolver.browser.new_context(
            user_agent=random.choice(self.user_agents),
            locale="en-US",
            timezone_id="America/New_York",
        )
        await context.add_init_script(
            """
            Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
            Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});
            Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3]});
            """
        )
        return context
    
    def _springer_candidates(self, landing: str, doi: str) -> List[str]:
        base = f"https://{self._SPRINGER_HOST}"
        candidates = [f"{base}/content/pdf/{doi}.pdf"]
        if landing.endswith(("fulltext.html", "fulltext.htm")):
            candidates.append(landing.rsplit("/", 1)[0] + ".pdf")
        elif landing.endswith(".html"):
            candidates.append(landing[:-5] + ".pdf")
        elif "/article/" in landing and not landing.endswith(".pdf"):
            candidates.append(landing + ".pdf")
        return candidates

    async def _f1000_pdf(self, landing: str, doi: str) -> Optional[str]:
        client = self._client_required()
        article_id = doi.split("/")[-1]
        host = urllib.parse.urlparse(landing).hostname
        api_url = f"https://api.{host}/article/{article_id}"
        try:
            r = await client.get(api_url)
            r.raise_for_status()
            f100_url = r.json()["data"]["pdf_url"]
        except Exception:
            return None 
        try: 
            url_response = await self.try_pdf_url(f100_url)
        except Exception as e: 
            if url_response: 
                return url_response 
        return None

    async def _oup_pdf(self, landing: str) -> Optional[str]:
        client = await self._client_required()
        html = (await client.get(landing)).text
        oup_meta = self._extract_meta_pdf(html)
        try: 
            meta_response = await self.try_pdf_http(oup_meta)
        except Exception as e: 
            if meta_response: 
                return meta_response 
        oup_anchor = self._extract_anchor_pdf_score(html, landing)
        try: 
            anchor_response = await self.try_pdf_url(oup_anchor)
        except Exception as e: 
            if anchor_response: 
                return anchor_response
        return None

    async def _wiley_pdf(self , landing:str , doi:str) -> Optional[str]:
        client = self._client_required()
        
        #1 TDM Api
        api_url = f"https://api.wiley.com/tdm/v1/articles?{doi}/pdf"
        try: 
            url_response = await self.try_pdf_http(api_url)
        except Exception as e: 
            if url_response: 
                return url_response 
        
        #pdfdirect 
        
        pdf_url = f"https://onlinelibrary.wiley.com/doi/pdfdirect/{doi}"
        r2 = await client.get(pdf_url)
        try:
            url_response = await self.try_pdf_url(r2)
        except Exception as e: 
            if url_response: 
                return url_response
        #htmlparse 
        
        html = (await client.get(landing)).text
        soup = BeautifulSoup(html, "html.parser")
        btn = soup.find("a", class_="pdf-download-link", href=True)
        if btn:
            resolved_btn_link =  urllib.parse.urljoin(landing, btn["href"])
            try: 
                btn_response = await self.try_pdf_url(resolved_btn_link)
            except Exception as e: 
                if btn_response: 
                    return btn_response 
        return None
    
    async def _tand_pdf(self , landing:str , doi:str) -> Optional[str]:
        client = self._client_required()
        
        #direct pdf endpoint 
        
        pdf_endpoint = f"https://www.tandfonline.com/doi/pdf/{doi}"
        resolved_tand_link  = await client.get(pdf_endpoint)
        try: 
            url_response = await self.try_pdf_url(resolved_tand_link)
        except Exception as e: 
            if url_response: 
                return url_response 
    
        #meta tag
        html = (await client.get(landing)).text
        if (meta:= self._extract_meta_pdf(html)):
            resolved_tand_meta = urllib.parse.urljoin(landing, meta)
            try: 
                meta_response = await self.try_pdf_http(resolved_tand_meta)
            except Exception as e: 
                if meta_response: 
                    return meta_response 
        
        if (anchor := self._extract_anchor_pdf_score(html, landing)):
            resolved_tand_anchor = anchor 
            try: 
                anchor_response = await self.try_pdf_url(resolved_tand_anchor)
            except Exception as e: 
                if anchor_response: 
                    return resolved_tand_anchor 
        return None
            
        
    async def _sage_pdf(self, landing: str, doi: str) -> Optional[str]:
        client = self._client_required()
        
        #pdf endpoint
        
        pdf_url = f"https://journals.sagepub.com/doi/pdf/{doi}"
        r = await client.get(pdf_url)
        try: 
            url_response = await self.try_pdf_url(r)
        except Exception as e: 
            if url_response: 
                return r 
        
        html = (await client.get(landing)).text
        if (meta:= self._extract_meta_pdf(html)):
            resolved_sage_meta = urllib.parse.urljoin(landing, meta)
            try:
                meta_response = await self.try_pdf_http(resolved_sage_meta)
            except Exception as e: 
                if meta_response: 
                    return resolved_sage_meta 
        
        if (anchor := self._extract_anchor_pdf_score(html, landing)):
            resolved_sage_anchor = anchor
            try: 
                anchor_response = await self.try_pdf_url(resolved_sage_anchor)
            except Exception as e: 
                #log 
                if anchor_response: 
                    return resolved_sage_anchor 
        return None
        
        
    async def _karger_pdf(self, landing: str, doi: str) -> Optional[str]:
        client = self._client_required()
        
        #must get article id 
        
        html = (await client.get(landing)).text
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("meta", attrs={"name": "dc.identifier"})
        if not tag or not tag.get("data-article-id"):
            try: 
                tag_response = await self.try_pdf_http(tag)
            except Exception as e: 
                if tag_response: 
                    return tag_response
    
        article_id = tag["data-article-id"]
        resolved_karger_link = f"https://www.karger.com/Article/Pdf/{article_id}"
        try: 
            karger_response = await self.try_pdf_url(resolved_karger_link)
        except Exception as e: 
            #log 
            if karger_response: 
                return resolved_karger_link 
        return None
    
    
    async def try_pdf_url(self,
        url: str,
        context: BrowserContext | None = None
    ) -> str | None:
        """
        Download *url* via Playwright’s request API and extract text if it is a PDF.

        Parameters
        ----------
        context : BrowserContext
            The Playwright context that owns the current page.
        url : str
            Candidate PDF link (absolute URL).

        Returns
        -------
        str | None
            Extracted text if successful, otherwise *None*.
        """
        if context is None: 
            context =  await self.context_required()
        try:
            resp = await context.request.get(url, timeout=self.selector_timeout)
        except Exception as exc:
            print(f"[resolver]   GET failed for {url!s}: {exc}")
            return None

        # Quick validation: 200 OK + URL / MIME hint contains 'pdf'
        if resp.status != 200:
            return None

        # Prefer content-type header when present; fallback to URL check
        ctype = resp.headers.get("content-type", "").lower()
        if "pdf" not in ctype and not url.lower().endswith(".pdf"):
            return None

        # Confirm first bytes contain %PDF-magic
        raw = await resp.body()
        if not raw:
            return None
        print('Successfully Extracted PDF from URL:', url)
        loop = asyncio.get_running_loop()
        text = await loop.run_in_executor(None, functools.partial(extract_pdf, raw))
        if text:
            return text or None
    
    
    async def try_pdf_http(self,url:str):
        """
        Download *url* with httpx, verify it is a PDF (%PDF magic),
        then return extracted text (or None on failure).
        """
        client = self._client_required()
        try:
            resp = await client.get(url, timeout=15)
        except Exception as exc:
            print(f"[resolver] httpx GET failed: {exc}")
            return None

        if resp.status_code != 200:
            return None

        ctype = resp.headers.get("content-type", "").lower()
        if "pdf" not in ctype and not url.lower().endswith(".pdf"):
            return None

        raw = resp.content
        if not raw:
            return None
        print('Successfully downloaded PDF from URL:', url)
        loop = asyncio.get_event_loop()
        text = await loop.run_in_executor(None, functools.partial(extract_pdf, raw))
        if text: 
            return text or None
    
    def _extract_anchor_pdf_score(self, html: str, base_url: str) -> Optional[str]:
        soup = BeautifulSoup(html, "html.parser")
        join = lambda h: urllib.parse.urljoin(base_url, h)
        best = None

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            lower = href.lower()
            full_url = join(href)

            text = a.get_text(" ").lower()
            cls = " ".join(a.get("class", [])).lower()
            ident = a.get("id", "").lower()
            title = a.get("title", "").lower()
            aria = a.get("aria-label", "").lower()

            score = 0
            if lower.endswith(".pdf"):
                score += 3
            if "/article-pdf/" in lower or "/advance-article-pdf/" in lower:
                score += 2
            if "pdf" in text:
                score += 2
            if re.search(self._ANCHOR_HINT_RE, cls) or re.search(self._ANCHOR_HINT_RE, ident):
                score += 1
            if "full text" in text or "read article" in text:
                score += 1
            if "pdf" in title or "pdf" in aria:
                score += 1

            if score >= 3:  # only consider fairly confident ones
                return full_url
            elif not best and score > 0:
                best = full_url
        return best
    
    async def find_via_selector(self,domain: str, page: Page) -> str | None:
        selector = JOURNAL_PDF_SELECTORS.get(domain)
        if not selector:
            return None                # nothing to try

        try:
            await page.wait_for_selector(selector, timeout=self.selector_timeout)
        # take only the first match to avoid strict-mode failure
            href   = await page.locator(selector).first.get_attribute("href")
            if not href:
                return None

            resolved = urljoin(page.url, href)
            text     = await self.try_pdf_url(resolved)
            return text                 # None when not a PDF / could not extract
        except PWTimeoutError:
            return None
        
    async def find_pdf_button(self,page: Page) -> str | None:
        try:
            href_btn= await page.get_by_role("link", name="PDF").get_attribute("href")
        except TimeoutError: 
            return None
        if not href_btn:
            return None                     # no PDF button found
        try:
            resolved_href = urljoin(page.url, href_btn)
            href_response = await self.try_pdf_url(resolved_href)
            if href_response: 
                return href_response
        except TimeoutError:
            return None
    
    async def find_via_anchor(self,page: Page) -> str | None:
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                text = a.get_text(" ").lower()
                cls = " ".join(a.get("class", [])).lower()
                id_ = a.get("id", "").lower()
                title = a.get("title", "").lower()
                aria = a.get("aria-label", "").lower()
                
                looks_like_pdf = (
                    href.lower().endswith(".pdf") or
                    self._ANCHOR_HINT_RE.search(text) or
                    self._ANCHOR_HINT_RE.search(href) or
                    self._ANCHOR_HINT_RE.search(cls) or
                    self._ANCHOR_HINT_RE.search(id_) or
                    self._ANCHOR_HINT_RE.search(title) or
                    self._ANCHOR_HINT_RE.search(aria)
                )
                
                if not looks_like_pdf:
                    continue
                
                anchor_link = urljoin(page.url, href)
                try:
                    anchor_response = await self.try_pdf_url(anchor_link)
                    if anchor_response: 
                        return anchor_response
                except Exception as e:
                    continue 
                        #log
        return None
                    
    async def find_via_redirect(self, page: Page) -> str | None:
        current_url = page.url
        if not  current_url.lower().endswith(".pdf") or "pdf" in current_url.lower():
            return None
        try:
            redirect_response = await self.try_pdf_url(current_url)
            if redirect_response: 
                return redirect_response
        except Exception as e: 
                #log 
            return None
            
    async def try_browser_strategies(self, domain:str,page: Page) -> str | None:
        """
        Run the **four** browser strategies in order.
        Return extracted text or *None* when everything fails.
        """
        
        strategies = [
            self.find_via_selector,
            self.find_via_redirect,
            self.find_pdf_button,
            self.find_via_anchor
        ]
        for fn in strategies:
            try:
                text = await fn(domain, page) if fn is self.find_via_selector else await fn(page)
            except Exception:
                continue
            if text:                # one of the strategies succeeded
                return text  
        # All four strategies failed
        print("All browser methods failed to extract PDF.")
        return None

    async def fetch_pdf_with_browser(self, landing) -> Optional[str]:
    
        client = self._client_required()
        # if doi:
        #     landing_url = str((await client.get(f"https://doi.org/{doi}")).url)
        # else:
        #     landing_url = str(landing)
        landing_url = landing
        domain = drop_www(urlparse(landing_url).netloc.lower())

        username = os.getenv("uni_username")
        password = os.getenv("uni_password")
        if not username or not password:
            raise RuntimeError("Missing EZProxy credentials.")

        context = await self.context_required()
        async with await  context.new_page() as page:
            try:
                await stealth_async(page)
            except ImportError:
                logging.warning("playwright_stealth not installed; continuing without stealth")

            await page.goto(landing_url, wait_until='networkidle')
                
            text = await self.try_browser_strategies(domain,page)
            if text:
                return text
            print("All browser methods failed to extract PDF.")

            return None
        
    @staticmethod
    def _extract_meta_pdf(html: str) -> Optional[str]:
        tag = BeautifulSoup(html, "html.parser").find("meta", attrs={"name": "citation_pdf_url"})
        return tag["content"].strip() if tag and tag.get("content") else None
    
    @staticmethod
    def _crossref_fallback(doi: str) -> Optional[str]:
        try:
            r = requests.get(f"https://api.crossref.org/works/{doi}", timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception:
            return None
        links = data.get("message", {}).get("link", [])
        best = None
        for link in links:
            ct = link.get("content-type")
            url = link.get("URL")
            if ct == "text/html":
                return url
            if ct == "application/pdf" and best is None:
                best = url
            if url and url.endswith(".pdf") and best is None:
                best = url
        return best 

    
    
    async def get_pdf(self, doi, paper_id ) -> str:
        print(f"Attempting to resolve pdf of paper_id: {paper_id}")
        client = self._client_required()
        if doi: 
            landing = str((await client.get(f"https://doi.org/{doi}")).url)
        else:
            return self.MissingIdentifier()
        print(f"Attempting to resolve pdf from : {landing}")

        if self._SPRINGER_HOST in landing:
            print("Trying with Springer landing page.")
            for springer_url in self._springer_candidates(landing, doi):
                try: 
                    springer_url = await self.try_pdf_url(springer_url)
                except Exception as e:
                    continue
                if springer_url:
                    print("Extracted PDF from Springer landing page.")

                    return springer_url

        if self._HINDAWI_DOWNLOAD_RE.match(landing) or self._HINDAWI_LANDING_RE.match(landing):
            print("Trying with Hindawi landing page.")
            for hindawi_paper in (landing, f"https://doi.org/{doi}"):
                try:
                    hindawi_url = await self.try_pdf_url(hindawi_paper)
                except Exception as e:
                    continue
                if hindawi_url:
                    print("Extracting PDF from Hindawi landing page.")
                    return hindawi_url

        if self._F1000_HOST_RE.match(landing):
            print("Trying with F1000 landing page.")
            f100_pdf = await self._f1000_pdf(landing, doi)
            if f100_pdf:
                print("Extracted PDF from F1000 landing page.")
                return f100_pdf

        if self._OUP_HOST_RE.match(landing):
            print("Trying with OUP landing page.")
            oup_pdf = await self._oup_pdf(landing)
            if oup_pdf:
                print("Extracted PDF from OUP landing page.")
                return oup_pdf
        
        
        if "onlinelibrary.wiley.com" in landing:
            print("Trying with Wiley landing page.")
            wiley_pdf = await self._wiley_pdf(landing, doi)
            if wiley_pdf:
                print("Extracted PDF from Wiley landing page.")
                return wiley_pdf
            
        html = (await client.get(landing)).text
        anchor = self._extract_anchor_pdf_score(html, landing)
        if anchor:
            print("Trying with anchor hint matching.")

            try: 
                anchor_pdf = await self.try_pdf_url(anchor)
            except Exception as e:
                anchor_pdf = None
            if anchor_pdf:
                print("Extracted PDF via anchor hint.")
                return anchor_pdf
        
        if doi and (cross := self._crossref_fallback(doi)):
            print("Trying with crossref fallback.")

            try: 
                cross_ref_pdf = await self.try_pdf_url(cross)
            except Exception as e:
                cross_ref_pdf = None
            if cross_ref_pdf:
                print("Extracted PDF via CrossRef fallback.")
                return cross_ref_pdf
                
        print("Trying with browser automation to extract PDF.")
        try:
            browser_pdf = await self.fetch_pdf_with_browser(landing)
        except Exception as e:
            browser_pdf = None
        if browser_pdf:
            print("Extrated PDF via browser automation.")
            return browser_pdf
        

        raise self.CantDownload(doi , landing)

    