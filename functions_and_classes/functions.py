from pdf2image import convert_from_bytes
import pytesseract
import pdfplumber
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
import io
from playwright.async_api import async_playwright
import random
import os
import httpx
from dotenv import load_dotenv

load_dotenv()


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
]

def chunk_text_by_char_limit(text, limit=8000):
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
                headless=False,
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
                print("playwright_stealth not installed; continuing without stealth")

            print(f"Navigating to EZProxy URL: {proxied_url}")
            await page.goto(proxied_url, wait_until="load")
            await page.wait_for_timeout(30000)

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
                print(f"⚠️ Failed extracting nested class-based PDF link: {e}")

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

        print("All extraction methods failed.")
        return None

    except Exception as e:
        print(f"Error during extraction: {e}")
        return None



async def get_biorxiv_pdf_link(article_url: str) -> str:
    try:
        random_ua = random.choice(USER_AGENTS)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--no-sandbox",
                    "--disable-gpu"
                ]
            )

            context = await browser.new_context(
                user_agent=random_ua,
                viewport={"width": 1280, "height": 720},
                locale='en-US',
                timezone_id='America/New_York',
                geolocation={"longitude": -114.0719, "latitude": 51.0447},
                permissions=["geolocation"]
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
                print("playwright-stealth not installed. Proceeding without stealth mode.")

            print(f"Navigating to: {article_url}")
            await page.goto(article_url, wait_until="networkidle")
            await page.wait_for_timeout(30000)

            current_url = page.url
            if current_url.lower().endswith(".pdf") or "pdf" in current_url.lower():
                print(f"Already on PDF page: {current_url}")
                pdf_link = current_url
            else:
                pdf_href = None
                try:
                    await page.wait_for_selector("a.article-dl-pdf-link", timeout=30000)
                    pdf_href = await page.locator("a.article-dl-pdf-link").get_attribute("href")
                except Exception as e:
                    print(f"Could not find PDF link via class selector: {e}")

                if not pdf_href:
                    print("No PDF link found.")
                    await browser.close()
                    return None

                pdf_link = urljoin(page.url, pdf_href)
                print(f"Final PDF URL: {pdf_link}")

            cookies = await context.cookies()
            await browser.close()

            cookie_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
            headers = {
                "Cookie": "; ".join(f"{k}={v}" for k, v in cookie_dict.items()),
                "User-Agent": random_ua,
                "Accept": "text/html,application/pdf;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": article_url,
                "Connection": "keep-alive",
                "DNT": "1"
            }

            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.get(pdf_link, headers=headers)
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                if "pdf" not in content_type.lower():
                    print(f"Unexpected Content-Type: {content_type}")
                    return None

                pdf_data = response.content
                extracted_text = extract_pdf(pdf_data)
                return extracted_text

    except Exception as e:
        print(f"Error: {e}")
        return None

