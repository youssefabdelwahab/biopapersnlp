from docling.document_converter import DocumentConverter
import requests, re, time, os

CROSSREF_API_WAIT = 1/50 +0.05

def get_article_info_from_title(title):
    """
    This method takes a title string from a bibliography as input and returns a dictionary
    containing the title, DOI, and document link (if available) for the most relevant research
    paper on Crossref. If no suitable document link is found, the method returns None.

    :param title: A string representing the title of the research paper.
    :return: A dictionary containing the title, DOI, and document_link, or None if no document link is found.
    """
    url = f"https://api.crossref.org/works?query.bibliographic={title}&rows=1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "ok":
            items = data.get("message", {}).get("items", [])
            if items:
                # Get the first item
                item = items[0]
                doi = item.get("DOI")

                # Get all links from the item
                links = item.get("link", [])

                document_link = None

                # Iterate through links once to find the best document link
                for link in links:
                    content_type = link.get("content-type")
                    url_link = link.get("URL")

                    # Prioritize text/html
                    if content_type == "text/html":
                        document_link = url_link
                        break
                    # Fallback to application/pdf
                    elif content_type == "application/pdf" and document_link is None:
                        document_link = url_link
                    # Fallback to any .pdf URL
                    elif url_link and url_link.endswith(".pdf") and document_link is None:
                        document_link = url_link

                # Construct the result
                result = {"title": title, "doi": doi}

                if document_link:
                    result["document_link"] = document_link
                else: result["document_link"] = None

                return result

    except requests.exceptions.RequestException:
        return None

    return None

def get_info_from_doi(doi, returnTitle=True, addLicense=False):
    """
    Retrieve information from a DOI using the Crossref API, including title, document link, and license.
    
    Args:
        doi (str): Document Object Identifier
        returnTitle (bool): If True, include the title in the result. Default is True.
        addLicense (bool): If True, include the license information. Default is False.
    
    Returns:
        dict or None: A dictionary containing the requested information or None if no document link is found and returnTitle is False.
    """
    url = f"https://api.crossref.org/works/{doi}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        result = {"doi": doi}

        # Title retrieval
        if returnTitle:
            title = data.get('message', {}).get('title', [None])
            result['title'] = title[0] if title else None

        # Document link selection
        document_link = None
        links = data.get('message', {}).get('link', [])

        for link in links:
            content_type = link.get('content-type')
            url_link = link.get('URL')

            if content_type == 'text/html':
                document_link = url_link
                break
            elif content_type == 'application/pdf' and document_link is None:
                document_link = url_link
            elif url_link and url_link.endswith('.pdf') and document_link is None:
                document_link = url_link

        # Add document link if found
        if document_link:
            result['document_link'] = document_link

            # Add license if requested
            if addLicense:
                license_info = data.get('message', {}).get('license')
                if license_info:
                    result['license'] = license_info

        # Determine final return value
        if returnTitle:
            return result
        else:
            if 'document_link' in result:
                new_result = {'document_link': result['document_link']}
                if addLicense and 'license' in result:
                    new_result['license'] = result['license']
                return new_result
            else:
                return None

    except requests.exceptions.RequestException:
        return None

def parse_bibtex(file_path):
    """
    Parses a BibTeX file and returns a list of dictionaries, where each
    dictionary contains the key-value pairs from one BibTeX entry.
    
    Args:
        file_path (str): The path to the BibTeX file to parse.

    Returns:
        list: A list of dictionaries, where each dictionary contains the
            key-value pairs from one BibTeX entry.
    """

    with open(file_path, 'r', encoding='utf-8') as f:
        raw_text = f.read()
    
    raw_text = re.split("\@\w+\{", raw_text)
    articles = [i.strip() for i in raw_text if i.strip()]

    # Split each article into lines and strip whitespace from each line
    processed_articles = [[i.strip() for i in entry.splitlines()] for entry in articles]

    final_tex_dicts = []
    for item in processed_articles:
        key_value_pairs = [i.split(' = ') for i in item[1:] if ' = ' in i]

        element_dict = {}
        for element in key_value_pairs:
            # Remove latex classes
            feature_values = re.sub(r'\{\\.*?\}', '', element[1])
            # Remove braces and quotes from the value
            feature_values = feature_values.replace('{', '').replace('}', '').replace('"', '').rstrip(',')

            # Add the key-value pair to the dictionary
            if element[0] in ["title", "url", "author", "doi", "keywords"]:
                # Handle special cases for 'author' field
                element_dict[element[0]] = feature_values.split(" and ") if element[0]=="author" else feature_values
        
        if not element_dict.get("doi"):

            title_dict = get_article_info_from_title(element_dict.get("title"))

            if title_dict is not None:
                element_dict.update(title_dict)

        final_tex_dicts.append(element_dict)

    return final_tex_dicts

def get_omid_from_doi(doi):
    """
    Fetches the OpenCitations Metadata Identifier (OMID) for a given DOI.
    
    Args:
        doi (str): The DOI of the publication.

    Returns:
        str: The OMID in the format 'br/<number>'.

    Raises:
        Exception: If the request fails, no metadata is found, or the OMID is not found in the metadata.
    """
    url = f"https://opencitations.net/meta/api/v1/metadata/doi:{doi}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch metadata: {response.status_code}")
    metadata = response.json()

    if not metadata:
        raise Exception("No metadata found for the given DOI")

    id_field = metadata[0].get("id", "")

    omid_match = re.search(r"omid:br/(\d+)", id_field)
    if not omid_match:
        raise Exception("OMID not found in the metadata")

    omid = f"br/{omid_match.group(1)}"
    return omid

def get_doi_from_omid(omid):
    """
    Fetches the DOI for a given OpenCitations Metadata Identifier (OMID).

    Args:
        omid (str): The OMID of the publication in the format 'br/<number>'.

    Returns:
        str: The DOI of the publication.

    Raises:
        Exception: If the request fails, no metadata is found, or the DOI is not found in the metadata.
    """
    url = f"https://opencitations.net/meta/api/v1/metadata/omid:{omid}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch metadata: {response.status_code}")
    metadata = response.json()

    if not metadata:
        raise Exception("No metadata found for the given OMID")

    doi_field = metadata[0].get("doi", "")
    if not doi_field:
        raise Exception("DOI not found in the metadata")

    return doi_field

def get_citing_entities(omid, 
                        sparql_url="https://opencitations.net/index/sparql"
                        ):
    """
    Runs a SPARQL query to find citations for a given OMID.

    Args:
        omid (str): The OpenCitations Metadata Identifier in the format 'br/<number>'.
        sparql_url (str): The URL of the SPARQL endpoint. Defaults to the OpenCitations SPARQL endpoint.

    Returns:
        list: A list of strings representing the citing entities in the format '<prefix>/<suffix>'.

    Raises:
        Exception: If the request fails.
    """
    sparql_query = f"""
    PREFIX cito:<http://purl.org/spar/cito/>
    SELECT ?citation ?citing_entity WHERE {{
        ?citation a cito:Citation .
        ?citation cito:hasCitingEntity ?citing_entity .
        ?citation cito:hasCitedEntity <https://w3id.org/oc/meta/{omid}>
    }}
    """

    headers = {
        "Content-Type": "application/sparql-query",
        "Accept": "application/sparql-results+json"
    }
    response = requests.post(sparql_url, data=sparql_query.encode('utf-8'), headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to run SPARQL query: {response.status_code}")

    results = response.json()
    return ["/".join(i["citing_entity"]["value"].split("/")[-2:]) for i in results["results"]["bindings"]]

def process_document_to_dict(docfile):
    """
    Process a document and convert it to markdown text with bibliography references.
    After processing, it finds the DOI for the references from the bibliography.

    :param docfile: A string representing the file path of the document.
    """
    converter = DocumentConverter()

    md_text = converter.convert(docfile).document.export_to_markdown()

    # Try to split the markdown text into body and bibliography, if it fails, just return the markdown text
    textsplit = re.split(r'##+\s*(?:\*\*)?\s*references\s*(?:\*\*)?\s*[\n\r]*', md_text, flags=re.IGNORECASE)

    # Get the doi from the text body if it exists
    doiregex = r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+"
    matchobject = re.search(doiregex, textsplit[0]) # Search for DOI in the text body
    if matchobject:
        doi = matchobject.group(0)
        title = get_info_from_doi(doi)["title"]
    else:
        doi = None
        title = None

    # Processing the bibliography
    if len(textsplit) != 2:
        return {"body": textsplit[0], "title": title, "doi": doi}
    else:
        paper_body, bibliography = textsplit

        # Sometimes the bibliography can have sections after it so if there is, just get the bibliography
        # and then merge back the other sections into the paper_body
        separator_location = re.search(r'##+\s*(?:\*\*)?\s*[a-zA-Z\s]+(?:\*\*)?\s*[\n\r]*', bibliography, flags=re.IGNORECASE)
        if separator_location:
            separator_location = separator_location.span()[0]
            
            paper_body = paper_body + "\n\n" + bibliography[separator_location:]
            bibliography = bibliography[:separator_location]


        # Convert the bibliography string to individual ref strings
        # Regular expression to match a reference block
        pattern = r'([A-Z][a-zA-Z]*[^\n]*\s*\(\d{4}\).*?)(?=\n[A-Z][a-zA-Z]*[^\n]*\s*\(\d{4}\)|\Z)'
        
        # Use re.DOTALL to allow '.' to match newlines
        references = re.findall(pattern, bibliography.encode('utf-8').decode('utf-8'), re.DOTALL)
        
        # Clean up any extra whitespace or newlines
        references = [ref.strip() for ref in references if ref.strip()]
        

        # TODO: do a similarity search for a query on the references itself before sending it to the crossref api in case I only need the references related to a particular query

        refdois = []
        for ref in references:
            time.sleep(CROSSREF_API_WAIT) # Crossref api grace period
            refdois.append(get_article_info_from_title(ref))
        
        # Combine the paper body and the bibliography with DOIs
        return {"title" : title, "doi" : doi, "body": paper_body, "bibliography": refdois, }
    
def process_article(article):

    # Check if the article is a link to a file
    if os.path.isfile(article):
        return process_document_to_dict(article)
    elif article.startswith("http"):
        pass
        
    else:
        raise ValueError("Unsupported article format")

if __name__ == "__main__":
    # Example usage
    texfile="Att_interactions.bib"
    tex_dicts = parse_bibtex(texfile)
    print(tex_dicts)
