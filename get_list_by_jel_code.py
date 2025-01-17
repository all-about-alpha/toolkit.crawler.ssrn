import httpx
from selectolax.parser import HTMLParser
from typing import List, Dict
import time
from urllib.parse import urljoin
import json
from datetime import datetime


# J14 code is for finance
def get_list_by_jel_code(jel_code: str = "J14", max_pages: int = None) -> List[Dict]:
    """
    Fetch paper list by JEL classification code

    Args:
        jel_code: JEL classification code (e.g., 'J14')
        max_pages: Maximum number of pages to fetch, None for all pages

    Returns:
        List of dictionaries containing paper information
    """
    base_url = "https://papers.ssrn.com/sol3/jweljour_results.cfm"
    papers = []
    page = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    with httpx.Client(headers=headers, timeout=30.0) as client:
        while True:
            if max_pages and page > max_pages:
                break

            params = {"code": jel_code, "page": page}

            try:
                print(f"Fetching page {page}...")
                response = client.get(base_url, params=params)
                response.raise_for_status()

                # Debug: Print response status and content length
                print(f"Response status: {response.status_code}")
                print(f"Content length: {len(response.text)} bytes")

                parser = HTMLParser(response.text)

                # Get total pages information
                if page == 1:
                    pagination = parser.css_first("div.pagination")
                    if pagination:
                        total_pages_elem = pagination.css_first("li.total")
                        if total_pages_elem:
                            total_pages = int(total_pages_elem.text().strip())
                            print(f"\nTotal pages found: {total_pages}")
                            if max_pages is None:
                                max_pages = total_pages

                paper_elements = parser.css("div.trow")

                # Debug: Print progress
                current_papers = len(paper_elements)
                total_processed = len(papers) + current_papers
                print(
                    f"\nProcessing page {page}/{max_pages} - Found {current_papers} papers on this page"
                )
                print(f"Total papers processed so far: {total_processed}")

                if not paper_elements:
                    print("No more papers found. HTML content sample:")
                    print(response.text[:500])
                    break

                for element in paper_elements:
                    paper = {}

                    # Get paper ID from div attributes
                    paper_id = element.attributes.get("id", "").replace("div_", "")
                    if paper_id:
                        paper["paper_id"] = paper_id
                        print(f"\nProcessing paper ID: {paper_id}")

                    description = element.css_first("div.description")
                    if description:
                        # Get title and link
                        title_elem = description.css_first("a.title.optClickTitle")
                        if title_elem:
                            paper["title"] = title_elem.text().strip()
                            paper["url"] = urljoin(
                                base_url, title_elem.attributes.get("href", "")
                            )
                            print(f"\nFound paper: {paper['title']}")

                        # Get paper details from note-list
                        note_list = description.css_first("div.note.note-list")
                        if note_list:
                            spans = note_list.css("span")
                            for span in spans:
                                text = span.text().strip()
                                if "Number of pages:" in text:
                                    paper["pages"] = text.replace(
                                        "Number of pages:", ""
                                    ).strip()
                                elif "Posted:" in text:
                                    paper["posted_date"] = text.replace(
                                        "Posted:", ""
                                    ).strip()
                                elif "Last Revised:" in text:
                                    paper["last_revised"] = text.replace(
                                        "Last Revised:", ""
                                    ).strip()
                                print(f"Detail found: {text}")

                        # Get authors with affiliations
                        authors = []
                        author_elements = description.css("div.authors-list a")
                        affiliation_elements = description.css("div.afiliations")

                        for idx, author in enumerate(author_elements):
                            author_info = {
                                "name": author.text().strip(),
                                "profile_url": urljoin(
                                    base_url, author.attributes.get("href", "")
                                ),
                            }
                            if idx < len(affiliation_elements):
                                author_info["affiliation"] = (
                                    affiliation_elements[idx].text().strip()
                                )
                            authors.append(author_info)
                            print(
                                f"Author found: {author_info['name']} ({author_info.get('affiliation', 'No affiliation')})"
                            )
                        paper["authors"] = authors

                        # Get keywords if available
                        keywords_div = description.css_first("div.keywords")
                        if keywords_div:
                            paper["keywords"] = keywords_div.text().strip()

                    # Get downloads count
                    downloads_div = element.css_first("div.downloads")
                    if downloads_div:
                        downloads_count = downloads_div.css_first("span:nth-child(2)")
                        if downloads_count:
                            paper["downloads"] = downloads_count.text().strip()
                            print(f"Downloads: {paper['downloads']}")

                    papers.append(paper)
                    print(f"Progress: {len(papers)} papers collected")

                # Check if we've reached the max pages
                if max_pages and page >= max_pages:
                    print(f"\nReached maximum pages limit ({max_pages})")
                    break

                page += 1
                time.sleep(1)  # Rate limiting

            except httpx.HTTPError as e:
                print(f"Error fetching page {page}: {str(e)}")
                break

    # Final statistics
    print(f"\nScraping completed:")
    print(f"Total pages processed: {page-1}")
    print(f"Total papers collected: {len(papers)}")

    # Save results to JSON file with error handling
    output_file = (
        f"ssrn_papers_jel_{jel_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(papers, f, ensure_ascii=False, indent=2)
        print(f"\nResults successfully saved to {output_file}")
    except Exception as e:
        print(f"\nError saving to JSON: {str(e)}")
        # Try to save with basic ASCII encoding if Unicode fails
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(papers, f, ensure_ascii=True, indent=2)
            print(f"Results saved with ASCII encoding to {output_file}")
        except Exception as e:
            print(f"Failed to save even with ASCII encoding: {str(e)}")

    return papers


# Usage example
if __name__ == "__main__":
    results = get_list_by_jel_code()
    print(f"\nScript completed. Total papers collected: {len(results)}")
