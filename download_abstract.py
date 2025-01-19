import json
import time
from typing import Dict, Optional, List
import httpx
from selectolax.parser import HTMLParser
from pathlib import Path
import logging
import random
from tenacity import retry, stop_after_attempt, wait_exponential
import argparse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

_existing_ids = set()


class AbstractDownloader:
    def __init__(self, input_file: str):
        """
        Initialize the abstract downloader

        Args:
            input_file: Path to the input JSON file containing paper information
        """
        self.input_file = input_file
        self.result_dict = {}
        self.failed_papers = []  # Store failed papers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def download_abstract(self, paper: Dict, is_retry: bool = False) -> Optional[Dict]:
        """
        Download and parse abstract for a single paper with retry logic

        Args:
            paper: Paper information dictionary
            is_retry: Whether this is a retry attempt for a previously failed paper
        """
        try:
            # Longer delay between requests (5-10 seconds)
            # Extra delay (15-20 seconds) if retrying a failed paper
            if is_retry:
                time.sleep(random.uniform(30, 45))
            else:
                time.sleep(random.uniform(45, 50))

            with httpx.Client(headers=self.headers, timeout=30.0) as client:
                response = client.get(paper["url"])
                response.raise_for_status()

                parser = HTMLParser(response.text)
                abstract_div = parser.css_first("div.abstract-text")

                if abstract_div and abstract_div.css_first("p"):
                    abstract_text = abstract_div.css_first("p").text().strip()

                    # Extract abstract_id from URL
                    abstract_id = paper["url"].split("abstract_id=")[-1].split("&")[0]

                    return {
                        "abstract_id": abstract_id,
                        "title": paper["title"],
                        "url": paper["url"],
                        "abstract": abstract_text,
                    }

                logging.warning(f"No abstract found for paper: {paper['title']}")
                return None

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logging.error(f"Rate limit exceeded for {paper['title']}")
                # Add to failed papers list if not already retrying
                if not is_retry:
                    self.failed_papers.append(paper)
                # Extra long delay on rate limit
                time.sleep(random.uniform(30, 40))
            raise
        except Exception as e:
            logging.error(f"Error downloading abstract for {paper['title']}: {str(e)}")
            if not is_retry:
                self.failed_papers.append(paper)
            raise

    def save_results(self) -> None:
        """Save results to JSON file"""
        output_file = Path(self.input_file).stem + "_with_abstracts.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.result_dict, f, ensure_ascii=False, indent=2)
        logging.info(f"Results saved to {output_file}")

    def save_failed_papers(self) -> None:
        """Save failed papers list to JSON file"""
        if self.failed_papers:
            failed_file = Path(self.input_file).stem + "_failed_papers.json"
            with open(failed_file, "w", encoding="utf-8") as f:
                json.dump(self.failed_papers, f, ensure_ascii=False, indent=2)
            logging.info(f"Failed papers list saved to {failed_file}")
            logging.info(f"Total failed papers: {len(self.failed_papers)}")

    def retry_failed_papers(self) -> None:
        """Retry downloading abstracts for failed papers"""
        if not self.failed_papers:
            return

        logging.info(f"Retrying {len(self.failed_papers)} failed papers...")
        retry_papers = self.failed_papers.copy()
        self.failed_papers.clear()

        for paper in retry_papers:
            try:
                if paper["abstract_id"] in _existing_ids:
                    logging.info(f"Skipping existing paper: {paper['title']}")
                    continue
                logging.info(f"Retrying paper: {paper['title']}")
                result = self.download_abstract(paper, is_retry=True)
                if result:
                    self.result_dict[result["abstract_id"]] = result
                    self.save_results()
            except Exception as e:
                logging.error(f"Retry failed for paper: {paper['title']}")
                self.failed_papers.append(paper)

            # Extra delay between retries
            time.sleep(random.uniform(10, 15))

    def run(self) -> Dict:
        """
        Run the abstract downloading process

        Returns:
            Dictionary containing all downloaded abstracts
        """
        # Load papers from input file
        with open(self.input_file, "r", encoding="utf-8") as f:
            papers = json.load(f)

        total_papers = len(papers)
        logging.info(f"Starting to process {total_papers} papers")

        # Process each paper
        for i, paper in enumerate(papers, 1):
            if "url" not in paper or "title" not in paper:
                continue

            try:
                result = self.download_abstract(paper)
                if result:
                    self.result_dict[result["abstract_id"]] = result

                    # Save intermediate results every 5 papers
                    if len(self.result_dict) % 5 == 0:
                        self.save_results()

                logging.info(f"Processed {i}/{total_papers} papers")

            except Exception as e:
                logging.error(
                    f"Failed to process paper after retries: {paper['title']}"
                )
                # Save results on error to preserve progress
                self.save_results()

        # Try to download failed papers
        self.retry_failed_papers()

        # Save final results and failed papers list
        self.save_results()
        self.save_failed_papers()

        return self.result_dict


def download_abstracts(input_file: str) -> Dict:
    """
    Download abstracts for papers in the input file

    Args:
        input_file: Path to JSON file containing paper information

    Returns:
        Dictionary containing downloaded abstracts
    """
    downloader = AbstractDownloader(input_file)
    return downloader.run()


# Usage example
if __name__ == "__main__":
    # Create argument parser
    parser = argparse.ArgumentParser(
        description="Download abstracts from SSRN papers",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Add arguments
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to input JSON file containing paper information\n"
        "Example: ssrn_papers_jel_J14_20250117_175715.json",
    )

    # optional arguments - resume
    parser.add_argument(
        "--resume",
        "-r",
        type=str,
        help="Path to previous results file to resume downloading\n"
        "Example: ssrn_papers_jel_J14_20250117_175715_with_abstracts.json",
    )

    # Parse arguments
    args = parser.parse_args()

    # Check if file exists
    if not Path(args.input_file).exists():
        print(f"Error: Input file '{args.input_file}' not found!")
        parser.print_help()
        exit(1)

    # Load existing IDs from resume file if specified
    _existing_ids = set()
    if args.resume:
        try:
            with open(args.resume, "r", encoding="utf-8") as f:
                _existing_ids = set(json.load(f).keys())
        except FileNotFoundError:
            print(f"Error: Resume file '{args.resume}' not found!")
            parser.print_help()
            exit(1)

    # Run downloader
    try:
        results = download_abstracts(args.input_file)
        print(f"\nDownload completed:")
        print(f"- Successfully downloaded {len(results)} abstracts")
        print(f"- Results saved to: {Path(args.input_file).stem}_with_abstracts.json")
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        parser.print_help()
