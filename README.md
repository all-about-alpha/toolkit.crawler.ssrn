# Download Abstracts from SSRN

This script downloads abstracts from SSRN for a list of papers.

## Usage

```bash
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python get_list_of_papers.py # get list of papers from SSRN
python download_abstract.py # download abstracts from SSRN
```

## Notes

When downloading abstracts, the script will intentionally sleep for a random amount of time between 45 and 50 seconds between requests. This is to avoid being rate limited by SSRN.
