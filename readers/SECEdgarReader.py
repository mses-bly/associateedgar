import json
import os
from typing import Any, List, Optional, Final, Union
import re
import requests
from llama_index.core import Document
from llama_index.core.readers.base import BaseReader
from ratelimit import sleep_and_retry, limits


class SECEdgarReader(BaseReader):
    FORM_TYPE_ALL = 'FORM_TYPE_ALL'
    SEC_ARCHIVE_URL: Final[str] = "https://www.sec.gov/Archives/edgar/data"
    SEC_SEARCH_URL: Final[str] = "http://www.sec.gov/cgi-bin/browse-edgar"
    SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions"

    def load_data(self, *args: Any, **load_kwargs: Any) -> List[Document]:
        """
            *args needs to contain:
                - 'ticker': company ticker
                - 'form_type': e.g. '6-K', '20-F', '10-K'
                - 'k_forms': how many forms to pull
        """
        if 'ticker' not in load_kwargs:
            raise ValueError(f"Need to provide a company 'ticker'")
        ticker = load_kwargs['ticker']
        form_type = load_kwargs['form_type'] if 'form_type' in load_kwargs else SECEdgarReader.FORM_TYPE_ALL
        k_forms = load_kwargs['k_forms'] if 'k_forms' in load_kwargs else float('inf')

        session = self._get_session()
        cik = self._get_cik_by_ticker(session, ticker)
        forms_descriptor = self._get_forms_by_cik(session, cik)

        base_metadata = {
            'cik': forms_descriptor['cik'],
            'company_name': forms_descriptor['name'],
            'company_ticker': forms_descriptor['tickers'][0],
            'exchange': forms_descriptor['exchanges'][0],
            'fiscal_year_end': forms_descriptor['fiscalYearEnd'],
            'state_of_incorporation': forms_descriptor['stateOfIncorporationDescription'],
        }
        filings = forms_descriptor['filings']['recent']
        n_filings = len(filings['accessionNumber'])

        documents = []

        for i in range(n_filings):
            _form_type = filings['form'][i]
            if  form_type == SECEdgarReader.FORM_TYPE_ALL or _form_type == form_type:
                form_metadata = {
                    'form_type': filings['form'][i],
                    'accession_number': filings['accessionNumber'][i],
                    'filing_date': filings['filingDate'][i],
                    'filing_name': filings['primaryDocument'][i],
                }

                form = self._get_filing(session, cik, self._drop_dashes(form_metadata['accession_number']), form_metadata['filing_name'])
                documents.append(Document(text = form, metadata=base_metadata|form_metadata))
                if len(documents) >= k_forms:
                    break
            else:
                continue

        return documents

    def _get_session(self, company: Optional[str] = None, email: Optional[str] = None) -> requests.Session:
        """Creates a requests sessions with the appropriate headers set. If these headers are not
        set, SEC will reject your request.
        ref: https://www.sec.gov/os/accessing-edgar-data"""
        if company is None:
            company = os.environ.get("SEC_API_ORGANIZATION")
        if email is None:
            email = os.environ.get("SEC_API_EMAIL")
        assert company
        assert email
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": f"{company} {email}",
                "Content-Type": "text/html",
            }
        )
        return session

    @sleep_and_retry
    @limits(calls=10, period=1)
    def _get_cik_by_ticker(self, session: requests.Session, ticker: str) -> str:
        """Gets a CIK number from a stock ticker by running a search on the SEC website."""
        cik_re = re.compile(r".*CIK=(\d{10}).*")
        url = self._search_url(ticker)
        response = session.get(url, stream=True)
        response.raise_for_status()
        results = cik_re.findall(response.text)
        return str(results[0])

    def _search_url(self, cik: Union[str, int]) -> str:
        search_string = f"CIK={cik}&Find=Search&owner=exclude&action=getcompany"
        url = f"{SECEdgarReader.SEC_SEARCH_URL}?{search_string}"
        return url

    @sleep_and_retry
    @limits(calls=10, period=1)
    def _get_forms_by_cik(self, session: requests.Session, cik: Union[str, int]) -> list:
        """Gets retrieves dict of recent SEC form filings for a given cik number."""
        json_name = f"CIK{cik}.json"
        response = session.get(f"{SECEdgarReader.SEC_SUBMISSIONS_URL}/{json_name}")
        response.raise_for_status()
        content = json.loads(response.content)
        return content

    @sleep_and_retry
    @limits(calls=10, period=1)
    def _get_filing(
            self, session: requests.Session, cik: Union[str, int], accession_number: Union[str, int], filename: str
    ) -> str:
        """Wrapped so filings can be retrieved with an existing session."""
        url = self._archive_url(cik, accession_number, filename)
        response = session.get(url)
        response.raise_for_status()
        return response.text

    def _archive_url(self, cik: Union[str, int], accession_number: Union[str, int], filename: str) -> str:
        """Builds the archive URL for the SEC accession number. Looks for the .txt file for the
        filing, while follows a {accession_number}.txt format."""
        #     filename = f"{_add_dashes(accession_number)}.txt"
        accession_number = self._drop_dashes(accession_number)
        print(f"{SECEdgarReader.SEC_ARCHIVE_URL}/{cik}/{accession_number}/{filename}")
        return f"{SECEdgarReader.SEC_ARCHIVE_URL}/{cik}/{accession_number}/{filename}"

    def _add_dashes(self, accession_number: Union[str, int]) -> str:
        """Adds the dashes back into the accession number"""
        accession_number = str(accession_number)
        return f"{accession_number[:10]}-{accession_number[10:12]}-{accession_number[12:]}"

    def _drop_dashes(self, accession_number: Union[str, int]) -> str:
        """Converts the accession number to the no dash representation."""
        accession_number = str(accession_number).replace("-", "")
        return accession_number.zfill(18)
