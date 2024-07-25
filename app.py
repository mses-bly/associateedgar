
from readers.SECEdgarReader import SECEdgarReader
import os


os.environ["SEC_API_ORGANIZATION"] = "Baly, LLC"
os.environ["SEC_API_EMAIL"] = "moises.baly@gmail.com"

if __name__ == '__main__':
    reader = SECEdgarReader()
    metadata = {'ticker': 'SSYS', 'form_type': '6-K', 'k_forms': 10}
    documents = reader.load_data(**metadata)
    print(len(documents))
    for document in documents:
        print(document.metadata)
