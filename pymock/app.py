import random
import uuid
from time import sleep
from datetime import datetime, timezone

import requests


def generate_document():
    return {
        "id": str(uuid.uuid4()),
        "val": random.randint(1, 1_000_000),
        "date": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def main():
    while True:
        # generate random number of documents
        docs = [generate_document() for _ in range(0, random.randint(1, 1000))]
        print(docs[0], "...",  f"({len(docs)} documents)")
        try:
            resp = requests.post("http://localhost:8000/from_json", json=docs, timeout=1.5)
            print(resp.status_code, "->", resp.content)
        except Exception as e:
            pass
        sleep(1)


if __name__ == "__main__":
    main()