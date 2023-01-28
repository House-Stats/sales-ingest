from ingest import Ingest
import asyncio

if __name__ == "__main__":
    ingester = Ingest()
    asyncio.run(ingester.main_loop())
