from ingest import Ingest
import asyncio
import sentry_sdk

if __name__ == "__main__":
    sentry_sdk.init(
        dsn="https://429d564121d14b3fa3ef61eba553f8b6@sentry.housestats.co.uk/1",

        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0
    )
    ingester = Ingest()
    asyncio.run(ingester.main_loop())
