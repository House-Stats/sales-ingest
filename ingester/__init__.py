from ingest import Ingest
import asyncio
import atexit

import sentry_sdk

if __name__ == "__main__":
    sentry_sdk.init(
        dsn="https://aadfc763299c4541b15f33e414c395d4@o4504585220980736.ingest.sentry.io/4504649935945728",
        traces_sample_rate=1.0
    )

    ingester = Ingest()
    atexit.register(asyncio.run, ingester.remove_status())
    asyncio.run(ingester.main_loop())
