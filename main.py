# build entry point

import asyncio
from accex.process.core import _main

if __name__ == "__main__":
    asyncio.run(_main())