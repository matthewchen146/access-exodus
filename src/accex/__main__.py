from . import process
import logging, asyncio

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(process.main())
