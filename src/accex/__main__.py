from . import process
import logging, asyncio

async def main():
    
    await process.main()

def init():
    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        asyncio.run(main())

init()