from ai_browser.browser import get_data_from_source
import pandas as pd
import asyncio
from ai_browser.utils.config_loading import config


async def extract_data_from_sources() -> dict[str, pd.DataFrame]:
    data = {}
    for source in config.sources:
        data = await get_data_from_source(source)
        data[source.source_id] = data
    return data


async def main() -> dict[str, pd.DataFrame]:
    extracted_data = await extract_data_from_sources()
    return extracted_data



if __name__ == '__main__':
    data = asyncio.run(main())
    print("Goodbye")