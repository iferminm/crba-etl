import pandas as pd
import json
from browser_use import Agent, ChatOpenAI, Controller
from browser_use.llm import ChatOpenAI
from pathlib import Path
from ai_browser.config_model import Source
from ai_browser.output_models import OutputMOdel_1
from ai_browser.utils.config_loading import config


async def get_data_from_source(source: Source) -> pd.DataFrame:
    return await scrape_source(source)


async def scrape_source(source: Source) -> pd.DataFrame:
    task = _get_task_insructions(source)
    model = _get_model()
    controller = _get_controller()
    agent = Agent(task=task, llm=model, controller=controller)
    history = await agent.run()
    file = Path(OutputMOdel_1(**json.loads(history.final_result())).csv_file_path)
    return pd.read_csv(file)


def _get_task_insructions(source: Source) -> str:
    instructions_list = []
    instructions_list.append(f"navigate the url {source.start_url} then follow the instructions:")
    instructions_list.extend(source.instructions)
    instructions = "\n".join(instructions_list)
    return instructions


def _get_model() -> ChatOpenAI:
    return ChatOpenAI(model=config.browser.model, temperature=config.browser.temperature)


def _get_controller() -> Controller:

    return Controller(output_model=OutputMOdel_1)
