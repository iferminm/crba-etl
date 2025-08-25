from pydantic import BaseModel


class BrowserConfig(BaseModel):
    model: str
    temperature: float | None = None


class Source(BaseModel):
    source_id: str
    description: str = ""
    start_url: str
    instructions: list[str]
    output_format: str


class ConfigModel(BaseModel):
    sources: list[Source]
    browser: BrowserConfig
