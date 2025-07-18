import os
import re
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from etl.source_adapter import ManualTransformer
from etl.source_adapter import SourceAdapter


class NRGI(SourceAdapter):
    """
    TODO: not multithreading capable
    """

    def __init__(
            self, config, **kwarg
    ):
        prefs = {"download.default_directory": str(config.raw_output.resolve())}

        options = Options()
        options.add_argument("--headless=new")  # 👈 new headless mode (use this!)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        self.driver = webdriver.Chrome(options=options)
        self.nrgi_sector = kwarg.get("NRGI_SECTOR")
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    @staticmethod
    def every_downloads_chrome(driver):
        """
        Fromh https://stackoverflow.com/questions/48263317/selenium-python-waiting-for-a-download-process-to-complete-using-chrome-web
        Returns:

        """
        if not driver.current_url.startswith("chrome://downloads"):
            driver.get("chrome://downloads/")
        WebDriverWait(driver, 10).until(
            expected_conditions.visibility_of_element_located((By.XPATH, "x_path_table")))
        return driver.execute_script("""
            var items = document.querySelector('downloads-manager')
                .shadowRoot.getElementById('downloadsList').items;
            if (items.every(e => e.state === "COMPLETE"))
                return items.map(e => e.fileUrl || e.file_url);
            """)

    def _download(self):
        x_path_table = "//ul[@class='ranking-list']"
        x_path_download_button = "//button[@data-type='download']"
        if not (self.config.raw_output / f"nrgi_data_{self.nrgi_sector}.csv").is_file():
            self.driver.get(self.endpoint)
            WebDriverWait(self.driver, 10).until(
                expected_conditions.visibility_of_element_located((By.XPATH, x_path_table)))

            # This commands clicks the download button. The file is ten donloaded to the prefs download.default_directory
            self.driver.find_element(by=By.XPATH, value=x_path_download_button).click()
            # waits for all the files to be completed and returns the paths
            # TODO not working
            # paths = WebDriverWait(self.driver, 120, 1).until(NRGI.every_downloads_chrome)
            time.sleep(10)
            # Rename Doenlaoded file. Because all NRGI downloaded files called the same
            # Probably not multithread capable
            os.rename(self.config.raw_output / "nrgi_data.csv",
                      self.config.raw_output / f"nrgi_data_{self.nrgi_sector}.csv")

        # Clear data. Schema ['name', 'sector', 'region', 'Composite/component', 'Subcomponent','Indicator', 'Score2017', 'Score2021']
        self.dataframe = pd.read_csv(self.config.raw_output / f"nrgi_data_{self.nrgi_sector}.csv")
        # Only Select Columns 'name', 'region' and Score columns
        self.dataframe = self.dataframe.filter(regex=("name|sector|Score\d{4}"))
        # Rename Score Columns from Score2017 --> 2017
        self.dataframe = self.dataframe.rename(columns=lambda x: re.sub("Score", "", x))
        # Make from ScoreXXXX Columns rows. The new Column to distinguish is called TIME_PERIOD
        self.dataframe = self.dataframe.melt(id_vars=["name","sector"],
                                             var_name="TIME_PERIOD",
                                             value_name="RAW_OBS_VALUE")

        self.dataframe = self.dataframe.rename(
            columns={"name": "COUNTRY_NAME", "sector": "DIM_SECTOR"})

        return self.dataframe
