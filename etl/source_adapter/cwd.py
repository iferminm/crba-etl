import requests
import pandas as pd

from etl.source_adapter import SourceAdapter, ManualTransformer


class Climate_Watch_Data_Paris_Contributions(SourceAdapter):

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        """

        Returns:
            _type_: _description_

        Yields:
            _type_: _description_
        """

        def pagination_loop():
            """Structure of link next in response header
            link: <https://www.climatewatchdata.org/api/v1/data/ndc_content?category_ids%5B%5D=29884&indicator_ids%5B%5D=286571&page=4>; rel="last", <https://www.climatewatchdata.org/api/v1/data/ndc_content?category_ids%5B%5D=29884&indicator_ids%5B%5D=286571&page=2>; rel="next"'
            """
            session = requests.Session()
            next_adress = self.endpoint
            while next_adress:
                page = session.get(next_adress)
                yield pd.json_normalize(page.json()["data"])
                next_adress = {
                    link_rel.split(";")[1]
                    .split("=")[1]
                    .replace('"', ""): link_rel.split(";")[0]
                    .strip()
                    .removeprefix("<")
                    .removesuffix(">")
                    for link_rel in page.headers["link"].split(",")
                }.get("next", None)

        # This APIis paginated. Therefore we need a little loop to get all the data
        self.dataframe = pd.concat(pagination_loop())

        # The api provieds us with "Date of ratification" and "Ratified". Rowwise!
        # Unfortunally if a conutry has not signed there is no entry for "Date of ratification"
        # Theefore a combination of this two informations happend here.
        self.dataframe = self.dataframe.pivot(
            index=[
                "source",
                "iso_code3",
                "country",
                "global_category",
                "overview_category",
                "sector",
                "subsector",
            ],
            columns="indicator_name",
            values="value",
        )
        self.dataframe["value"] = self.dataframe[
            ["Date of ratification", "Ratified"]
        ].apply(lambda x: x[0] if x[1] == "Yes" else None, axis=1)

        self.dataframe = self.dataframe.reset_index()

        # If the ratification is before the crba_release_year
        crba_release_year = pd.to_datetime(str(self.config.get("CRBA_RELEASE_YEAR")))
        self.dataframe["value"] = pd.to_datetime(self.dataframe["value"]).apply(
            lambda sign_date: "Yes" if sign_date < crba_release_year else "No"
        )
        self.dataframe["TIME_PERIOD"] = self.config.get("CRBA_RELEASE_YEAR")

        return self.dataframe