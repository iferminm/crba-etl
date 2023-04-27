import pytest
import json
import os
import filecmp
from inspect import signature
from importlib.resources import path as res_path

from etl.conf import Config
from etl.indicators.crin import s_131


@pytest.fixture
def config(tmp_path):

    config_json = {}

    p = tmp_path / "config.json"
    p.write_text(json.dumps(config_json))

    conf = Config( **{'config_path': tmp_path, 'build_indicators': ''})
    conf.bootstrap()
    return conf

def test_s_131(config,requests_mock):
    endpoint = signature(s_131).parameters['endpoint'].default
    with open("tests/resources/data_in/archive.crin.org_sites_default_files_access_to_justice_data.xls") as f:
        requests_mock.get('http://test.com', text=f.read)


    s_131(config=config)
    output = config.output_dir / "raw" / 's_131.csv'


    assert os.listdir(config.output_dir / "raw") == ['s_131.csv']
    assert filecmp.cmp(output, "tests/resources/data_out/raw/s_131.csv")

    