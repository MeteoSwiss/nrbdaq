# /tests/tests.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
import polars as pl

# Project imports
import nrbdaq.instr.avo as avo
from nrbdaq.instr.thermo import Thermo49i
from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import load_config


# -------------------------
# Unit fixtures (self-contained)
# -------------------------

@pytest.fixture(scope="session")
def config() -> dict[str, Any]:
    """Load the project config once per test session."""
    return load_config(config_file="nrbdaq.yml")


@pytest.fixture
def tmp_text_file(tmp_path: Path) -> Path:
    """Create a temporary text file with known content."""
    p = tmp_path / "hello_world.txt"
    p.write_text("Hello, world!", encoding="utf-8")
    return p


@pytest.fixture
def sftp_mock(monkeypatch: pytest.MonkeyPatch):
    """Mock SFTPClient fully for fast, isolated unit tests."""
    class _RemoteFS:
        def __init__(self):
            self.files: set[str] = set()

    remote_fs = _RemoteFS()
    remote_base = "sftp://bucket/base"

    class MockSFTP:
        def __init__(self, *_, **__):
            self._remote_path = remote_base

        @property
        def remote_path(self) -> str:
            return self._remote_path

        def is_alive(self) -> bool:
            return True

        def remote_item_exists(self, remote_path: str) -> bool:
            return remote_path in remote_fs.files

        def remove_remote_item(self, remote_path: str) -> None:
            remote_fs.files.discard(remote_path)

        def put_file(self, local_path: str | Path, remote_path: str):
            dst = os.path.join(remote_path, os.path.basename(str(local_path)))
            remote_fs.files.add(dst)
            return {"path": dst, "size": Path(local_path).stat().st_size}

        def list_local_files(self, local_path: str | Path) -> list[str]:
            lp = Path(local_path)
            return [str(p) for p in lp.glob("*") if p.is_file()]

        def transfer_files(
            self, local_path: str | Path, remote_path: str, remove_on_success: bool = False
        ) -> bool:
            for f in self.list_local_files(local_path):
                dst = os.path.join(remote_path, os.path.basename(f))
                remote_fs.files.add(dst)
                if remove_on_success:
                    Path(f).unlink(missing_ok=True)
            return True

    monkeypatch.setattr("nrbdaq.utils.sftp.SFTPClient", MockSFTP)
    return MockSFTP


@pytest.fixture
def avo_sample(monkeypatch: pytest.MonkeyPatch):
    """Stub AVO functions for deterministic unit tests."""
    sample_data = {
        "historical": [{"date": "2024-01-01", "value": 1}],
        "name": "kmd_hq_nairobi",
        "current": {"date": "2024-12-31", "value": 2},
    }

    def fake_download_data(*_, **__):
        return sample_data

    def fake_data_to_dfs(*, data, file_path, staging):
        return data["name"], {
            "historical": pl.DataFrame(data["historical"]),
            "current": pl.DataFrame([data["current"]]),
        }

    monkeypatch.setattr(avo, "download_data", fake_download_data)
    monkeypatch.setattr(avo, "data_to_dfs", fake_data_to_dfs)
    return sample_data


@pytest.fixture
def thermo49i_instance(config: dict[str, Any]) -> Thermo49i:
    return Thermo49i(config=config)


# -------------------------
# Unit tests
# -------------------------

def test_sftp_is_alive(sftp_mock):
    sftp = SFTPClient(config={"dummy": True})
    assert sftp.is_alive() is True


def test_sftp_transfer_single_file(tmp_text_file: Path, sftp_mock):
    sftp = SFTPClient(config={"dummy": True})
    remote_base = sftp.remote_path
    remote_path = os.path.join(remote_base, os.path.basename(tmp_text_file))

    if sftp.remote_item_exists(remote_path):
        sftp.remove_remote_item(remote_path)

    sftp.put_file(local_path=tmp_text_file, remote_path=remote_base)
    assert sftp.remote_item_exists(remote_path) is True


def test_fidas_transfer_staged_files(tmp_path: Path, sftp_mock, config: dict[str, Any]):
    staging_dir = tmp_path / "staging" / "fidas"
    staging_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for i in range(3):
        p = staging_dir / f"dummy_{i}.txt"
        p.write_text(f"file {i}", encoding="utf-8")
        files.append(p)

    sftp = SFTPClient(config={"dummy": True})
    remote_full = os.path.join(
        sftp.remote_path,
        config.get("fidas", {}).get("remote_path", "remote/fidas"),
    )
    sftp.transfer_files(local_path=staging_dir, remote_path=remote_full, remove_on_success=False)

    assert sftp.remote_item_exists(os.path.join(remote_full, os.path.basename(files[0]))) is True
    assert sftp.remote_item_exists(os.path.join(remote_full, os.path.basename(files[-1]))) is True


def test_avo_download_and_to_dfs(avo_sample, config: dict[str, Any]):
    data = avo.download_data(url=config["AVO"]["urls"]["url_nairobi"])
    assert list(data.keys()) == ["historical", "name", "current"]

    station, dfs = avo.data_to_dfs(
        data=data,
        file_path=os.path.join(os.path.expanduser(config["root"]), config["AVO"]["data"]),
        staging=os.path.join(os.path.expanduser(config["root"]), config["AVO"]["staging"]),
    )

    assert station == "kmd_hq_nairobi"
    assert isinstance(dfs["historical"], pl.DataFrame)
    assert isinstance(dfs["current"], pl.DataFrame)


def test_thermo49i_init(thermo49i_instance: Thermo49i):
    assert thermo49i_instance._data == str()


# import os
# import unittest
# from pathlib import Path

# import polars as pl

# import nrbdaq.instr.avo as avo
# from nrbdaq.instr.ae31 import AE31
# from nrbdaq.instr.fidas import FIDAS
# from nrbdaq.instr.thermo import Thermo49i
# from nrbdaq.utils.sftp import SFTPClient
# from nrbdaq.utils.utils import load_config

# config = load_config(config_file="nrbdaq.yml")

# class TestSFTP(unittest.TestCase):
#     def test_config_host(self):
#         self.assertEqual(config['sftp']['host'], 'sftp.meteoswiss.ch')

#     def test_is_alive(self):
#         sftp = SFTPClient(config=config)

#         self.assertEqual(sftp.is_alive(), True)

#     def test_transfer_single_file(self):
#         sftp = SFTPClient(config=config)

#         # setup
#         file_path = 'nrbdaq/tests/hello_world.txt'
#         file_content = 'Hello, world!'
#         os.makedirs(os.path.dirname(file_path), exist_ok=True)
#         with open(file_path, 'w') as fh:
#             fh.write(file_content)
#             fh.close()

#         remotepath = sftp.remote_path
#         remote_path = os.path.join(remotepath, os.path.basename(file_path))
#         if sftp.remote_item_exists(remote_path=remote_path):
#             sftp.remove_remote_item(remote_path=remote_path)

#         attr = sftp.put_file(local_path=file_path, remote_path=remotepath)

#         self.assertEqual(sftp.remote_item_exists(remote_path=remote_path), True)

#         # clean up
#         sftp.remove_remote_item(remote_path=remote_path)
#         os.remove(path=file_path)


# class TestAVO(unittest.TestCase):
#     def test_download_data(self):
#         data = avo.download_data(url=config['AVO']['urls']['url_nairobi'])
#         self.assertEqual(list(data.keys()), ['historical', 'name', 'current'])

#     def test_data_to_dfs(self):
#         data = avo.download_data(url=config['AVO']['urls']['url_nairobi'])
#         station, dfs = avo.data_to_dfs(data=data,
#                               file_path=os.path.join(os.path.expanduser(config['root']), config['AVO']['data']),
#                               staging=os.path.join(os.path.expanduser(config['root']), config['AVO']['staging']))
#         self.assertEqual(station, 'kmd_hq_nairobi')

# class TestAE31(unittest.TestCase):
#     def test_validate_ae31_csv_file(self):
#         ae31 = AE31(config=config)
#         valid_file = 'nrbdaq/tests/data/ae31/AE31_20240825.csv'
#         df_valid = ae31.csv_to_df(file=valid_file)

#         test_file = 'nrbdaq/tests/data/ae31/AE31_20240805.csv'
#         df_test = ae31.csv_to_df(file=test_file)

#         self.assertEqual(df_valid.schema, df_test.schema)

# class TestThermo49i(unittest.TestCase):
#     def test_init(self):
#         thermo49i = Thermo49i(config=config)

#         self.assertEqual(thermo49i._data, str())

# class TestFidas(unittest.TestCase):
#     def test_transfer_staged_files(self, name="fidas"):
#         sftp = SFTPClient(config=config)

#         fidas_staging_path = Path(config['root']).expanduser() / config['staging'] / config[name]['staging_path']
#         fidas_remote_path = config[name]['remote_path']

#         remote_path = os.path.join(sftp.remote_path, fidas_remote_path)
#         local_file_paths = sftp.list_local_files(fidas_staging_path)
#         sftp.transfer_files(local_path=fidas_staging_path,
#                             remote_path=remote_path,
#                             remove_on_success=False)
#         self.assertEqual(sftp.remote_item_exists(local_file_paths[0]), True)
#         self.assertEqual(sftp.remote_item_exists(local_file_paths[-1]), True)


# if __name__ == "__main__":
#     unittest.main(verbosity=2)
