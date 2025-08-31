"""Map upload unit tests"""
import asyncio
import configparser
import os
from pathlib import Path
from yarl import URL
from multidict import CIMultiDict, CIMultiDictProxy
from pytest import mark
from fixtures import temp_dir_fixture  # pylint: disable=W0611
from mapupload import DownloadFileResponse, _compress_to_bz2, _escape_everything, MapUploadConfig, \
    _extract_file, _get_safe_filename_no_ext, _get_map_files, _insert_mapcycle, _is_bsp_file, \
    _is_bz2_file, _map_exists_in_mapcycle, _get_filename_from_headers, _get_filename_from_url, \
    _remove_mapcycle
from run import _config_parse
from util import removefile_unchecked


CUR_DIR = os.path.abspath(os.path.dirname(__file__))

dispositions = [
    'attachment; filename=name.bsp',
    'attachment; filename="name.bsp"',
    'attachment; filename="../name.bsp"'
]

urls = [
    'https://example.com/name.bsp',
    'https://example.com/name.bsp.bz2',
    'http://example.com/name.bsp.bz2',
    'http://example.com/name.bsp.bz2&dl=1',
    'http://example.com/name.bsp.bz2#asdf',
    'http://127.0.0.1/name.bsp.bz2#asdf',
]


def test_escape_everything():
    """Escape for Discord messages."""
    assert '\\`hello\\`' == _escape_everything('`hello`')
    assert '@\u200beveryone' == _escape_everything('@everyone')


def test_get_safe_filename_no_ext():
    """Get filename with no extention"""
    assert 'filename' == _get_safe_filename_no_ext('filename.bsp')
    assert 'filename' == _get_safe_filename_no_ext('filename.bsp.bz2')
    assert 'filename' == _get_safe_filename_no_ext('../filename.bsp.bz2')
    assert 'filename' == _get_safe_filename_no_ext(
        'C:/path/filename.bsp.bz2')
    assert 'filename' == _get_safe_filename_no_ext(
        'C:\\path\\filename.bsp.bz2')


def test_get_map_files():
    """Get map files"""
    files = _get_map_files(MapUploadConfig(), 'mapname', False, False)
    assert 'mapname.bsp' in files
    assert 'mapname.txt' in files
    assert 'mapname.nav' in files


def test_map_exists_in_mapcycle():
    """Map exists in mapcycle"""
    assert _map_exists_in_mapcycle(['a', 'b'], 'a') is True
    assert _map_exists_in_mapcycle(['a', 'b'], 'b') is True
    assert _map_exists_in_mapcycle(['a', 'b'], 'c') is False


@mark.parametrize('value', dispositions)
def test_get_filename_from_headers(value: str):
    """"Filename from Content-Disposition."""
    d = CIMultiDictProxy(
        CIMultiDict({'Content-Disposition': value}))
    assert _get_filename_from_headers(d) == 'name'


@mark.parametrize('value', urls)
def test_get_filename_from_url(value: str):
    """Filename from URL."""
    assert _get_filename_from_url(URL(value)) == 'name'


def test_config_parse():
    """Config mapping from a file."""
    parser = configparser.ConfigParser()
    parser.read_string("""
[server]
mapcyclefile=mapcyclefile
mapsdir=mapsdir
mapcycleregex=mapcycleregex
[upload]
maxbytes=1
[sftp]
hostname=hostname
port=1234
username=username
password=password
mapsdir=remotemapsdir
[bz2]
ignoreregex=ignoreregex
compressionlevel=8
    """)
    config = _config_parse(parser)
    assert config.bz2_compressionlevel == 8
    assert config.bz2_ignore_regex == 'ignoreregex'
    assert config.mapcycle_file == 'mapcyclefile'
    assert config.mapcycle_sort_regex == 'mapcycleregex'
    assert config.maps_dir == 'mapsdir'
    assert config.sftp_hostname == 'hostname'
    assert config.sftp_password == 'password'
    assert config.sftp_port == 1234
    assert config.sftp_username == 'username'
    assert config.sftp_remote_maps_path == 'remotemapsdir'
    assert config.upload_max_bytes == 1


def test_config_parse_defaults():
    """Config defaults."""
    parser = configparser.ConfigParser()
    parser.read_string("""
[server]
mapcyclefile=
mapsdir=
mapcycleregex=
[upload]
maxbytes=
[sftp]
hostname=
port=
username=
password=
mapsdir=
[bz2]
ignoreregex=
compressionlevel=
""")
    config = _config_parse(parser)
    assert config.bz2_compressionlevel == 4
    assert config.bz2_ignore_regex == ''
    assert config.mapcycle_file == ''
    assert config.mapcycle_sort_regex == ''
    assert config.maps_dir == ''
    assert config.sftp_hostname == ''
    assert config.sftp_password == ''
    assert config.sftp_port == 22
    assert config.sftp_username == ''
    assert config.sftp_remote_maps_path == ''
    assert config.upload_max_bytes == 100_000_000


def test_is_bz2_file():
    """Is bzip2 file?"""
    assert _is_bz2_file(os.path.join(
        CUR_DIR, 'testcontent/test.bsp.bz2')) is True
    assert _is_bz2_file(
        os.path.join(CUR_DIR, 'testcontent/test.bsp')) is False
    assert _is_bz2_file(
        os.path.join(CUR_DIR, 'testcontent/test.zip')) is False


def test_is_bsp_file():
    """Is bsp file?"""
    assert _is_bsp_file(
        os.path.join(CUR_DIR, 'testcontent/test.bsp')) is True
    assert _is_bsp_file(os.path.join(
        CUR_DIR, 'testcontent/test.bsp.bz2')) is False
    assert _is_bsp_file(
        os.path.join(CUR_DIR, 'testcontent/test.zip')) is False


def test_insert_into_mapcycle(temp_dir: Path):
    """Insert map to mapcycle."""

    mapcycle_file = os.path.join(temp_dir, 'mapcycle.txt')
    with open(mapcycle_file, 'w', encoding='utf-8') as fp:
        fp.write('a_map\nc_map\n')

    async def run_co():
        loop = asyncio.get_event_loop()
        return await _insert_mapcycle(loop,
                                      MapUploadConfig(mapcycle_file=mapcycle_file,
                                                      mapcycle_sort_regex='(\\w+)'),
                                      'b_map')

    response = asyncio.run(run_co())

    assert response.success is True

    lines: list[str] = []
    with open(mapcycle_file, 'r', encoding='utf-8') as fp:
        lines = fp.readlines()
    assert len(lines) == 3
    assert lines[0] == 'a_map\n'
    assert lines[1] == 'b_map\n'
    assert lines[2] == 'c_map\n'


def test_remove_from_mapcycle(temp_dir: Path):
    """Remove map from mapcycle."""

    mapcycle_file = os.path.join(temp_dir, 'mapcycle.txt')
    with open(mapcycle_file, 'w', encoding='utf-8') as fp:
        fp.write('a_map\nb_map\nc_map\n')

    async def run_co():
        loop = asyncio.get_event_loop()
        return await _remove_mapcycle(loop,
                                      MapUploadConfig(
                                          mapcycle_file=mapcycle_file),
                                      'b_map')

    success = asyncio.run(run_co())

    assert success is True

    lines: list[str] = []
    with open(mapcycle_file, 'r', encoding='utf-8') as fp:
        lines = fp.readlines()
    assert len(lines) == 2
    assert lines[0] == 'a_map\n'
    assert lines[1] == 'c_map\n'


def test_remove_from_mapcycle2(temp_dir: Path):
    """Mapcycle does not exist."""

    mapcycle_file = os.path.join(temp_dir, 'I DONT EXIST.txt')

    async def run_co():
        loop = asyncio.get_event_loop()
        return await _remove_mapcycle(loop,
                                      MapUploadConfig(
                                          mapcycle_file=mapcycle_file),
                                      'b_map')
    success = asyncio.run(run_co())

    assert success is False


def test_extract_file_zip(temp_dir: Path):
    """Extract zip file"""
    download_response = DownloadFileResponse()
    download_response.map_name = 'test'
    download_response.success = True
    download_response.temp_file = os.path.join(
        CUR_DIR, 'testcontent/test.zip')

    response = _extract_file(MapUploadConfig(
        maps_dir=temp_dir), download_response)

    out_filename = os.path.join(temp_dir, 'test.bsp')
    assert response.success is True
    assert out_filename in response.files_extracted
    assert os.path.exists(out_filename) is True


def test_extract_file_bzip(temp_dir: Path):
    """Extract bzip2 file"""
    download_response = DownloadFileResponse()
    download_response.map_name = 'test'
    download_response.success = True
    download_response.temp_file = os.path.join(
        CUR_DIR, 'testcontent/test.bsp.bz2')

    response = _extract_file(MapUploadConfig(
        maps_dir=temp_dir), download_response)

    out_filename = os.path.join(temp_dir, 'test.bsp')
    assert response.success is True
    assert out_filename in response.files_extracted
    assert os.path.exists(out_filename) is True


def test_extract_file_bsp(temp_dir: Path):
    """Extract bsp file"""
    download_response = DownloadFileResponse()
    download_response.map_name = 'test'
    download_response.success = True
    download_response.temp_file = os.path.join(
        CUR_DIR, 'testcontent/test.bsp')

    response = _extract_file(MapUploadConfig(
        maps_dir=temp_dir), download_response)

    out_filename = os.path.join(temp_dir, 'test.bsp')
    assert response.success is True
    assert out_filename in response.files_extracted
    assert os.path.exists(out_filename) is True


def test_compress_to_bz2():
    """Compress a file with bzip2"""
    file_path = os.path.join(CUR_DIR, 'testcontent/test.bsp')
    compressed_files = _compress_to_bz2(
        1, [file_path])
    assert len(compressed_files) == 1
    assert compressed_files[0].real_file == file_path
    assert _is_bz2_file(compressed_files[0].compressed_file) is True
    removefile_unchecked(compressed_files[0].compressed_file)
