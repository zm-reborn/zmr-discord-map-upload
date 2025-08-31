"""Map upload "end-to-end" tests"""
import asyncio
import os
from pathlib import Path
import shutil
from fixtures import http_server, sftp_server, temp_dir_fixture  # pylint: disable=W0611
from mapupload import MapUploadConfig, add_map, add_mapcycle, remove_mapcycle


CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def test_add_map(temp_dir: Path, http_server_port: int, sftp_port: int):
    """Successful map add."""
    mapcycle_file = os.path.join(temp_dir, 'mapcycle.txt')
    with open(mapcycle_file, 'w', encoding='utf-8') as fp:
        fp.write('something\n')

    async def run_co():
        loop = asyncio.get_event_loop()
        return await add_map(loop,
                             MapUploadConfig(sftp_hostname='127.0.0.1',
                                             sftp_port=sftp_port,
                                             sftp_username='username',
                                             sftp_password='password',
                                             sftp_remote_maps_path='some_directory/maps',
                                             maps_dir=temp_dir,
                                             mapcycle_file=mapcycle_file),
                             f'http://127.0.0.1:{http_server_port}/test.bsp.bz2')

    response = asyncio.run(run_co())

    assert response.is_success() is True

    lines: list[str] = []
    with open(mapcycle_file, 'r', encoding='utf-8') as fp:
        lines = fp.readlines()
    assert lines[0] == 'something\n'
    assert lines[1] == 'test\n'


def test_map_exists(temp_dir: Path, http_server_port: int, sftp_port: int):
    """Map already exists."""
    shutil.copyfile(os.path.join(CUR_DIR, 'testcontent/test.bsp'),
                    os.path.join(temp_dir, 'test.bsp'))

    async def run_co():
        loop = asyncio.get_event_loop()
        return await add_map(loop,
                             MapUploadConfig(sftp_hostname='127.0.0.1',
                                             sftp_port=sftp_port,
                                             sftp_username='username',
                                             sftp_password='password',
                                             sftp_remote_maps_path='some_directory/maps',
                                             maps_dir=temp_dir),
                             f'http://127.0.0.1:{http_server_port}/test.bsp')

    response = asyncio.run(run_co())

    assert response.is_success() is False
    assert 'test.bsp already exists.' in response.get_errors()


def test_add_mapcycle(temp_dir: Path, sftp_port: int):
    """Add map to mapcycle."""
    mapcycle_file = os.path.join(temp_dir, 'mapcycle.txt')
    with open(mapcycle_file, 'w', encoding='utf-8') as fp:
        fp.write('something\n')
    shutil.copyfile(os.path.join(CUR_DIR, 'testcontent/test.bsp'),
                    os.path.join(temp_dir, 'test.bsp'))

    async def run_co():
        loop = asyncio.get_event_loop()
        return await add_mapcycle(loop,
                                  MapUploadConfig(sftp_hostname='127.0.0.1',
                                                  sftp_port=sftp_port,
                                                  sftp_username='username',
                                                  sftp_password='password',
                                                  sftp_remote_maps_path='some_directory/maps',
                                                  maps_dir=temp_dir,
                                                  mapcycle_file=mapcycle_file),
                                  'test')

    response = asyncio.run(run_co())

    assert response.is_success() is True
    assert not response.get_errors()

    # Run again.
    response = asyncio.run(run_co())

    assert response.is_success() is False
    assert 'Fast-dl already has all files.' in response.get_errors()
    assert 'Map test already exists in mapcycle.' in response.get_errors()


def test_remove_mapcycle(temp_dir: Path):
    """Remove map from mapcycle."""
    mapcycle_file = os.path.join(temp_dir, 'mapcycle.txt')
    with open(mapcycle_file, 'w', encoding='utf-8') as fp:
        fp.write('something\ntest\n')

    async def run_co():
        loop = asyncio.get_event_loop()
        return await remove_mapcycle(loop,
                                     MapUploadConfig(maps_dir=temp_dir,
                                                     mapcycle_file=mapcycle_file),
                                     'test')

    success = asyncio.run(run_co())

    assert success is True

    # Run again.
    success = asyncio.run(run_co())

    assert success is False
