"""Map upload integration tests"""
import asyncio
import os
import filecmp
from fixtures import http_server, sftp_server  # pylint: disable=W0611
from util import removefile_unchecked
from mapupload import MapUploadConfig, _download_file, _upload_files, \
    _CompressedFile


CUR_DIR = os.path.abspath(os.path.dirname(__file__))


class TestServices:
    """Service integration tests"""

    def test_download_file(self, http_server_port: int):
        """Download file from a file server"""
        async def run_co():
            loop = asyncio.get_event_loop()
            return await _download_file(loop, MapUploadConfig(),
                                        f'http://127.0.0.1:{http_server_port}/test.zip')

        response = asyncio.run(run_co())

        assert response.success is True
        assert response.map_name == 'test'
        assert os.path.exists(response.temp_file) is True
        assert filecmp.cmp(response.temp_file, os.path.join(
            CUR_DIR, 'testcontent/test.zip'))
        removefile_unchecked(response.temp_file)

    def test_download_file_size(self, http_server_port: int):
        """Download file from a file server that is too large."""
        async def run_co():
            loop = asyncio.get_event_loop()
            return await _download_file(loop, MapUploadConfig(upload_max_bytes=10),
                                        f'http://127.0.0.1:{http_server_port}/test.zip')

        response = asyncio.run(run_co())

        assert response.success is False

    def test_upload_files(self, sftp_port: int):
        """Upload a new file to server in a specific directory."""
        config = MapUploadConfig(sftp_hostname='127.0.0.1',
                                 sftp_port=sftp_port,
                                 sftp_username='username',
                                 sftp_password='password',
                                 sftp_remote_maps_path='some_directory/maps')

        compressed_file = _CompressedFile(os.path.join(
            CUR_DIR, 'testcontent/test.bsp'),
            os.path.join(
            CUR_DIR, 'testcontent/test.bsp.bz2'))

        async def run_co():
            return await _upload_files(config, [compressed_file])

        uploaded = asyncio.run(run_co())
        assert len(uploaded) == 1

        uploaded = asyncio.run(run_co())
        assert len(uploaded) == 0  # Already uploaded
