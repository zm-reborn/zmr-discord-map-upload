"""Pytest fixtures"""
import os
from pytest import FixtureRequest, fixture, TempPathFactory
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


@fixture(name='temp_dir', scope='function')
def temp_dir_fixture(tmp_path_factory: TempPathFactory):
    """Temp directory fixture"""
    return tmp_path_factory.mktemp('mapupload_test_temp')


@fixture(scope='class', name='http_server_port')
def http_server(request: FixtureRequest):
    """HTTP server docker fixture"""
    container = DockerContainer('halverneus/static-file-server', ports=[8080], volumes=[
                                (os.path.join(CUR_DIR, 'testcontent'), '/web', 'ro')])
    container.start()

    def cleanup():
        container.stop()
    request.addfinalizer(cleanup)

    return container.get_exposed_port(8080)


@fixture(scope='class', name='sftp_port')
def sftp_server(request: FixtureRequest):
    """SFTP docker fixture"""
    container = DockerContainer('emberstack/sftp', ports=[22], volumes=[
                                (os.path.join(CUR_DIR, 'sftp.json'),
                                 '/app/config/sftp.json',
                                 'ro'
                                 )])

    container.start()

    def cleanup():
        container.stop()
    request.addfinalizer(cleanup)

    wait_for_logs(container, 'Server listening on 0.0.0.0 port 22')

    return container.get_exposed_port(22)
