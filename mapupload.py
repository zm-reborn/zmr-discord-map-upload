"""Map upload specific logic"""
from asyncio import AbstractEventLoop
import os
import tempfile
import zipfile
import bz2
import re
import logging
import dataclasses
import shutil
from typing import NamedTuple
import asyncssh
import aiohttp
import discord
import multidict
import yarl
from util import removefile_unchecked

logger = logging.getLogger(__name__)


class _CompressedFile(NamedTuple):
    real_file: str
    compressed_file: str


@dataclasses.dataclass
class MapUploadConfig:
    """Configuration of map upload bot functions."""
    sftp_hostname: str = ''
    sftp_port: int = 22
    sftp_username: str = ''
    sftp_password: str = ''
    sftp_remote_maps_path: str = ''
    """SFTP remote maps-directory path"""
    upload_max_bytes: int = 100_000_000
    bz2_ignore_regex: str = '\\.nav'
    bz2_compressionlevel: int = 4
    maps_dir: str = ''
    """Local maps-directory path"""
    mapcycle_file: str = ''
    mapcycle_sort_regex: str = '(\\w+)'
    """What part of map name is used for sorting."""


class _BaseResponse():
    def __init__(self):
        self.success = False
        self.errors: list[str] = []

    def copy_errors(self, resp: '_BaseResponse'):
        """Copy errors from another response to this response."""
        self.errors.extend(resp.errors)

    def error(self, msg: str):
        """Append error."""
        self.errors.append(msg)


class _BaseTopResponse(_BaseResponse):
    def get_errors(self):
        """Get errors"""
        return self.errors

    def is_success(self):
        """Is success"""
        return self.success


class ExtractResponse(_BaseResponse):
    """File extraction response."""

    def __init__(self):
        super().__init__()
        # All files for this specific map that exist
        self.files: list[str] = []
        # All the files extracted, superset of files
        self.files_extracted: list[str] = []


class FastDLResponse(_BaseResponse):
    """Fast DL upload response."""

    def __init__(self):
        super().__init__()
        self.files_uploaded = []


class MapcycleResponse(_BaseResponse):
    """Mapcycle update response."""

    def __init__(self):
        super().__init__()
        self.wrote_to_file = False


class DownloadFileResponse(_BaseResponse):
    """File download response."""

    def __init__(self):
        super().__init__()
        self.temp_file = ''
        self.map_name = ''


#
# Top responses
#
class AddMapToMapcycleResponse(_BaseTopResponse):
    """Add map to mapcycle response."""

    def __init__(self):
        super().__init__()
        self.fastdl = FastDLResponse()
        self.mapcycle = MapcycleResponse()

    def get_errors(self):
        errors = self.errors[:]
        errors.extend(self.fastdl.errors)
        errors.extend(self.mapcycle.errors)
        return errors

    def is_success(self):
        if not self.success:
            return False

        # No files were uploaded and mapcycle wasn't updated
        if len(self.fastdl.files_uploaded) == 0:
            if not self.mapcycle.wrote_to_file:
                return False

        return True


class AddMapResponse(_BaseTopResponse):
    """Add map to server response."""

    def __init__(self):
        super().__init__()
        self.extract = ExtractResponse()
        self.fastdl = FastDLResponse()
        self.mapcycle = MapcycleResponse()

    def get_errors(self):
        errors = self.errors[:]
        errors.extend(self.extract.errors)
        errors.extend(self.fastdl.errors)
        errors.extend(self.mapcycle.errors)
        return errors

    def is_success(self):
        if not self.success:
            return False

        # No files were extracted or uploaded
        # And mapcycle was not updated.
        if len(self.fastdl.files_uploaded) == 0:
            if len(self.extract.files_extracted) == 0:
                if not self.mapcycle.wrote_to_file:
                    return False

        return True


async def add_map(loop: AbstractEventLoop, config: MapUploadConfig, url: str):
    """Extracts map from a zip file,
        uploads it to fast-dl and adds it to mapcycle."""

    logger.info('Adding map from url %s...', url)

    resp = AddMapResponse()

    #
    # Download
    #
    data = await _download_file(loop, config, url)
    resp.copy_errors(data)

    if not data.success:
        resp.error('Failed to download file.')
        return resp

    #
    # Extract
    #
    resp.extract = await loop.run_in_executor(None, _extract_file, config, data)

    try:
        loop.run_in_executor(None, os.remove, data.temp_file)
    except OSError as e:
        logger.error('Failed to remove temp file: %s', e)

    if not resp.extract.success:
        return resp

    if not resp.extract.files_extracted:
        resp.error('No files were extracted.')

        # Not even already existing files existed, just return
        if not resp.extract.files:
            return resp

    #
    # Add map to fast-dl
    #
    resp.fastdl = await _add_map_to_fastdl(loop, config, data.map_name)

    if not resp.fastdl.success:
        return resp

    #
    # Finally, insert into mapcycle file.
    #
    resp.mapcycle = await _insert_mapcycle(loop, config, data.map_name)

    if not resp.mapcycle.success:
        return resp

    resp.success = True

    return resp


async def add_mapcycle(loop: AbstractEventLoop, config: MapUploadConfig, mapname: str):
    """Add map to mapcycle and upload files to fast-dl if necessary."""

    mapname = _get_safe_filename_no_ext(mapname)

    logger.info('Adding map %s to mapcycle...', mapname)

    resp = AddMapToMapcycleResponse()

    #
    # Get map files (make sure it exists)
    #
    files = await loop.run_in_executor(
        None,
        _get_map_files, config, mapname, True)

    if not files:
        resp.error('Map does not exist!')
        return resp

    #
    # Add to fast-dl
    #
    resp.fastdl = await _add_map_to_fastdl(loop, config, mapname)

    if not resp.fastdl.success:
        return resp

    #
    # Add to mapcycle
    #
    resp.mapcycle = await _insert_mapcycle(loop, config, mapname)

    if not resp.mapcycle.success:
        return resp

    resp.success = True
    return resp


async def remove_mapcycle(loop: AbstractEventLoop, config: MapUploadConfig, mapname: str):
    """Remove map from mapcycle."""

    mapname = _get_safe_filename_no_ext(mapname)

    logger.info('Removing map %s from mapcycle...', mapname)

    return await _remove_mapcycle(loop, config, mapname)


def _extract_file(config: MapUploadConfig,
                  data: DownloadFileResponse):
    extract_resp = ExtractResponse()
    if not os.path.exists(data.temp_file):
        logger.error('Temporary file %s did not exist??', data.temp_file)
        return extract_resp

    if _is_bz2_file(data.temp_file):
        return _extract_bzip2(config, data)
    elif _is_bsp_file(data.temp_file):
        return _extract_bsp(config, data)
    # Must be checked last, because bsp contains zip data.
    elif zipfile.is_zipfile(data.temp_file):
        return _extract_zip(config, data)
    else:
        extract_resp.error('Unrecognized file format.')
    return extract_resp


async def _download_file(loop: AbstractEventLoop, config: MapUploadConfig, url: str):
    response = DownloadFileResponse()

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                await _parse_response(loop, resp, config, response)
        except aiohttp.InvalidURL:
            response.error(f'Invalid URL: {_escape_everything(url)}')
        except aiohttp.ClientError as e:
            logger.error('Failed to make a request to url %s (%s)', url, e)

    return response


async def _upload_files(config: MapUploadConfig, files: list[_CompressedFile], override=False):
    logger.info('Uploading files to SFTP...')

    uploaded = None
    try:
        async with asyncssh.connect(
                host=config.sftp_hostname,
                port=config.sftp_port,
                options=_ssh_connection_options(config)) as conn:
            logger.debug('Established SSH connection server.')

            async with conn.start_sftp_client() as sftp:
                logger.debug('Established SFTP.')

                # Check if those files exist.
                if not override:
                    fls = [f.real_file for f in files]
                    new_files = await _get_new_sftp_files(sftp, config.sftp_remote_maps_path, fls)
                    if new_files:
                        # Remove files already in fast-dl.
                        for i in range(len(files)-1, -1, -1):
                            if files[i].real_file not in new_files:
                                files.pop(i)
                    elif new_files is not None:
                        files = []  # Don't upload anything

                # Finally, upload them
                uploaded: list[str] = []
                if files:
                    try:
                        for f in files:
                            remotepath = os.path.join(config.sftp_remote_maps_path,
                                                      os.path.basename(
                                                          f.real_file)).replace('\\', '/') + '.bz2'
                            await sftp.put(
                                f.compressed_file,
                                remotepath=remotepath)
                            logger.info('Uploaded %s', remotepath)
                            uploaded.append(f.real_file)
                    except (OSError, asyncssh.SFTPError) as e:
                        logger.error('SFTP error putting files: %s', e)
            logger.debug('Closed SFTP connection.')

        logger.debug('Closed SSH connection.')

    except (OSError, asyncssh.Error):
        logger.error('SSH/SFTP Error.', exc_info=True)

    return uploaded


async def _get_new_sftp_files(sftp: asyncssh.SFTPClient,
                              sftp_remote_maps_path: str,
                              files: list[str]):
    """Returns list of files that don't exist in SFTP."""
    files = files[:]
    logger.info('Checking for any existing files...')
    try:
        old_files = files[:]
        for f in old_files:
            full_path = os.path.join(
                sftp_remote_maps_path,
                os.path.basename(f)).replace('\\', '/') + '.bz2'
            if await sftp.exists(full_path):
                files.remove(f)
                logger.info('%s already exists.', full_path)
            else:
                logger.info('%s did not exist.', full_path)
        return files
    except asyncssh.SFTPError as e:
        logger.error('SFTP error checking files: %s', e)
        return None


async def _insert_mapcycle(loop: AbstractEventLoop, config: MapUploadConfig, mapname: str):
    logger.info('Inserting map %s into mapcycle...', mapname)

    resp = MapcycleResponse()

    maps = await loop.run_in_executor(None, _read_mapcycle, config.mapcycle_file)

    if maps is None:
        logger.info('Mapcycle does not exists!')
        return resp

    if _map_exists_in_mapcycle(maps, mapname):
        resp.error(f'Map {mapname} already exists in mapcycle.')
        # Consider it success
        resp.success = True
        return resp

    def sort_key(mapname: str):
        """The map name may have comments or
        other fluff in it that we want to ignore when sorting."""
        lower = mapname.lower()
        match = re.search(config.mapcycle_sort_regex, lower)
        return lower if not match else match.group(1)

    sorted_maps = maps
    sorted_maps.append(mapname)
    sorted_maps = sorted(maps, key=sort_key)

    wrote = await loop.run_in_executor(
        None,
        _save_mapcycle, config.mapcycle_file, sorted_maps)

    if wrote:
        resp.wrote_to_file = True
        resp.success = True

    return resp


async def _remove_mapcycle(loop: AbstractEventLoop, config: MapUploadConfig, mapname: str):
    """Remove map from mapcycle."""

    mapcycle = await loop.run_in_executor(None, _read_mapcycle, config.mapcycle_file)

    if mapcycle is None:
        return False

    removed = False
    for lmap in mapcycle:
        if re.match(mapname, lmap):
            mapcycle.remove(lmap)
            removed = True
            break

    if removed:
        wrote = await loop.run_in_executor(
            None,
            _save_mapcycle, config.mapcycle_file, mapcycle)
        return wrote

    return False


async def _add_map_to_fastdl(loop: AbstractEventLoop,
                             config: MapUploadConfig,
                             mapname: str,
                             override_files=False):
    """Add map to fast-dl if we have any files to add."""

    logger.info('Adding map %s to fast-dl...', mapname)

    resp = FastDLResponse()

    # Add local path, upload only files
    files = await loop.run_in_executor(
        None,
        _get_map_files, config, mapname, True, True)

    # We don't have this map's files!
    if not files:
        return resp

    # Check if those files exist on the fast-dl.
    if not override_files:
        new_files: list[str] = []
        try:
            async with asyncssh.connect(
                    host=config.sftp_hostname,
                    port=config.sftp_port,
                    options=_ssh_connection_options(config)) as conn:
                logger.debug('Established SSH connection server.')

                async with conn.start_sftp_client() as sftp:
                    logger.debug('Established SFTP.')
                    new_files = await _get_new_sftp_files(sftp, config.sftp_remote_maps_path, files)

                logger.debug('Closed SFTP connection.')

            logger.debug('Closed SSH connection.')

        except (OSError, asyncssh.Error):
            logger.error('SSH/SFTP error.', exc_info=True)

        if new_files is None:
            logger.info('Error occurred checking %s fast-dl files.', mapname)
            return resp

        if not new_files:
            resp.error('Fast-dl already has all files.')
            # Consider it as success
            resp.success = True
            return resp

        if len(new_files) != len(files):
            logger.info('Fast-dl already had some of the map files...')
            files = new_files

    #
    # Compress the files with BZIP2
    #
    compressed_files = await loop.run_in_executor(
        None,
        _compress_to_bz2,
        config.bz2_compressionlevel,
        files)

    if not compressed_files:
        resp.error('No files were compressed!')
        return resp

    #
    # Upload
    #
    uploaded = await _upload_files(config, compressed_files)

    resp.files_uploaded = [os.path.basename(f) for f in uploaded]

    if uploaded is None:
        resp.error('Failed to upload files to fast-dl!')

    def remove_compressed():
        logger.info('Removing local compressed files...')
        for f in compressed_files:
            removefile_unchecked(f.compressed_file)
    loop.run_in_executor(None, remove_compressed)

    resp.success = True
    return resp


def _map_exists_in_mapcycle(mapcycle: list[str], mapname: str):
    for lmap in mapcycle:
        if re.match(mapname, lmap):
            return True
    return False


async def _parse_response(loop: AbstractEventLoop,
                          resp: aiohttp.ClientResponse,
                          config: MapUploadConfig,
                          ret_response: DownloadFileResponse):
    if 'Content-Length' not in resp.headers:
        logger.info('Header had no Content-Length!')
        return ret_response

    if 'Content-Type' not in resp.headers:
        logger.info('Header had no Content-Type!')
        return ret_response

    cont_type = resp.headers['Content-Type']

    valid_types = [
        'application/zip',
        'application/binary',
        'application/octet-stream'
    ]

    if cont_type not in valid_types:
        logger.info('Header had invalid Content-Type "%s"!', cont_type)
        return ret_response

    content_len = int(resp.headers['Content-Length'])
    logger.info('Content Length: %i', content_len)

    filename = _get_filename_from_headers(resp.headers)

    # Try url
    if not filename:
        filename = _get_filename_from_url(resp.url)

    if not filename:
        logger.info('Headers had no filename!')
        return ret_response

    logger.info('Filename: %s', filename)

    ret_response.map_name = filename

    if content_len > config.upload_max_bytes:
        ret_response.error(
            f'File size goes over limit of {config.upload_max_bytes / 1e6} MB')
        logger.info('Content length is past max length of %i!',
                    config.upload_max_bytes)
        return ret_response

    await _write_response_to_tempfile(loop, resp, ret_response)


def _read_mapcycle(mapcycle_file: str):
    try:
        with open(mapcycle_file, 'r', encoding='utf-8') as fp:
            return fp.read().splitlines()
    except OSError as e:
        logger.error('Error reading mapcycle: %s', e)

    return None


def _save_mapcycle(mapcycle_file: str, maps: list[str]):
    logger.info('Saving maps to mapcycle...')

    ok = False

    try:
        with open(mapcycle_file, 'w', encoding='utf-8') as fp:
            fp.write('\n'.join(maps))
            fp.write('\n')
        ok = True
    except OSError as e:
        logger.error('Error writing mapcycle: %s', e)

    return ok


def _extract_zip(config: MapUploadConfig, data: DownloadFileResponse):
    ret = ExtractResponse()

    logger.info('Extracting ZIP contents of %s...', data.temp_file)

    safe_extract = _get_map_files(config, data.map_name)

    with zipfile.ZipFile(data.temp_file) as file:
        for f in safe_extract:
            out_file = os.path.join(config.maps_dir, f)

            info = None
            try:
                info = file.getinfo(f)
            except KeyError:
                pass

            if os.path.exists(out_file):
                # Already exists
                if info:
                    ret.files.append(f)
                ret.error(f'{f} already exists.')
                logger.info(
                    'Cannot extract file %s because it already exists.', out_file)
                continue

            # File does not exist
            if not info:
                continue

            out_file2 = file.extract(f, config.maps_dir)
            if out_file2:
                ret.files.append(out_file)
                ret.files_extracted.append(out_file)

    logger.info('Extracted files:')
    for f in ret.files_extracted:
        logger.info('%s', f)

    ret.success = True

    return ret


def _extract_bzip2(config: MapUploadConfig, data: DownloadFileResponse):
    ret = ExtractResponse()
    out_file = os.path.join(config.maps_dir, data.map_name + '.bsp')
    ret.files.append(out_file)
    if os.path.exists(out_file):
        ret.error(f'{data.map_name}.bsp already exists.')
        return ret

    with bz2.open(data.temp_file) as in_fp:
        with open(out_file, 'wb') as out_fp:
            while True:
                buffer = in_fp.read(16000)
                if not buffer:
                    break
                out_fp.write(buffer)

    ret.success = True
    ret.files_extracted.append(out_file)

    return ret


def _extract_bsp(config: MapUploadConfig, data: DownloadFileResponse):
    ret = ExtractResponse()
    out_file = os.path.join(config.maps_dir, data.map_name + '.bsp')
    ret.files.append(out_file)

    if os.path.exists(out_file):
        ret.error(f'{data.map_name}.bsp already exists.')
        return ret

    shutil.copyfile(data.temp_file, out_file)

    ret.success = True
    ret.files_extracted.append(out_file)

    return ret


def _compress_to_bz2(compresslevel: int, files: list[str]):
    """Compresses the given files to temporary files."""
    compressed_files: list[_CompressedFile] = []

    for uf in files:
        with open(uf, 'rb') as ufp:
            temp_fp = tempfile.NamedTemporaryFile(
                prefix='mapupload_bz2_',
                delete=False)
            with bz2.open(temp_fp.name, 'wb', compresslevel) as cfp:
                while True:
                    chunk = ufp.read(16000)
                    if not chunk:
                        break
                    cfp.write(chunk)
                compressed_files.append(_CompressedFile(
                    real_file=uf, compressed_file=temp_fp.name))

    logger.info('Compressed files:')
    for f in compressed_files:
        logger.info('%s: %s', f.real_file, f.compressed_file)

    return compressed_files


async def _write_response_to_tempfile(loop: AbstractEventLoop,
                                      resp: aiohttp.ClientResponse,
                                      ret_data: DownloadFileResponse):
    buffer_size = 16000
    fp = tempfile.NamedTemporaryFile(
        prefix='mapupload_',
        delete=False)
    logger.info('Writing to temporary file %s in chunks of %i...',
                fp.name, buffer_size)
    try:
        while True:
            chunk = await resp.content.read(buffer_size)
            if not chunk:
                break
            await loop.run_in_executor(None, fp.write, chunk)

        ret_data.temp_file = fp.name
        ret_data.success = True
    except:  # pylint: disable=W0702
        logger.error(
            'Failed to read response / write to temp file.', exc_info=True)
        await loop.run_in_executor(None, removefile_unchecked, fp.name)


def _get_filename_from_headers(headers: multidict.CIMultiDictProxy[str]):
    if 'Content-Disposition' not in headers:
        logger.info('Header had no Content-Disposition!')
        return None

    disp = headers['Content-Disposition']
    match = re.search(
        r'filename=(?:"|)((?:\w|.)+?)(?:"|)(?:;|$)',
        disp,
        re.MULTILINE)

    if not match:
        return None

    filename = _get_safe_filename_no_ext(match.group(1))

    return filename


def _get_filename_from_url(url: yarl.URL):
    return _get_safe_filename_no_ext(url.path)


def _ssh_connection_options(config: MapUploadConfig):
    return asyncssh.SSHClientConnectionOptions(
        username=config.sftp_username,
        password=config.sftp_password,
        known_hosts=None,
        x509_trusted_certs=None)


def _get_map_files(config: MapUploadConfig, mapname: str, add_local_path=False, upload_only=False):
    files = [
        mapname + '.bsp',
        mapname + '.txt',
        mapname + '.nav'
    ]

    # Remove all files not getting uploaded.
    if upload_only:
        temp = files[:]
        for f in temp:
            if re.search(config.bz2_ignore_regex, f):
                files.remove(f)

    # Add full path to files
    # Make sure the files exist.
    if add_local_path:
        temp = []
        for f in files:
            full_path = os.path.join(config.maps_dir, f)

            if os.path.exists(full_path):
                temp.append(full_path)

        files = temp

    return files


def _is_bz2_file(file: str):
    with open(file, 'rb') as fp:
        return fp.read(3) == b'BZh'


def _is_bsp_file(file: str):
    with open(file, 'rb') as fp:
        return fp.read(4) == b'VBSP'


def _escape_everything(data: str):
    """Escape all characters that could be used in Discord."""
    return discord.utils.escape_markdown(discord.utils.escape_mentions(data))


def _get_safe_filename_no_ext(filename: str):
    name = os.path.basename(filename)
    index = name.rfind('\\')  # Windows path hack
    if index != -1:
        name = name[index+1:]
    index = name.find('.')
    if index != -1:
        name = name[:index]
    return name
