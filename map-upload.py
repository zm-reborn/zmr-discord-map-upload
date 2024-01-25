# Discord
import discord

# Other custom
import asyncssh
import aiohttp

# Our stuff
from configparser import ConfigParser
import os
import tempfile
import zipfile
import bz2
import re
import yarl
import multidict


def escape_everything(data: str):
    return discord.utils.escape_markdown(discord.utils.escape_mentions(data))


def safe_extract_from_zip(zipfile, filename, out_dir):
    try:
        return zipfile.extract(filename, out_dir)
    except (KeyError) as e:
        pass

    return ''


def get_filename_no_ext(filename: str):
    name = os.path.basename(filename)
    # Split always returns an array with at least 1 element.
    return name.split('.')[0]


class BaseResponse():
    def __init__(self):
        self.success = False
        self.errors = []

    def copy_errors(self, resp):
        self.errors.extend(resp.errors)

    def error(self, msg):
        self.errors.append(msg)


class BaseTopResponse(BaseResponse):
    def get_errors(self):
        return self.errors

    def is_success(self):
        return self.success


class ExtractResponse(BaseResponse):
    def __init__(self):
        super().__init__()
        # All files for this specific map that exist
        self.files = []
        # All the files extracted, superset of files
        self.files_extracted = []


class FastDLResponse(BaseResponse):
    def __init__(self):
        super().__init__()
        self.files_uploaded = []


class MapCycleResponse(BaseResponse):
    def __init__(self):
        super().__init__()
        self.wrote_to_file = False


class DownloadFileResponse(BaseResponse):
    def __init__(self):
        super().__init__()
        self.temp_file = ''
        self.map_name = ''


#
# Top responses
#
class AddMapToMapCycleResponse(BaseTopResponse):
    def __init__(self):
        super().__init__()
        self.fastdl = FastDLResponse()
        self.mapcycle = MapCycleResponse()

    def get_errors(self):
        errors = self.errors
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


class AddMapResponse(BaseTopResponse):
    def __init__(self):
        super().__init__()
        self.extract = ExtractResponse()
        self.fastdl = FastDLResponse()
        self.mapcycle = MapCycleResponse()

    def get_errors(self):
        errors = self.errors
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


class MyDiscordClient(discord.Client):
    def __init__(self, config: ConfigParser):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)

        self.my_channel = None
        # self.my_guild = None

        self.token = config.get('discord', 'token')
        self.channel_id = int(config.get('discord', 'channel'))

        self.upload_chunksize = int(config.get('upload', 'chunksize'))
        self.upload_max_bytes = int(config.get('upload', 'maxbytes'))

        self.maps_dir = os.path.abspath(config.get('server', 'mapsdir'))
        self.mapcycle_file = config.get('server', 'mapcyclefile')
        self.mapcycle_regex = config.get('server', 'mapcycleregex')

        self.sftp_hostname = config.get('sftp', 'hostname')
        self.sftp_username = config.get('sftp', 'username')
        self.sftp_password = config.get('sftp', 'password')
        self.sftp_remote_maps = config.get('sftp', 'mapsdir')

        self.bz2_ignore_regex = config.get('bz2', 'ignoreregex')
        self.bz2_compressionlevel = int(config.get('bz2', 'compressionlevel'))

    #
    # Discord.py
    #
    async def on_ready(self):
        print('Logged on as', self.user)

        self.my_channel = self.get_channel(self.channel_id)
        if self.my_channel is None:
            raise Exception('Channel with id %i does not exist!' %
                            self.channel_id)

        # self.my_guild = self.my_channel.guild

    async def on_message(self, message: discord.Message):
        if not self.is_ready():
            return
        # Not command?
        if not message.content or message.content[0] != '!':
            return
        # Don't respond to ourselves
        if message.author == self.user:
            return
        # Only in my channel
        if message.channel != self.my_channel:
            return
        # Not a member of my server
        # member = self.my_guild.get_member(message.author.id)
        # if member is None:
        #    return

        #
        # Actual actions.
        #

        # Add map through url
        if message.content.startswith('addmap ', 1):
            ret = await self.add_map(
                    message.content[len('!addmap '):])

            if ret.is_success():
                await self.quick_channel_msg(
                    '%s Success! %s' %
                    (message.author.mention, '\n'.join(ret.get_errors())))
            else:
                await self.quick_channel_msg(
                    '%s Failed to upload map. %s' %
                    (message.author.mention, '\n'.join(ret.get_errors())))

        # Add map to mapcycle (+ add to fast-dl)
        if message.content.startswith('addmapcycle ', 1):
            ret = await self.add_mapcycle(
                    message.content[len('!addmapcycle '):])

            if ret.is_success():
                await self.quick_channel_msg(
                    '%s Success! %s' %
                    (message.author.mention, '\n'.join(ret.get_errors())))
            else:
                await self.quick_channel_msg(
                    '%s Failed to add map to mapcycle! %s' %
                    (message.author.mention, '\n'.join(ret.get_errors())))

        # Remove map from mapcycle
        if message.content.startswith('removemapcycle ', 1):
            ret = await self.remove_mapcycle(
                    message.content[len('!removemapcycle '):])

            if ret:
                await self.quick_channel_msg(
                    '%s Removed map from mapcycle.' %
                    (message.author.mention))
            else:
                await self.quick_channel_msg(
                    '%s Failed to remove map from mapcycle!' %
                    (message.author.mention))

    #
    # Discord utils
    #
    async def quick_channel_msg(self, msg: str):
        try:
            await self.my_channel.send(msg)
        except Exception as e:
            print(e)

    #
    # Our tasks
    #
    """Extracts map from a zip file,
        uploads it to fast-dl and adds it to mapcycle."""
    async def add_map(self, url: str):
        print('Adding map from url %s...' % (url))

        resp = AddMapResponse()

        #
        # Download
        #
        data = await self.download_file(url)
        resp.copy_errors(data)

        if not data.success:
            resp.errors = ['Failed to download file.'] + resp.errors
            return resp

        #
        # Extract from ZIP
        #
        extract_resp = None

        if zipfile.is_zipfile(data.temp_file):
            extract_resp = await self.loop.run_in_executor(
                None,
                self.extract_zip,
                data)

        if not extract_resp:
            resp.error('File must be a .zip file!')
            return resp

        resp.extract = extract_resp

        if not extract_resp.success:
            return resp

        if not len(extract_resp.files_extracted):
            resp.errors = ['No files were extracted.'] + resp.errors

            # Not even already existing files existed, just return
            if not len(extract_resp.files):
                return resp

        #
        # Add map to fast-dl
        #
        resp.fastdl = await self.add_map_to_fastdl(data.map_name)

        if not resp.fastdl.success:
            return resp

        #
        # Finally, insert into mapcycle file.
        #
        resp.mapcycle = await self.insert_into_mapcycle(data.map_name)

        if not resp.mapcycle.success:
            return resp

        resp.success = True

        return resp

    """Add map to mapcycle and upload files to fast-dl if necessary."""
    async def add_mapcycle(self, mapname: str):
        mapname = get_filename_no_ext(mapname)

        print('Adding map %s to mapcycle...' % (mapname))

        resp = AddMapToMapCycleResponse()

        #
        # Get map files (make sure it exists)
        #
        files = await self.loop.run_in_executor(
            None,
            self.get_map_files, mapname, True)

        if not files:
            resp.error('Map does not exist!')
            return resp

        #
        # Add to fast-dl
        #
        resp.fastdl = await self.add_map_to_fastdl(mapname)

        if not resp.fastdl.success:
            return resp

        #
        # Add to mapcycle
        #
        resp.mapcycle = await self.insert_into_mapcycle(mapname)

        if not resp.mapcycle.success:
            return resp

        resp.success = True
        return resp

    """Remove map from mapcycle."""
    async def remove_mapcycle(self, mapname: str):
        mapname = get_filename_no_ext(mapname)

        print('Removing map %s from mapcycle...' % (mapname))

        return await self.remove_from_mapcycle(mapname)

    #
    # Lower routines
    #
    async def download_file(self, url: str):
        data = DownloadFileResponse()

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as resp:
                    await self.parse_response(resp, data)
            except (aiohttp.ClientResponseError) as e:
                print('Response error: %s' % (e))
            except (aiohttp.InvalidURL) as e:
                data.error('Invalid URL: %s' %
                           (escape_everything(url)))
                print('Invalid URL (%s): %s' % (url, e))

        return data

    async def upload_files(self, files: list[str], override=False):
        print('Uploading files to SFTP...')

        uploaded = -1

        try:
            async with asyncssh.connect(
                    host=self.sftp_hostname,
                    options=self.get_ssh_conn_options()) as conn:
                print('Established SSH connection server.')

                async with conn.start_sftp_client() as sftp:
                    print('Established SFTP.')

                    # Check if those files exist.
                    if not override:
                        try:
                            old_files = files[:]
                            for f in old_files:
                                full_path = os.path.join(
                                    self.sftp_remote_maps,
                                    os.path.basename(f))
                                if await sftp.exists(full_path):
                                    files.remove(f)
                        except (asyncssh.SFTPError) as e:
                            print('Exception checking files: ' + str(e))

                    # Finally, upload them
                    try:
                        await sftp.put(
                            files,
                            remotepath=self.sftp_remote_maps)

                        uploaded = len(files)
                    except (OSError, asyncssh.SFTPError) as e:
                        print('Exception putting files: ' + str(e))

                print('Closed SFTP connection.')

            print('Closed SSH connection.')

        except (OSError, asyncssh.SFTPError) as e:
            print('SSH/SFTP Exception: ' + str(e))

        return uploaded

    async def insert_into_mapcycle(self, mapname: str):
        print('Inserting map %s into mapcycle...' % (mapname))

        resp = MapCycleResponse()

        maps = await self.loop.run_in_executor(None, self.read_mapcycle)

        if maps is None:
            print('Mapcycle does not exists!' % (mapname))
            return resp

        if await self.map_exists_in_mapcycle(mapname, maps):
            resp.error('Map %s already exists in mapcycle.' % (mapname))
            # Consider it success
            resp.success = True
            return resp

        sorted_maps = maps
        sorted_maps.append(mapname)
        sorted_maps = sorted(maps, key=self.mapcycle_sort)

        wrote = await self.loop.run_in_executor(
            None,
            self.save_mapcycle, sorted_maps)

        if wrote:
            resp.wrote_to_file = True
            resp.success = True

        return resp

    async def remove_from_mapcycle(self, mapname: str):
        mapcycle = await self.loop.run_in_executor(None, self.read_mapcycle)

        if mapcycle is None:
            return False

        removed = False
        for lmap in mapcycle:
            if re.match(mapname, lmap):
                mapcycle.remove(lmap)
                removed = True
                break

        if removed:
            wrote = await self.loop.run_in_executor(
                None,
                self.save_mapcycle, mapcycle)
            return wrote

        return False

    """Add map to fast-dl if we have any files to add."""
    async def add_map_to_fastdl(self, mapname: str, override_files=False):
        mapname = get_filename_no_ext(mapname)

        print('Adding map %s to fast-dl...' % (mapname))

        resp = FastDLResponse()

        # Add local path, upload only files
        files = await self.loop.run_in_executor(
            None,
            self.get_map_files, mapname, True, True)

        # We don't have this map's files!
        if not files:
            return resp

        files_exist = None

        # Check if those files exist on the fast-dl.
        if not override_files:
            try:
                async with asyncssh.connect(
                        host=self.sftp_hostname,
                        options=self.get_ssh_conn_options()) as conn:
                    print('Established SSH connection server.')

                    async with conn.start_sftp_client() as sftp:
                        print('Established SFTP.')

                        print('Checking for any existing files...')
                        files_exist = []
                        try:
                            for f in files:
                                full_path = os.path.join(
                                    self.sftp_remote_maps,
                                    os.path.basename(f + '.bz2'))
                                if await sftp.exists(full_path):
                                    print('%s already exists' % (full_path))
                                    files_exist.append(f)
                        except (asyncssh.SFTPError) as e:
                            print('Exception checking files: %s' % (e))

                    print('Closed SFTP connection.')

                print('Closed SSH connection.')

            except (OSError, asyncssh.SFTPError) as e:
                print('SSH/SFTP Exception: ' + str(e))

            if files_exist is None:
                print('Error occurred checking %s fast-dl files.' % (mapname))
                return resp

            if len(files_exist) == len(files):
                resp.error('Fast-dl already has all files.')
                # Consider it as success
                resp.success = True
                return resp

            if len(files_exist) > 0:
                print('Fast-dl already had some of the map files...')
                for f in files_exist:
                    files.remove(f)

        #
        # Compress the files with BZIP2
        #
        compressed_files = await self.loop.run_in_executor(
            None,
            self.compress_to_bz2,
            files)

        if not compressed_files:
            resp.error('No files were compressed!')
            return resp

        #
        # Upload
        #
        uploaded = await self.upload_files(compressed_files)

        resp.files_uploaded = compressed_files

        if uploaded == -1:
            resp.error('Failed to upload files to fast-dl!')

        print('Removing local compressed files...')
        # Is this blocking?
        for f in compressed_files:
            os.remove(f)

        resp.success = True
        return resp

    async def map_exists_in_mapcycle(self, mapname: str, mapcycle: list[str] | None):
        if not mapcycle:
            mapcycle = await self.loop.run_in_executor(
                None,
                self.read_mapcycle)
            
        if mapcycle is None:
            return

        for lmap in mapcycle:
            if re.match(mapname, lmap):
                return True

        return False

    async def parse_response(self, resp: aiohttp.ClientResponse, ret_data: DownloadFileResponse):
        if 'Content-Length' not in resp.headers:
            print('Header had no Content-Length!')
            return ret_data

        if 'Content-Type' not in resp.headers:
            print('Header had no Content-Type!')
            return ret_data

        cont_type = resp.headers['Content-Type']

        valid_types = [
            'application/zip',
            'application/binary'
        ]

        if cont_type not in valid_types:
            print('Header had invalid Content-Type "%s"!' % (cont_type))
            return ret_data

        content_len = int(resp.headers['Content-Length'])
        print('Content Length: %i' % (content_len))

        self.get_filename_from_url(resp.url)
        filename = self.get_filename_from_headers(resp.headers)

        # Try url
        if not filename:
            filename = self.get_filename_from_url(resp.url)

        if not filename:
            print('Headers had no filename!')
            return ret_data

        print('Filename: %s' % (filename))

        ret_data.map_name = filename

        if content_len > self.upload_max_bytes:
            ret_data.error('File size goes over limit of %.1f MB' %
                           (self.upload_max_bytes / 1e6))
            print('Content length is past max length of %i!' %
                  (self.upload_max_bytes))
            return ret_data

        await self.write_response_to_tempfile(resp, ret_data)

    #
    # Blocking
    #
    def read_mapcycle(self):
        maps = None

        try:
            with open(self.mapcycle_file, 'r') as fp:
                maps = fp.read().splitlines()
        except FileNotFoundError:
            pass
        except OSError as e:
            print('Unexpected error reading mapcycle: %s' % (e))

        return maps

    def save_mapcycle(self, maps: list[str]):
        print('Saving maps to mapcycle...')

        ok = False

        try:
            with open(self.mapcycle_file, 'w') as fp:
                fp.write('\n'.join(maps))
                fp.write('\n')
            ok = True
        except OSError as e:
            print('Error writing mapcycle: %s' % (e))

        return ok

    def extract_zip(self, data: DownloadFileResponse):
        ret = ExtractResponse()

        print('Extracting ZIP contents of %s...' % (data.temp_file))

        if not os.path.exists(data.temp_file):
            print('Zip file %s does not exist!' % (data.temp_file))
            return ret

        safe_extract = self.get_map_files(data.map_name)

        with zipfile.ZipFile(data.temp_file) as file:
            for f in safe_extract:
                out_file = os.path.join(self.maps_dir, f)

                info = None
                try:
                    info = file.getinfo(f)
                except KeyError:
                    pass

                if os.path.exists(out_file):
                    # Already exists
                    if info:
                        ret.files.append(f)
                    ret.error('%s already exists.' % (f))
                    print('Cannot extract file %s because it already exists.' %
                          (out_file))
                    continue

                # File does not exist
                if not info:
                    continue

                out_file2 = file.extract(f, self.maps_dir)
                if out_file2:
                    ret.files.append(out_file)
                    ret.files_extracted.append(out_file)

        os.remove(data.temp_file)

        print('Extracted files:')
        for f in ret.files_extracted:
            print('%s' % (f))

        ret.success = True

        return ret

    def compress_to_bz2(self, files: list[str]):
        compressed_files: list[str] = []

        for uf in files:
            with open(uf, 'rb') as ufp:
                cf_name = uf + '.bz2'
                with bz2.open(cf_name, 'wb', self.bz2_compressionlevel) as cfp:
                    while True:
                        chunk = ufp.read(16000)
                        if not chunk:
                            break
                        cfp.write(chunk)
                    compressed_files.append(cf_name)

        print('Compressed files:')
        for f in compressed_files:
            print('%s' % (f))

        return compressed_files

    async def write_response_to_tempfile(self, resp: aiohttp.ClientResponse, ret_data: DownloadFileResponse):
        chunk_size = self.upload_chunksize

        with tempfile.NamedTemporaryFile(
                prefix='mapupload_',
                delete=False) as fp:
            print('Writing to temporary file %s in chunks of %i...' %
                  (fp.name, chunk_size))
            while True:
                chunk = None
                try:
                    chunk = await resp.content.read(chunk_size)
                except (Exception) as e:
                    print('Exception reading content: %s' % (e))

                if not chunk:
                    break
                await self.loop.run_in_executor(None, fp.write, chunk)

            ret_data.temp_file = fp.name
            ret_data.success = True

    #
    # Utils
    #
    def get_filename_from_headers(self, headers: multidict.CIMultiDictProxy[str]):
        if 'Content-Disposition' not in headers:
            print('Header had no Content-Disposition!')
            return ''

        disp = headers['Content-Disposition']
        match = re.search(
                r'filename=(?:"|)((?:\w|.)+?)(?:"|)(?:;|$)',
                disp,
                re.MULTILINE)

        if not match:
            return ''

        filename = get_filename_no_ext(match.group(1))

        return filename

    def get_filename_from_url(self, url: yarl.URL):
        return get_filename_no_ext(url.path)

    """The map name may have comments or
        other fluff in it that we want to ignore when sorting."""
    def mapcycle_sort(self, mapname: str):
        lower = mapname.lower()

        match = re.search(self.mapcycle_regex, lower)

        if not match:
            return lower

        return match.group(1)

    def get_ssh_conn_options(self):
        return asyncssh.SSHClientConnectionOptions(
            username=self.sftp_username,
            password=self.sftp_password,
            known_hosts=None,
            x509_trusted_certs=None)

    def get_map_files(self, mapname: str, add_local_path=False, upload_only=False):
        files = [
            mapname + '.bsp',
            mapname + '.txt',
            mapname + '.nav'
        ]

        # Remove all files not getting uploaded.
        if upload_only:
            temp = files[:]
            for f in temp:
                if re.search(self.bz2_ignore_regex, f):
                    files.remove(f)

        # Add full path to files
        # Make sure the files exist.
        if add_local_path:
            temp = []
            for f in files:
                full_path = os.path.join(self.maps_dir, f)

                if os.path.exists(full_path):
                    temp.append(full_path)

            files = temp

        return files


if __name__ == '__main__':
    config = ConfigParser()
    with open(os.path.join(os.path.dirname(__file__), '.config.ini')) as fp:
        config.read_file(fp)

    client = MyDiscordClient(config)

    try:
        client.run(config.get('discord', 'token'))
    except discord.LoginFailure:
        print('Failed to log in! Make sure your token is correct!')
    except Exception as e:
        print(f'Something went wrong: {str(e)}')
