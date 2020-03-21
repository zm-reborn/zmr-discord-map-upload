# Discord
import discord

# Other custom
import asyncssh
import aiohttp

# Our stuff
from configparser import ConfigParser
import os
import asyncio
import tempfile
import zipfile
import bz2
import re


def safe_extract_from_zip(zipfile, filename, out_dir):
    try:
        return zipfile.extract(filename, out_dir)
    except (KeyError) as e:
        pass

    return ''


def get_filename_no_ext(filename):
    name = os.path.basename(filename)
    # Split always returns an array with at least 1 element.
    return name.split('.')[0]


class MapFileData():
    def __init__(self):
        self.temp_file = ''
        self.map_name = ''
        self.success = False


class AddMapResponse():
    def __init__(self):
        self.errors = []
        self.files_extracted = 0
        self.files_compressed = 0


class MyDiscordClient(discord.Client):
    def __init__(self, config):
        super().__init__()

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

    async def on_message(self, message):
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
        if message.content.startswith('addmap ', 1):
            ret = await self.add_map(
                    message.content[len('!addmap '):])

            if not ret.errors:
                await self.quick_channel_msg(
                    '%s Success! Extracted %i file(s) and uploaded %i to fast-dl.' %
                    (message.author.mention,
                     ret.files_extracted,
                     ret.files_compressed))
            else:
                await self.quick_channel_msg(
                    '%s Failed to upload map. Error: %s' %
                    (message.author.mention, '\n'.join(ret.errors)))

    #
    # Discord utils
    #
    async def quick_channel_msg(self, msg, channel=None):
        if channel is None:
            channel = self.my_channel
        try:
            await channel.send(msg)
        except Exception as e:
            print(e)

    #
    # Our tasks
    #
    async def add_map(self, url):
        resp = AddMapResponse()

        data = await self.download_file(url)
        if not data.success:
            resp.errors.append('Failed to download file.')
            return resp

        files = []

        # Extract from ZIP
        if zipfile.is_zipfile(data.temp_file):
            files = await self.loop.run_in_executor(
                None,
                self.extract_zip,
                data)

        if not files:
            resp.errors.append('No files were extracted!')
            resp.errors.append('The map may already exists.')
            resp.errors.append(
                'Make sure you have at least %s.bsp inside the zip.' %
                (data.map_name))
            return resp

        resp.files_extracted = len(files)

        # Compress the files with BZIP2
        compressed_files = await self.loop.run_in_executor(
            None,
            self.compress_to_bz2,
            files)

        if not compressed_files:
            resp.errors.append('No files were compressed!')
            return resp

        resp.files_compressed = len(compressed_files)

        # Upload
        success = await self.upload_files(compressed_files)

        if not success:
            resp.errors.append('Failed to upload %i files to fast-dl!' %
                               (resp.files_compressed))
            return resp

        print('Removing local compressed files...')
        # Is this blocking?
        for f in compressed_files:
            os.remove(f)

        # Finally, insert into mapcycle file.
        await self.insert_into_mapcycle(data.map_name)

        return resp

    async def download_file(self, url):
        ret_data = MapFileData()

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if 'Content-Length' not in resp.headers:
                    print('Header had no Content-Length!')
                    return ret_data

                if 'Content-Disposition' not in resp.headers:
                    print('Header had no Content-Disposition!')
                    return ret_data

                content_len = int(resp.headers['Content-Length'])
                print('Content Length: %i' % (content_len))

                filename = self.get_filename_from_headers(resp.headers)

                if not filename:
                    print('Content-Disposition had no filename!')
                    return ret_data

                print('Filename: %s' % (filename))

                ret_data.map_name = filename

                if content_len > self.upload_max_bytes:
                    print('Content length is past max length of %i!' %
                          self.upload_max_bytes)
                    return ret_data

                await self.write_response_to_tempfile(resp, ret_data)

        return ret_data

    async def upload_files(self, files):
        print('Uploading files to SFTP...')

        success = False

        try:
            options = asyncssh.SSHClientConnectionOptions(
                username=self.sftp_username,
                password=self.sftp_password,
                known_hosts=None,
                x509_trusted_certs=None)

            async with asyncssh.connect(
                    host=self.sftp_hostname,
                    options=options) as conn:
                print('Established SSH connection server.')

                async with conn.start_sftp_client() as sftp:
                    print('Established SFTP.')

                    try:
                        await sftp.put(
                            files,
                            remotepath=self.sftp_remote_maps)

                        success = True
                    except (OSError, asyncssh.SFTPError) as e:
                        print('Exception putting files: ' + str(e))

                print('Closed SFTP connection.')

            print('Closed SSH connection.')

        except (OSError, asyncssh.SFTPError) as e:
            print('SSH/SFTP Exception: ' + str(e))

        return success

    async def insert_into_mapcycle(self, mapname):
        print('Inserting map %s into mapcycle...' % (mapname))
        
        maps = await self.loop.run_in_executor(None, self.read_mapcycle)

        sorted_maps = maps
        sorted_maps.append(mapname)
        sorted_maps = sorted(maps, key=self.mapcycle_sort)

        await self.loop.run_in_executor(None, self.save_mapcycle, sorted_maps)

    async def map_exists_in_mapcycle(self, mapname):
        maps = await self.loop.run_in_executor(None, self.read_mapcycle)

        async for lmap in maps:
            if re.search(mapname, lmap):
                return True

        return False

    #
    # Blocking
    #
    def read_mapcycle(self):
        maps = []

        with open(self.mapcycle_file, 'r') as fp:
            maps = fp.read().splitlines()

        return maps

    def save_mapcycle(self, maps):
        print('Saving maps to mapcycle...')

        with open(self.mapcycle_file, 'w') as fp:
            fp.write('\n'.join(maps))
            fp.write('\n')

    def extract_zip(self, data):
        print('Extracting ZIP contents of %s...' % (data.temp_file))

        files = []

        if not os.path.exists(data.temp_file):
            print('Zip file %s does not exist!' % (data.temp_file))
            return files

        safe_extract = [
            data.map_name + '.bsp',
            data.map_name + '.txt',
            data.map_name + '.nav'
        ]

        with zipfile.ZipFile(data.temp_file) as file:
            for f in safe_extract:
                out_file = os.path.join(self.maps_dir, f)

                if os.path.exists(out_file):
                    print('Cannot extract file %s because it already exists!' %
                          (out_file))
                    continue

                out_file2 = safe_extract_from_zip(file, f, self.maps_dir)
                if out_file2:
                    files.append(out_file)

        os.remove(data.temp_file)

        print('Extracted files:')
        for f in files:
            print('%s' % (f))

        return files

    def compress_to_bz2(self, files):
        compressed_files = []

        for uf in files:
            # We want to ignore this file?
            if re.search(self.bz2_ignore_regex, uf):
                continue

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

    async def write_response_to_tempfile(self, resp, ret_data):
        chunk_size = self.upload_chunksize

        with tempfile.NamedTemporaryFile(
                prefix='mapupload_',
                delete=False) as fp:
            print('Writing to temporary file %s in chunks of %i...' %
                  (fp.name, chunk_size))
            while True:
                chunk = await resp.content.read(chunk_size)
                if not chunk:
                    break
                await self.loop.run_in_executor(None, fp.write, chunk)

            ret_data.temp_file = fp.name
            ret_data.success = True

    #
    # Utils
    #
    def get_filename_from_headers(self, headers):
        disp = headers['Content-Disposition']
        match = re.search(
                r'filename=(?:"|)((?:\w|.)+?)(?:"|)(?:;|$)',
                disp,
                re.MULTILINE)

        if not match:
            return ''

        filename = get_filename_no_ext(match.group(1))

        return filename

    """The map name may have comments or
        other fluff in it that we want to ignore when sorting."""
    def mapcycle_sort(self, mapname):
        lower = mapname.lower()

        match = re.search(self.mapcycle_regex, lower)

        if not match:
            return lower

        return match.group(1)


if __name__ == '__main__':
    # Read our config
    config = ConfigParser()
    with open(os.path.join(os.path.dirname(__file__), '.config.ini')) as fp:
        config.read_file(fp)
    client = MyDiscordClient(config)

    try:
        client.run(config.get('discord', 'token'))
    except discord.LoginFailure:
        print('Failed to log in! Make sure your token is correct!')
