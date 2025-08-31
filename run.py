"""Run map upload bot."""
from configparser import ConfigParser
import os
import sys
import logging
import discord
from mapupload import MapUploadConfig, add_map, add_mapcycle, remove_mapcycle


logger = logging.getLogger('mapupload.run')


def _config_parse(prsr: ConfigParser):
    sftp_hostname = prsr.get('sftp', 'hostname')
    sftp_username = prsr.get('sftp', 'username')
    sftp_password = prsr.get('sftp', 'password')
    sftp_mapsdir = prsr.get('sftp', 'mapsdir')
    sftp_port_str = prsr.get('sftp', 'port')
    sftp_port = 22 if not sftp_port_str else int(sftp_port_str)
    upload_maxbytes_str = prsr.get('upload', 'maxbytes')
    upload_maxbytes = 100_000_000 if not upload_maxbytes_str else int(
        upload_maxbytes_str)
    bz2_ignoreregex = prsr.get('bz2', 'ignoreregex')
    compresslevel_str = prsr.get('bz2', 'compressionlevel')
    compresslevel = 4 if not compresslevel_str else int(
        compresslevel_str)
    mapsdir = prsr.get('server', 'mapsdir')
    mapcyclefile = prsr.get('server', 'mapcyclefile')
    mapcycleregex = prsr.get('server', 'mapcycleregex')
    return MapUploadConfig(sftp_hostname=sftp_hostname,
                           sftp_username=sftp_username,
                           sftp_password=sftp_password,
                           sftp_remote_maps_path=sftp_mapsdir,
                           sftp_port=sftp_port,
                           upload_max_bytes=upload_maxbytes,
                           bz2_ignore_regex=bz2_ignoreregex,
                           bz2_compressionlevel=compresslevel,
                           maps_dir=mapsdir,
                           mapcycle_file=mapcyclefile,
                           mapcycle_sort_regex=mapcycleregex)


class _MyDiscordClient(discord.Client):
    def __init__(self, config_parser: ConfigParser):
        intents = discord.Intents.none()
        intents.guild_messages = True
        intents.message_content = True
        super().__init__(intents=intents)

        self.channel_id = int(config_parser.get('discord', 'channel'))
        self.upload_config = _config_parse(config_parser)

    async def on_message(self, message: discord.Message):
        """Message, let's go!!!"""
        if not self.is_ready():
            return
        if not message.content or message.content[0] != '!':
            return
        if message.author == self.user:
            return
        if message.channel.id != self.channel_id:
            return

        # Add map through url
        if message.content.startswith('addmap ', 1):
            ret = await add_map(self.loop, self.upload_config,
                                message.content[len('!addmap '):])

            if ret.is_success():
                await self._channel_msg(
                    '%s Success! %s' %
                    (message.author.mention, '\n'.join(ret.get_errors())))
            else:
                await self._channel_msg(
                    '%s Failed to upload map. %s' %
                    (message.author.mention, '\n'.join(ret.get_errors())))

        # Add map to mapcycle (+ add to fast-dl)
        if message.content.startswith('addmapcycle ', 1):
            ret = await add_mapcycle(self.loop, self.upload_config,
                                     message.content[len('!addmapcycle '):])

            if ret.is_success():
                await self._channel_msg(
                    '%s Success! %s' %
                    (message.author.mention, '\n'.join(ret.get_errors())))
            else:
                await self._channel_msg(
                    '%s Failed to add map to mapcycle! %s' %
                    (message.author.mention, '\n'.join(ret.get_errors())))

        # Remove map from mapcycle
        if message.content.startswith('removemapcycle ', 1):
            ret = await remove_mapcycle(self.loop, self.upload_config,
                                        message.content[len('!removemapcycle '):])

            if ret:
                await self._channel_msg(
                    f'{message.author.mention} Removed map from mapcycle.')
            else:
                await self._channel_msg(
                    f'{message.author.mention} Failed to remove map from mapcycle!')

    async def _channel_msg(self, msg: str):
        try:
            await self.get_partial_messageable(self.channel_id).send(msg)
        except discord.DiscordException as e:
            logger.error('Failed to send message: %s', e)


def _main():
    frmt = '[%(asctime)s] [%(levelname)s] [%(threadName)s] %(name)s: %(message)s'
    logging.getLogger('asyncssh').setLevel(logging.WARN)
    logging.getLogger('asyncssh.sftp').setLevel(logging.WARN)
    logging.basicConfig(level=logging.INFO, format=frmt)

    parser = ConfigParser()
    with open(os.path.join(os.path.dirname(__file__), '.config.ini'), encoding='utf-8') as fp:
        parser.read_file(fp)

    client = _MyDiscordClient(parser)

    try:
        client.run(parser.get('discord', 'token'), log_handler=None)
    except discord.LoginFailure:
        logger.error('Failed to log in! Make sure your token is correct!')
        sys.exit(2)
    except:  # pylint: disable=W0702
        logger.error('Something went wrong.', exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    _main()
