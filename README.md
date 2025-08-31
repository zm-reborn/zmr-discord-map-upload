# Discord Map Upload

A Discord bot to upload maps to game servers through Discord.

You can add a map with ```!addmap http://example.com/map.zip``` command. The file can be an uncompressed bsp or compressed bzip2 or zip file.

## Installation (Docker)

```bash
cp .config.template.ini .config.ini

# Configure .config.ini file.

cp docker-compose.template.yml docker-compose.yml

# Configure docker-compose.yml file with paths to your maps directory and mapcycle file.

docker compose up
```

## Development

Use Python 3.13

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
python -m pytest

# Run the bot, remember to fill config :)
python ./run.py
```
