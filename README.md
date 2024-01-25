# Discord Map Upload

A Discord bot to upload maps to game servers through Discord.

You can add a map with ```!addmap http://example.com/map.zip``` command. The file must be a zip.

## Installation (Docker)

```bash
cp .config.template.ini .config.ini

# Configure .config.ini file.

cp docker-compose.template.yml docker-compose.yml

# Configure docker-compose.yml file with paths to your maps directory and mapcycle file.

docker compose up
```
