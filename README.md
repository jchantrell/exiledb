# ExileDB

[![](https://img.shields.io/github/downloads/jchantrell/exiledb/total?label=Downloads)](https://github.com/jchantrell/exiledb/releases)

> [!IMPORTANT]
> This tool is a work in progress. I am not accepting PRs but you're welcome to open an issue or fork the repo.

ExileDB is a CLI tool that makes working with Path of Exile 1 & 2's various data formats easier. Its primary purpose is to use the [community's schema](https://github.com/poe-tool-dev/dat-schema) to parse and extract data into local SQLite databases, which makes it significantly easier to query the complex relationships and turn it into more usable formats.

The schema's latest release is published at [schema.min.json](https://github.com/poe-tool-dev/dat-schema/releases/download/latest/schema.min.json). To visually explore the game files it describes, use [poe-dat-viewer](https://snosme.github.io/poe-dat-viewer/).

See [releases](https://github.com/jchantrell/exiledb/releases) for pre-built binaries for Linux and Windows or alternatively run the below command to install it directly in your CLI:

Install it:
```bash
go install github.com/jchantrell/exiledb/cmd/exiledb@latest
```

Build from source:
```bash
git clone https://github.com/jchantrell/exiledb.git
cd exiledb
go build -o exiledb ./cmd/exiledb/
```

How to use:
```bash
# Print the current version
exiledb version

# Upgrade to the latest release (use --check to only look for new versions)
exiledb upgrade

# Browse the bundle index to find files and directories
exiledb list                     # list root directory
exiledb list --path data/balance # list a specific path

# Or dump every file path in the game (one per line) and grep it
exiledb manifest --patch 4.4.0.13 > 4.4.0.13.txt
grep waystone 4.4.0.13.txt

# Append uncompressed byte counts to each path (tab-separated)
exiledb manifest --patch 4.4.0.13 --sizes > 4.4.0.13-sizes.txt

# Diff manifests to see what changed between game versions
diff 4.4.0.12.txt 4.4.0.13.txt

# Or skip all of the above: manifests, added/removed file lists and dat file
# diffs are published for every patch under the data-poe1-* / data-poe2-* releases
https://github.com/jchantrell/exiledb/releases

# Download bundles and extract data to DB (exile.db by default)
exiledb extract --patch 4.4.0.13 --tables BaseItemTypes,ItemClasses

# Or extract directly from a Content.ggpk file instead of downloading from CDN
exiledb list --ggpk /path/to/Content.ggpk
exiledb extract --ggpk /path/to/Content.ggpk

# Then query it with any SQLite client. Tables are named after their schema
# counterparts (BaseItemTypes -> base_item_types) and rows reference each
# other by _index within the same _language
sqlite3 exile.db "
  SELECT bit.name AS item, ic.name AS class
  FROM base_item_types bit
  JOIN item_classes ic ON bit._language = ic._language AND bit.item_class = ic._index
  WHERE bit._language = 'English' AND ic.name = 'Stackable Currency'
  LIMIT 3;"
# Blacksmith's Whetstone|Stackable Currency
# Arcanist's Etcher|Stackable Currency
# Scroll of Wisdom|Stackable Currency
```

For more involved queries (items and mods joined across many tables and exported to JSON per language), see [examples](./examples/).

## Assets
I do not own any of the assets utilised in this project. In most cases they are taken from Path of Exile's game files or website dynamically and belong to Grinding Gear Games as a result. If you are the owner of an asset used in this application and wish for it to be removed, please reach out to me via my GitHub email.

## Acknowledgements
- [Path of Exile](https://www.pathofexile.com/) & [Path of Exile 2](https://www.pathofexile2.com/)
- [SnosMe](https://github.com/SnosMe) for inspiration and all of the work on their [dat viewer and library](https://github.com/SnosMe/poe-dat-viewer) and [schema](https://github.com/poe-tool-dev/dat-schema)
- [duskwoof](https://github.com/duskwuff) for [pogo](https://github.com/oriath-net/pogo) and [gooz](https://github.com/oriath-net/gooz)
- [Path of Exile Discord & tooldev channel](https://discord.gg/pathofexile) community 💚
- [PoE Wiki](https://www.poewiki.net/) & [PoE 2 Wiki](https://www.poe2wiki.net/)
