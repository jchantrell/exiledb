# ExileDB

[![](https://img.shields.io/github/downloads/jchantrell/exiledb/total?label=Downloads)](https://github.com/jchantrell/exiledb/releases)

> [!IMPORTANT]
> This tool is a work in progress and is being used to test various LLM assisted development methods. I am not accepting PRs but you're welcome to open an issue or fork the repo.

ExileDB is a CLI tool that makes working with Path of Exile 1 & 2's various data formats easier. Its primary purpose is to use the [community's schema](https://github.com/poe-tool-dev/dat-schema) to parse and extract data into local SQLite databases, which makes it significantly easier to query the complex relationships and turn it into more usable formats.

See [releases](https://github.com/jchantrell/exiledb/releases) for pre-built downloads of the executable or alternatively run the below command to install it directly in your CLI.

```bash
go install github.com/jchantrell/exiledb/cmd/exiledb@latest
```

## Assets
I do not own any of the assets utilised in this project. In most cases they are taken from Path of Exile's game files or website dynamically and belong to Grinding Gear Games as a result. If you are the owner of an asset used in this application and wish for it to be removed, please reach out to me via my GitHub email.

## Acknowledgements
- [Path of Exile](https://www.pathofexile.com/) & [Path of Exile 2](https://www.pathofexile2.com/)
- [SnosMe](https://github.com/SnosMe) for inspiration and all of the work on their [dat viewer and library](https://github.com/SnosMe/poe-dat-viewer) and [schema](https://github.com/poe-tool-dev/dat-schema)
- [duskwoof](https://github.com/duskwuff) for [pogo](https://github.com/oriath-net/pogo) and [gooz](https://github.com/oriath-net/gooz)
- [Path of Exile Discord & tooldev channel](https://discord.gg/pathofexile) community 💚
- [PoE Wiki](https://www.poewiki.net/) & [PoE 2 Wiki](https://www.poe2wiki.net/)
