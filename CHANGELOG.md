# Changelog

## [1.6.0](https://github.com/jchantrell/exiledb/compare/v1.5.0...v1.6.0) (2026-07-05)


### Features

* add upgrade command for self-updating from GitHub releases ([ee0de48](https://github.com/jchantrell/exiledb/commit/ee0de488f60adf01b0a85e09bbb787580a77a382))
* add version command with build-time version embedding ([9b8c978](https://github.com/jchantrell/exiledb/commit/9b8c978778ba2b86c1d50eb02cdf95dbc1db4864))


### Bug Fixes

* **bundle,ggpk:** reject corrupt input, collapse to one Index type ([a99b951](https://github.com/jchantrell/exiledb/commit/a99b95169baff5ab8d64ab661b4062bd3c64e573))
* **cdn:** atomic parallel bundle downloads ([eddb259](https://github.com/jchantrell/exiledb/commit/eddb259e96b59beaadf4b1cde73115c32ac90a6f))
* **database:** apply SQLite pragmas, document-only foreign keys, per-table insert plan ([5803256](https://github.com/jchantrell/exiledb/commit/5803256369c3dbb57442ac915cb40b3fdd6a797a))
* **dat:** harden parser against malformed input, add FieldType codec registry ([d1b30dd](https://github.com/jchantrell/exiledb/commit/d1b30dddd42960ab70e31f1f3d0c128a6bedcae3))
* **dds:** validate decode inputs, split into shared block driver per codec ([c5f73a4](https://github.com/jchantrell/exiledb/commit/c5f73a4f0f36204f10180d27643a148428706e10))
* **export:** correct sprite crop geometry and sheet discovery, honest export accounting ([435c611](https://github.com/jchantrell/exiledb/commit/435c6113a216a167e0aaef511a5eaf460c2ab20a))

## [1.5.0](https://github.com/jchantrell/exiledb/compare/v1.4.0...v1.5.0) (2026-07-05)


### Features

* add --sizes flag to manifest for uncompressed byte counts ([c063c3a](https://github.com/jchantrell/exiledb/commit/c063c3a637ecf8956b6f8deb80456d2de8821967))


### Bug Fixes

* skip empty diff assets, GitHub rejects zero-byte uploads ([222a66b](https://github.com/jchantrell/exiledb/commit/222a66bf660a899afd0a188b62da197ab6c25f04))

## [1.4.0](https://github.com/jchantrell/exiledb/compare/v1.3.0...v1.4.0) (2026-07-04)


### Features

* add manifest and diff commands for tracking files across game versions ([7e1174b](https://github.com/jchantrell/exiledb/commit/7e1174b8c6c87a4fe5a7068b4d46e364fb8139bc))
* decompress Oodle-compressed animation payload in .ast files ([8fde26c](https://github.com/jchantrell/exiledb/commit/8fde26c9e8d49750b0578264c67baf8e81343e2e))
* replace ImageMagick with native Go DDS decoder ([85fdb7c](https://github.com/jchantrell/exiledb/commit/85fdb7c5ce3b0de1320ac5d12e5fb2a180b578d0))
* support partial path matching for file extraction ([a570755](https://github.com/jchantrell/exiledb/commit/a5707556ff74c1305941f43308f47c1911162338))


### Bug Fixes

* gracefully handle non-UTF-16LE text files during extraction ([2d079b9](https://github.com/jchantrell/exiledb/commit/2d079b9db2a79e49abd0ee97f960fd355d53d39c))
* handle DXT1 1-bit alpha and skip unsupported DDS formats ([e358cc1](https://github.com/jchantrell/exiledb/commit/e358cc1a461b0666b3a3a8ab4a004ef2faa9ce22))
* race condition in progress bar description updates ([e604941](https://github.com/jchantrell/exiledb/commit/e604941f28a49b38e0200479a550d16d3442cea1))
* scan animation headers instead of trusting u8 animCount ([ce0a205](https://github.com/jchantrell/exiledb/commit/ce0a205ea19519f9a86f3afdc347c2b1d01d35d5))
* skip table extraction when --tables not provided ([e538a78](https://github.com/jchantrell/exiledb/commit/e538a7877719b32a6b0f1feb1017d20c79de6d39))


### Performance Improvements

* cache opened bundles to avoid redundant parsing ([ed074d7](https://github.com/jchantrell/exiledb/commit/ed074d7740889a31945dabde298543fc4636ee6e))
* cache parsed bundle index to disk ([7a68f84](https://github.com/jchantrell/exiledb/commit/7a68f8415dbda2cac86c34869b2194359d25152e))
* parallelize file extraction with worker pool ([26aa5d6](https://github.com/jchantrell/exiledb/commit/26aa5d6a7d1082688c15479cd312d543ae192ad1))
* remove unused raw slice accumulation in DDS decompressors ([fd8315f](https://github.com/jchantrell/exiledb/commit/fd8315f2d7bfcf8d1ae5354c5bb485d706383a7f))
* skip already exported files during extraction ([aeeabeb](https://github.com/jchantrell/exiledb/commit/aeeabebab6f71124e8d57b1cd94a3185fa867133))
* sort files by bundle before extraction ([3346a27](https://github.com/jchantrell/exiledb/commit/3346a2749591d07289c2d7fe127c101086389968))
* use BestSpeed PNG compression level ([b79ba83](https://github.com/jchantrell/exiledb/commit/b79ba8330ff98c7c6e5c9a2a3edce1482e4459b6))
* use binary search for path filtering in list command ([c93367a](https://github.com/jchantrell/exiledb/commit/c93367ad1d7016b94c1c6425654444d827f55e17))

## [1.3.0](https://github.com/jchantrell/exiledb/compare/v1.2.0...v1.3.0) (2026-05-19)


### Features

* add GGPK file support for extract and list commands ([5a53896](https://github.com/jchantrell/exiledb/commit/5a538968ebabf63bd0dff1f728752cf6a291ce8a))
* add list command for browsing bundle index contents ([807f4d5](https://github.com/jchantrell/exiledb/commit/807f4d504049db03d0074cda728e6d18af46b8af))
* include unnamed schema columns in database ([c8d9d1f](https://github.com/jchantrell/exiledb/commit/c8d9d1fe075d733a3c9c7212bc05ee10e5989d0c))


### Bug Fixes

* allow file-only extraction without processing tables ([f67ba9d](https://github.com/jchantrell/exiledb/commit/f67ba9d58111ccb04e606f38af644dbf43ffdeb2))
* correct PoE2 language path to data/balance/{lang}/{table} ([a43269c](https://github.com/jchantrell/exiledb/commit/a43269cb37a49de7a20746e6cf74069364ef91c0))
* join untranslated tables on English in mods query ([ed1958a](https://github.com/jchantrell/exiledb/commit/ed1958aea720f6d33870721295c677022fa2c528))
* prevent hang when exported files don't exist ([3d377dd](https://github.com/jchantrell/exiledb/commit/3d377dd5895a4b82eac36fee33747d9f4b62b2f2))
* use data/balance/ path prefix for PoE2 dat files ([807b5c5](https://github.com/jchantrell/exiledb/commit/807b5c5b753ece0b68e0f397b0d972bd80f8191c))
* use stats.id instead of nonexistent stats.text in mods query ([9a57afb](https://github.com/jchantrell/exiledb/commit/9a57afb2e5fd083d491cd8304d5e8af215d243bf))

## [1.2.0](https://github.com/jchantrell/exiledb/compare/v1.1.0...v1.2.0) (2025-10-20)


### Features

* file export support ([af22f3c](https://github.com/jchantrell/exiledb/commit/af22f3cb2d5f254b5e3e3cbc1cb6afd635fa9a17))

## [1.1.0](https://github.com/jchantrell/exiledb/compare/v1.0.6...v1.1.0) (2025-09-20)


### Features

* consolidate array field parsing ([7f5e030](https://github.com/jchantrell/exiledb/commit/7f5e0305af180e3a3ae0ae7c15f9da404e02700b))
* consolidate array parsing ([bdb0b57](https://github.com/jchantrell/exiledb/commit/bdb0b57ea027663c776d561278a1ce19a0f603b5))
* consolidate scalar field parsing ([6b63521](https://github.com/jchantrell/exiledb/commit/6b63521f9e8da312673bda4fbd7f38b96d869c32))
* consolidate string array parsing ([80591e2](https://github.com/jchantrell/exiledb/commit/80591e262cfb01fc62b5ed46548d90fdd6a05b0f))
* consolidate string parsing ([6a7c9f4](https://github.com/jchantrell/exiledb/commit/6a7c9f4da8ff2694c9e1ea44bfd4680d0074a06b))
* more clean up ([30735c4](https://github.com/jchantrell/exiledb/commit/30735c4e1a60b4d31d7bdd3f72355b60045eba5f))
* update progress bar layout ([1be8fb2](https://github.com/jchantrell/exiledb/commit/1be8fb20e32e323bb82498f3cdb069ff56440198))


### Bug Fixes

* fetch files for all languages ([1f08d4d](https://github.com/jchantrell/exiledb/commit/1f08d4d759aba1e58c057587867cda84d34b228e))
* readd dat parser opts ([b484d09](https://github.com/jchantrell/exiledb/commit/b484d09f71ede32da55b8d06204b8b032ca59cf2))

## [1.0.6](https://github.com/jchantrell/exiledb/compare/v1.0.5...v1.0.6) (2025-09-18)


### Bug Fixes

* handle windows filepaths ([21449b3](https://github.com/jchantrell/exiledb/commit/21449b3a0d173d760f8db6feee3b114085b3cc7e))

## [1.0.5](https://github.com/jchantrell/exiledb/compare/v1.0.4...v1.0.5) (2025-09-18)


### Bug Fixes

* remainder of deploy issues ([c655297](https://github.com/jchantrell/exiledb/commit/c655297f57abcad80a472f05d2b54f859ba76605))

## [1.0.4](https://github.com/jchantrell/exiledb/compare/v1.0.3...v1.0.4) (2025-09-18)


### Bug Fixes

* remove verbose flag ([a3ad493](https://github.com/jchantrell/exiledb/commit/a3ad4937b81405aa2b42d13114c956ff318fed26))

## [1.0.3](https://github.com/jchantrell/exiledb/compare/v1.0.2...v1.0.3) (2025-09-18)


### Bug Fixes

* use correct file name for uploading artifact ([e88c236](https://github.com/jchantrell/exiledb/commit/e88c2360db93f4824fe0c9988f1eac80811e1b5c))

## [1.0.2](https://github.com/jchantrell/exiledb/compare/v1.0.1...v1.0.2) (2025-09-18)


### Bug Fixes

* remove mac from release workflow ([d13b6e5](https://github.com/jchantrell/exiledb/commit/d13b6e5888e46a4967bf6ae0e02e2b350633cb2a))

## [1.0.1](https://github.com/jchantrell/exiledb/compare/v1.0.0...v1.0.1) (2025-09-18)


### Bug Fixes

* update release workflow ([cf32e10](https://github.com/jchantrell/exiledb/commit/cf32e1007cf3434d883e4174eb1b536cf9106a00))

## 1.0.0 (2025-09-18)


### Features

* initial commit ([331de11](https://github.com/jchantrell/exiledb/commit/331de11f0b172859ff829a19aa55b5e64aa7e5ce))
