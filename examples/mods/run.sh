#!/bin/bash

for lang in "English" "French" "German" "Russian"; do LANG="$lang" envsubst < examples/mods/query.sql | sqlite3 exile.db | jq > "examples/mods/${lang}.json"; done
