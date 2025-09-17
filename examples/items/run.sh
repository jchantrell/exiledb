#!/bin/bash

for lang in "English" "French" "German" "Russian"; do LANG="$lang" envsubst < examples/items/query.sql | sqlite3 exile.db | jq '.[].tags |= fromjson' > "examples/items/${lang}.json"; done
