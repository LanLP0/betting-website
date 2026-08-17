#!/usr/bin/env bash
for dir in /home/lanlp/Stuffs/Program/betting-website/src/*/; do
    [ -d "$dir" ] || continue
    (
        cd "$dir" || exit
        echo "> Current directory: $(pwd)"
        cargo sqlx prepare --database-url postgres://admin:admin@localhost:5432/betting_platform
    )
done
