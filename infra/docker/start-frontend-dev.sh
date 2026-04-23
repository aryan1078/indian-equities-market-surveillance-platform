#!/bin/sh
set -eu

cd /app/frontend
rm -rf .next

npm run build
exec npm run start -- --hostname 0.0.0.0 --port 3000
