#!/bin/sh
set -eu

cd /app/frontend
rm -rf .next

npm run build
mkdir -p .next/standalone/.next
rm -rf .next/standalone/.next/static .next/standalone/public
cp -R .next/static .next/standalone/.next/static
cp -R public .next/standalone/public
export HOSTNAME=0.0.0.0
export PORT=3000
exec node .next/standalone/server.js
