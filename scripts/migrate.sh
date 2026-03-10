#!/usr/bin/env sh
set -eu

exec alembic -c backend/alembic.ini upgrade head
