#!/bin/bash

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

echo "Starting Jupyter Book watcher..."
uv run jupyter-book clean .
uv run jupyter-book build .
uv run watchmedo shell-command \
    --patterns="*.md" \
    --recursive \
    --drop \
    --command='echo "Rebuilding..." && uv run jupyter-book clean . && uv run jupyter-book build .' \
    .
