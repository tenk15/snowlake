#!/usr/bin/env bash

src=./src

uv run ruff format $src
uv run ruff check --fix $src
uv run pyright $src
