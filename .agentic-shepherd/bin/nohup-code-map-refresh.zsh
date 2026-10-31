#!/usr/bin/env zsh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

nohup "$SCRIPT_DIR/code-map-refresh-live.zsh" > /dev/null 2>&1 &
echo "Started code-map-refresh-live.zsh in background (PID: $!)"