#!/bin/sh
# Run cargo deny.  Fetch the advisory DB and re-stamp the freshness file
# only when the existing stamp is missing or older than 3 days; otherwise
# run the offline check and leave the stamp untouched.  This mirrors the
# freshness skip in the pre-commit hook.

cd "$(git rev-parse --show-toplevel)" || exit 1

STAMP="${CARGO_TARGET_DIR:-target}/last-update-deny"
if [ ! -f "$STAMP" ]; then
    DENY_ALIAS=deny-check-update
else
    NOW=$(date +%s)
    if [ "$(uname)" = "Darwin" ]; then
        STAMP_AGE=$(stat -f %m "$STAMP")
    else
        STAMP_AGE=$(stat -c %Y "$STAMP")
    fi
    if [ $(( NOW - STAMP_AGE )) -gt 259200 ]; then
        AGE_DAYS=$(( (NOW - STAMP_AGE) / 86400 ))
        echo "cargo deny advisory DB is ${AGE_DAYS} days old at $STAMP, so fetching anew."
        DENY_ALIAS=deny-check-update
    else
        DENY_ALIAS=deny-check-offline
    fi
fi

if cargo $DENY_ALIAS; then
    if [ "$DENY_ALIAS" = "deny-check-update" ]; then
        date > "$STAMP"
        echo "Stamped $STAMP at $(cat "$STAMP")"
    fi
fi
