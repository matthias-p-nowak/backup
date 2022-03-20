#!/bin/bash
PP="topsecret"
exec 2>&1
for F in $*
do
  echo "### showing ${F}"
  unxz <$F | gpg -d --passphrase "${PP}" --batch | tar tvf -
done
