#!/usr/bin/env python3
"""Bucket bootstrap and cleanup for the LOCAL AWS emulator (floci).

  s3_bucket.py ensure <endpoint> <bucket>   create the bucket if it is absent
  s3_bucket.py purge  <endpoint> <bucket>   delete the pipeline's own prefixes
  s3_bucket.py list   <endpoint> <bucket>   print every key

Requests are unsigned, which only an emulator accepts, and the endpoint must be
a loopback address: this can never create or delete anything in real AWS.
purge touches one bucket and only keys under df_*/, reports/, _tmp/ and
_fixtures/; anything else in the bucket is left and reported.
"""
from __future__ import annotations

import re
import sys
import urllib.error
import urllib.parse
import urllib.request

PIPELINE_KEY = re.compile(r"^(df_[^/]+/|reports/|_tmp/|_fixtures/)")
LOOPBACK = {"localhost", "127.0.0.1", "::1"}


def call(method: str, url: str) -> tuple[int, str]:
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")


def list_keys(base: str) -> list[str]:
    keys: list[str] = []
    token = ""
    while True:
        query = "list-type=2" + (
            "&continuation-token=" + urllib.parse.quote(token, safe="") if token else ""
        )
        status, body = call("GET", f"{base}?{query}")
        if status != 200:
            sys.exit(f"listing {base} failed: HTTP {status} {body[:300]}")
        keys += [
            k.replace("&amp;", "&") for k in re.findall(r"<Key>([^<]*)</Key>", body)
        ]
        if "<IsTruncated>true</IsTruncated>" not in body:
            return keys
        found = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", body)
        if not found:
            sys.exit(f"listing {base} is truncated but carries no continuation token")
        token = found.group(1)


def main() -> None:
    if len(sys.argv) != 4 or sys.argv[1] not in {"ensure", "purge", "list"}:
        sys.exit(__doc__)
    action, endpoint, bucket = sys.argv[1], sys.argv[2].rstrip("/"), sys.argv[3]
    host = urllib.parse.urlparse(endpoint).hostname
    if host not in LOOPBACK:
        sys.exit(f"refusing: endpoint host {host!r} is not loopback (emulator only)")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        sys.exit(f"refusing: {bucket!r} is not a valid bucket name")
    base = f"{endpoint}/{bucket}"

    status, _ = call("HEAD", base)
    if action == "ensure":
        if status == 200:
            print(f"    bucket {bucket} already present")
            return
        if status != 404:
            sys.exit(f"HEAD {base} answered HTTP {status}; cannot tell whether the bucket exists")
        created, body = call("PUT", base)
        if created != 200:
            sys.exit(f"creating bucket {bucket} failed: HTTP {created} {body[:300]}")
        if call("HEAD", base)[0] != 200:
            sys.exit(f"bucket {bucket} still absent after create")
        print(f"    bucket {bucket} created")
        return

    if status == 404:
        print(f"    bucket {bucket} does not exist - nothing to do")
        return
    if status != 200:
        sys.exit(f"HEAD {base} answered HTTP {status}")
    keys = list_keys(base)
    if action == "list":
        print("\n".join(keys))
        return

    doomed = [k for k in keys if PIPELINE_KEY.match(k)]
    for key in doomed:
        code, body = call("DELETE", f"{base}/{urllib.parse.quote(key, safe='/')}")
        if code not in (200, 204):
            sys.exit(f"deleting {key} failed: HTTP {code} {body[:300]}")
    left = list_keys(base)
    survivors = [k for k in left if PIPELINE_KEY.match(k)]
    if survivors:
        sys.exit(f"{len(survivors)} pipeline object(s) survived the delete: {survivors[:5]}")
    print(f"    deleted {len(doomed)} pipeline object(s) from s3://{bucket}")
    if left:
        print(f"    left untouched ({len(left)} non-pipeline key(s)): {left[:10]}")


if __name__ == "__main__":
    main()
