"""
The feeder: fetch new scanned Acts so nobody has to supply the input.

Pipeline B reads a folder. Until now a person filled that folder by hand,
which made the pipeline unattended but not automatic -- it consumed a fixed
pile of documents and stopped. This closes that: given a page count, it finds
Acts it has not seen, downloads them, and leaves them where generate.py will
pick them up on the next pass.

WHY THIS EXISTS NOW AND NOT BEFORE. The project parked automated fetching in
August because documents.gov.lk is a JavaScript CMS with no sitemap, a 404
robots.txt and no discoverable URL pattern -- the risk of building on an
unverified dependency was not worth it. That objection is answered: the route
below was used on 2026-09-28 to download 314 Acts, and it is the site's own
mechanism rather than a guess at one.

HOW THE SITE ACTUALLY WORKS, because it is not obvious and the page source
gives nothing away:

    /web/acts renders its list through a Next.js SERVER ACTION, not a REST
    endpoint. Posting the action id below to that path returns all 1,733 Acts
    as JSON in an RSC stream -- Act number, year, title, and a file path per
    language. The PDF then comes from /api/content-file-proxy.

    There is no documented API. If the site is rebuilt, ACTION_ID changes and
    this stops working. That is a real fragility and the reason the folder,
    not this file, remains Pipeline B's interface: when this breaks, the
    pipeline keeps running on whatever a person puts in the folder.
"""

import io
import json
import os
import time
import urllib.parse
import urllib.request

BASE = "https://documents.gov.lk"
ACTS_PAGE = BASE + "/web/acts"
FILE_PROXY = BASE + "/api/content-file-proxy?file=/"

#: The server action behind the Acts listing. Lifted from the page's own
#: JavaScript bundle, where it is registered as "TableDataAction".
ACTION_ID = "7f679bcdf1c679caa0f2356b97387098c39c41312a"
API_ENDPOINT = "http://gvp-api:4500/website-data/act/get-all"

UA = "Mozilla/5.0"

#: Acts held back from fetching are read from the SinhaLegal cache -- see
#: _trained_on(). Nothing is hard-coded here any more.
#:
#: Seven Acts used to be listed: 2, 4, 6 and 7 of 1982 and 75, 76 and 78 of
#: 1981, which trained the retired v1 corrector. The current corrector was
#: trained on SinhaLegal and never saw them, so holding them back was denying
#: the pipeline seven documents it is free to read. Removed when v1 was
#: deleted on 2026-09-30.


def _get(url, timeout=90):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    return urllib.request.urlopen(req, timeout=timeout).read()


def catalogue(cache_path, max_age_hours=24):
    """
    Every Act the site lists, as [(act_no, year, title, pdf_path)].

    Cached on disk. The listing is 1.2 MB and changes a few times a year, so
    re-fetching it on every cycle would be rude to a government server for no
    benefit.
    """
    fresh = (os.path.exists(cache_path)
             and time.time() - os.path.getmtime(cache_path) < max_age_hours * 3600)
    if not fresh:
        body = json.dumps([{
            "apiEndpoint": API_ENDPOINT, "page": 1, "limit": 2000,
            "q": "", "search": "",
        }]).encode()
        req = urllib.request.Request(
            ACTS_PAGE, data=body,
            headers={"User-Agent": UA, "Next-Action": ACTION_ID,
                     "Content-Type": "text/plain;charset=UTF-8",
                     "Referer": ACTS_PAGE},
        )
        raw = urllib.request.urlopen(req, timeout=120).read().decode("utf-8")
        os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
        io.open(cache_path, "w", encoding="utf-8").write(raw)

    raw = io.open(cache_path, encoding="utf-8").read()
    line = next(l for l in raw.split("\n") if l.startswith("1:"))
    out = []
    for a in json.loads(line[2:])["data"]:
        sin = [c for c in a["contents"] if c["language"] == "SINHALA"]
        if sin:
            out.append((a["actNo"], a["actSubNo"],
                        (a["descriptionEnglish"] or "").strip(),
                        sin[0]["uploadedFile"]))
    return out


def _trained_on(repo_root):
    """
    Acts whose pages actually went into a corrector's training set.

    The SinhaLegal cache holds one file per Act CONSIDERED, not per Act used
    -- most were rejected as born-digital or had no reference text and
    contributed nothing. Holding those back would deny the pipeline input it
    is free to take, so the row count decides, not the filename.
    """
    used = set()
    cache = os.path.join(repo_root, "data", "sinhalegal", "_cache")
    if os.path.isdir(cache):
        for name in os.listdir(cache):
            if not name.endswith(".json"):
                continue
            try:
                year, act = name[:-5].split("_")
                with io.open(os.path.join(cache, name), encoding="utf-8") as fh:
                    if json.load(fh).get("rows"):
                        used.add((int(act), int(year)))
            except (ValueError, OSError, json.JSONDecodeError):
                continue
    return used


def fetch_new(input_dir, want, repo_root, cache_path=None, pause=1.0):
    """
    Download up to `want` Acts the folder does not already hold.

    Returns (downloaded, skipped_trained, failed).

    Oldest first, deliberately. Acts before 2000 are scans of paper; from the
    mid-2000s documents.gov.lk increasingly publishes born-digital PDFs, and
    classify_pdf() throws those out. Fetching oldest-first spends the
    bandwidth where the pages are usable.
    """
    os.makedirs(input_dir, exist_ok=True)
    cache_path = cache_path or os.path.join(repo_root, "data", "_acts_catalogue.txt")

    try:
        acts = catalogue(cache_path)
    except Exception as exc:                                  # noqa: BLE001
        print(f"  [feeder] catalogue unavailable: {type(exc).__name__}: {exc}")
        return 0, 0, 0

    have = set(os.listdir(input_dir))
    skip = _trained_on(repo_root)

    candidates = [a for a in acts
                  if os.path.basename(a[3]) not in have
                  and (a[0], a[1]) not in skip]
    candidates.sort(key=lambda a: (a[1], a[0]))

    trained_out = sum(1 for a in acts if (a[0], a[1]) in skip)
    got = failed = 0
    for act_no, year, _title, path in candidates:
        if got >= want:
            break
        dest = os.path.join(input_dir, os.path.basename(path))
        try:
            body = _get(FILE_PROXY + urllib.parse.quote(path))
        except Exception as exc:                              # noqa: BLE001
            print(f"  [feeder] {act_no}/{year} failed: {type(exc).__name__}")
            failed += 1
            continue
        if not body.startswith(b"%PDF"):
            failed += 1
            continue
        io.open(dest, "wb").write(body)
        got += 1
        print(f"  [feeder] + {act_no}/{year}  {len(body)/1024:.0f} KB  "
              f"{os.path.basename(path)}")
        time.sleep(pause)          # a government server, not a CDN

    if got == 0 and not failed:
        print("  [feeder] nothing new to fetch "
              f"({len(candidates)} candidates, {trained_out} held back as "
              "training documents)")
    return got, trained_out, failed
