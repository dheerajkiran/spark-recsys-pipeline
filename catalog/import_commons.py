"""Import bounded, credited Wikimedia Commons video metadata; never media bytes.

This builds an independently identified playable catalog. Commons items are not
KuaiRand items and are not assigned predictions from the existing DeepFM model.
Author/license attribution is retained per item. Media stays at its source.
"""

import argparse
from collections import Counter
from datetime import datetime, timezone
import email.utils
import hashlib
from html.parser import HTMLParser
import json
import math
from pathlib import Path
import re
import sqlite3
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
API = "https://commons.wikimedia.org/w/api.php"
USER_AGENT = "LoopCatalogPreview/0.1 (https://github.com/dheerajkiran/spark-recsys-pipeline)"
TOPICS = {"Nature", "Travel", "Food", "Design", "Science"}
PROGRESS_KEY = "commons_import_progress_v1"
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
SEARCH_BUSY_CODES = {"cirrussearch-too-busy-error"}
EXCLUDED_SUBJECT = re.compile(
    r"\b(?:porn(?:ography|ographic)?|sexual|sex|nude|nudity|naked|genitals?|penis|vagina|"
    r"masturbat\w*|intercourse|autopsy|surgery|surgical|dissection|amputation|cadavers?|"
    r"corpses?|gore|blood|bloodied|beheading|executions?|murder|torture|gunfire|combat|"
    r"violent|violence|mass\s+shooting|school\s+shooting)\b",
    re.IGNORECASE,
)
MEDIA_TYPE = re.compile(r'video/(?:webm|mp4|quicktime)(?:; ?codecs="[a-z0-9., ]{1,40}")?')
INSERT = ("INSERT INTO videos(id,source,source_id,category,creator_key,search_text,payload_json,imported_at) "
          "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING")


class SourceError(RuntimeError):
    """The upstream response could not be used without guessing metadata."""


class RequestBudgetReached(SourceError):
    pass


class SkipVideo(ValueError):
    """A file does not satisfy the explicit import policy."""


class _PlainHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.hidden = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.hidden += 1
        elif not self.hidden and tag in ("br", "p", "div", "li"):
            self.parts.append(" ")

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self.hidden:
            self.hidden -= 1
        elif not self.hidden and tag in ("p", "div", "li"):
            self.parts.append(" ")

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def plain_text(value, limit=500, *, input_limit=20_000):
    """Upstream HTML is text, never trusted page markup."""
    if not isinstance(value, str):
        return ""
    parser = _PlainHTML()
    parser.feed(value[:input_limit])
    text = re.sub(r"[\x00-\x1f\x7f]", " ", "".join(parser.parts))
    return " ".join(text.split())[:limit].strip()


def _url(value, hosts):
    if not isinstance(value, str) or len(value) > 3000 or any(ord(char) < 32 for char in value):
        return None
    try:
        parsed = urlsplit(value)
        if (parsed.scheme != "https" or parsed.hostname not in hosts or parsed.username
                or parsed.password or parsed.port not in (None, 443) or parsed.fragment):
            return None
    except ValueError:
        return None
    return value


def _metadata(info, name):
    metadata = info.get("extmetadata", {})
    if not isinstance(metadata, dict):
        return ""
    value = metadata.get(name, {})
    return value.get("value", "") if isinstance(value, dict) else ""


def _license(info):
    name = plain_text(_metadata(info, "LicenseShortName"), 80)
    candidate = _metadata(info, "LicenseUrl")
    # Older source templates use HTTP for the same canonical CC terms.
    if isinstance(candidate, str) and candidate.startswith(("http://creativecommons.org/", "http://www.creativecommons.org/")):
        candidate = "https://" + candidate.removeprefix("http://")
    url = _url(candidate, {"creativecommons.org", "www.creativecommons.org"})
    if not url:
        raise SkipVideo("missing_license_url")
    path = urlsplit(url).path.rstrip("/")
    match = re.fullmatch(r"/licenses/(by|by-sa)/(1\.0|2\.0|2\.5|3\.0|4\.0)(?:/deed(?:\.[a-z-]+)?)?", path)
    if match:
        expected = f"CC {match[1].upper()} {match[2]}"
        if name.upper() != expected:
            raise SkipVideo("inconsistent_license")
        return {"name": expected, "url": url}
    if re.fullmatch(r"/publicdomain/zero/1\.0(?:/deed(?:\.[a-z-]+)?)?", path) and name.lower() in ("cc0", "cc0 1.0", "cc0 1.0 universal"):
        return {"name": "CC0 1.0", "url": url}
    if re.fullmatch(r"/publicdomain/mark/1\.0(?:/deed(?:\.[a-z-]+)?)?", path) and name.lower() in ("public domain", "public domain mark", "pdm"):
        return {"name": "Public domain", "url": url}
    raise SkipVideo("unsupported_license")


def _media_type(value):
    """Keep a derivative's codecs parameter only when it has a known, plain form."""
    value = str(value or "").strip()
    return value if MEDIA_TYPE.fullmatch(value) else value.split(";", 1)[0].strip()


def select_sources(info):
    """Choose low-resolution derivatives: (WebM/MP4 URL, its type, ordered sources).

    Safari plays Commons VP9 WebM audio without rendering frames inside the
    feed, yet reports it as playable, so it cannot be feature-detected. The
    QuickTime/MPEG-4 derivative renders in Safari while Chrome and Firefox
    report it unplayable. Listing it first lets each browser take the first
    type it accepts. Items without it are skipped so every clip has a picture.
    """
    primary, apple = [], []
    derivatives = info.get("derivatives", [])
    for derivative in derivatives if isinstance(derivatives, list) else []:
        if not isinstance(derivative, dict) or not derivative.get("transcodekey"):
            continue
        full_type = _media_type(derivative.get("type"))
        media_type = full_type.split(";", 1)[0]
        if media_type not in ("video/webm", "video/mp4", "video/quicktime"):
            continue
        video_url = _url(derivative.get("src"), {"upload.wikimedia.org"})
        height, width = derivative.get("height"), derivative.get("width")
        bandwidth = derivative.get("bandwidth", 0)
        if (not video_url or "/transcoded/" not in urlsplit(video_url).path
                or type(height) is not int or type(width) is not int
                or not 144 <= height <= 480 or not 1 <= width <= 1600
                or type(bandwidth) not in (int, float) or not math.isfinite(bandwidth)
                or bandwidth < 0 or bandwidth > 2_500_000):
            continue
        option = (abs(height - 360), bandwidth, video_url, full_type)
        if media_type == "video/quicktime":
            if "mp4v" in full_type and urlsplit(video_url).path.lower().endswith(".mov"):
                apple.append(option)
        else:
            primary.append(option)
    if not primary:
        raise SkipVideo("no_browser_derivative")
    if not apple:
        raise SkipVideo("no_safari_derivative")
    _, _, video, video_type = min(primary)
    _, _, mov, mov_type = min(apple)
    return video, video_type.split(";", 1)[0], [{"src": mov, "type": mov_type}, {"src": video, "type": video_type}]


def normalize_page(page, category, query, max_duration=120):
    """Convert one real Commons video into the shared frontend post contract."""
    if category not in TOPICS or not isinstance(page, dict):
        raise SkipVideo("invalid_page")
    page_id = page.get("pageid")
    if type(page_id) is not int or page_id < 1:
        raise SkipVideo("missing_source_id")
    revisions = page.get("videoinfo")
    if not isinstance(revisions, list) or not revisions or not isinstance(revisions[0], dict):
        raise SkipVideo("missing_video_info")
    info = revisions[0]
    duration = info.get("duration")
    if type(duration) not in (int, float) or not math.isfinite(duration) or not 1 <= duration <= max_duration:
        raise SkipVideo("duration_out_of_range")
    if not str(info.get("mime", "")).startswith("video/"):
        raise SkipVideo("not_video")
    source_url = _url(info.get("descriptionurl"), {"commons.wikimedia.org"})
    if not source_url or not urlsplit(source_url).path.startswith("/wiki/File:"):
        raise SkipVideo("missing_source_page")
    poster = _url(info.get("thumburl"), {"upload.wikimedia.org", "thumb.wikimedia.org"})
    if not poster or not urlsplit(poster).path.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
        raise SkipVideo("missing_thumbnail")
    license_info = _license(info)
    artist = plain_text(_metadata(info, "Artist"), 16_001, input_limit=MAX_RESPONSE_BYTES)
    if not artist or artist.lower() in ("unknown", "unknown author", "author unknown", "n/a"):
        raise SkipVideo("missing_author")
    if len(artist) > 16_000:
        raise SkipVideo("attribution_too_long")
    video, media_type, sources = select_sources(info)
    title = plain_text(_metadata(info, "ObjectName"), 180)
    if not title:
        title = plain_text(re.sub(r"\.(?:webm|ogv|ogg|mp4|mov)$", "", str(page.get("title", "")).removeprefix("File:"), flags=re.I).replace("_", " "), 180)
    if not title:
        raise SkipVideo("missing_title")
    description = plain_text(_metadata(info, "ImageDescription"), 600) or title
    # A coarse default exclusion, not a claim of comprehensive moderation.
    if EXCLUDED_SUBJECT.search(title + " " + description):
        raise SkipVideo("excluded_subject")
    handle = "commons-" + hashlib.sha256(artist.casefold().encode()).hexdigest()[:12]
    initials = "".join(part[0] for part in artist.split()[:2]).upper()[:2] or "WC"
    tags = list(dict.fromkeys([category.lower(), *re.findall(r"[\w-]+", query.lower())[:4]]))
    return {
        "id": f"commons:{page_id}", "creator": artist[:160].strip(), "handle": handle,
        "initials": initials, "avatarColor": "#cbd6bd", "title": title,
        "description": description, "tags": tags, "category": category,
        "video": video, "sources": sources, "poster": poster, "likes": 0, "comments": [],
        "duration": float(duration), "reason": f"Discovered in the {category} catalog.",
        "location": "Wikimedia Commons",
        "source": {"provider": "wikimedia", "id": str(page_id), "url": source_url,
                   "license": license_info, "attribution": artist, "demo": False,
                   "kuairandId": None, "mediaType": media_type},
    }


def load_sources(path=ROOT / "catalog/commons_sources.json"):
    manifest = json.loads(Path(path).read_text())
    if manifest.get("version") != 1 or manifest.get("provider") != "wikimedia" or manifest.get("api") != API:
        raise ValueError("Use a version-1 Wikimedia source manifest with the approved API origin")
    queries = manifest.get("queries")
    if not isinstance(queries, list) or not 1 <= len(queries) <= 100:
        raise ValueError("The source manifest needs 1 to 100 queries")
    result = []
    for entry in queries:
        if (not isinstance(entry, dict) or entry.get("category") not in TOPICS
                or not isinstance(entry.get("query"), str) or not entry["query"].strip()
                or len(entry["query"]) > 150):
            raise ValueError("Source queries need supported categories and bounded search text")
        item = {"category": entry["category"], "query": entry["query"].strip()}
        if item not in result:
            result.append(item)
    return result


def _retry_delay(value, fallback):
    if not value:
        return fallback
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            seconds = (email.utils.parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return fallback
    if not math.isfinite(seconds) or seconds > 30:
        raise SourceError("The source requested a longer pause; retry this import later")
    return max(0, seconds)


class CommonsClient:
    """Serial API requests with a shared attempt budget and bounded retries."""

    def __init__(self, max_requests=400, delay=0.5, *, opener=urlopen, sleeper=time.sleep,
                 monotonic=time.monotonic, retries=2):
        self.max_requests, self.delay, self.retries = max_requests, delay, retries
        self.opener, self.sleeper, self.monotonic = opener, sleeper, monotonic
        self.requests, self.last_request = 0, None

    def query(self, params):
        for attempt in range(self.retries + 1):
            if self.requests >= self.max_requests:
                raise RequestBudgetReached("API request budget reached")
            if self.last_request is not None:
                self.sleeper(max(0, self.delay - (self.monotonic() - self.last_request)))
            self.requests += 1
            self.last_request = self.monotonic()
            request = Request(API + "?" + urlencode(params), headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            retry = None
            try:
                with self.opener(request, timeout=25) as response:
                    raw = response.read(MAX_RESPONSE_BYTES + 1)
                    if len(raw) > MAX_RESPONSE_BYTES:
                        raise SourceError("API metadata response exceeded the size limit")
                    body = json.loads(raw)
                    if not isinstance(body, dict):
                        raise SourceError("API metadata response was not an object")
                    error = body.get("error")
                    if error:
                        if not isinstance(error, dict):
                            raise SourceError("Commons API returned an invalid error response")
                        code = error.get("code")
                        if code in ("maxlag", "ratelimited", "readonly") or code in SEARCH_BUSY_CODES:
                            fallback = (5 if code in SEARCH_BUSY_CODES else 1) * 2 ** attempt
                            retry = _retry_delay(response.headers.get("Retry-After"), fallback)
                        else:
                            raise SourceError(f"Commons API error: {str(error.get('code', 'unknown'))[:80]}")
                    else:
                        return body
            except HTTPError as error:
                if error.code not in (429, 500, 502, 503, 504):
                    raise SourceError(f"Commons API returned HTTP {error.code}") from error
                retry = _retry_delay(error.headers.get("Retry-After"), 2 ** attempt)
            except (URLError, TimeoutError, json.JSONDecodeError) as error:
                retry = 2 ** attempt
                if attempt == self.retries:
                    raise SourceError("Commons API could not be reached or returned invalid JSON") from error
            if attempt == self.retries:
                raise SourceError("Commons API remained busy after bounded retries")
            if self.requests >= self.max_requests:
                raise RequestBudgetReached("API request budget reached")
            self.sleeper(retry)


def _connect(path, dry_run):
    path = Path(path)
    if dry_run:
        connection = sqlite3.connect(":memory:")
        if path.exists():
            original = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
            try:
                original.backup(connection)
            finally:
                original.close()
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(path, timeout=10)
        connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript((ROOT / "catalog/schema.sql").read_text())
    version = connection.execute("SELECT value FROM catalog_meta WHERE key='schema_version'").fetchone()[0]
    if version != "1":
        connection.close()
        raise ValueError("Unsupported catalog schema version")
    return connection


def _progress_key(source):
    return source["category"] + ":" + source["query"]


def _continuation(value):
    if not isinstance(value, dict) or any(name not in ("continue", "gsroffset") for name in value):
        raise SourceError("Commons search continuation has unsupported fields")
    if "gsroffset" in value and (type(value["gsroffset"]) is not int or not 0 <= value["gsroffset"] <= 10_000):
        raise SourceError("Commons search offset is invalid")
    if "continue" in value and (not isinstance(value["continue"], str) or len(value["continue"]) > 200):
        raise SourceError("Commons search continuation token is invalid")
    return value


def _response_pages(response):
    if not isinstance(response, dict) or not any(key in response for key in ("query", "batchcomplete", "continue")):
        raise SourceError("Commons API returned invalid search metadata")
    query = response.get("query", {})
    if not isinstance(query, dict) or not isinstance(query.get("pages", []), list):
        raise SourceError("Commons API returned invalid page metadata")
    return query.get("pages", []), _continuation(response.get("continue", {}))


def run_import(db_path=ROOT / "data/catalog/catalog.sqlite", *, limit=1000,
               max_requests=400, delay=0.5, max_duration=120, dry_run=False,
               sources=None, client=None, report_path=None):
    """Reach a total source-item target, resuming safely across bounded runs."""
    if type(limit) is not int or not 1 <= limit <= 5000:
        raise ValueError("limit must be an integer from 1 to 5000")
    if type(max_requests) is not int or not 1 <= max_requests <= 2000:
        raise ValueError("max_requests must be an integer from 1 to 2000")
    if not isinstance(delay, (int, float)) or not math.isfinite(delay) or not 0 <= delay <= 30:
        raise ValueError("delay must be a finite number from 0 to 30 seconds")
    if type(max_duration) not in (int, float) or not math.isfinite(max_duration) or not 1 <= max_duration <= 600:
        raise ValueError("max_duration must be from 1 to 600 seconds")
    sources = load_sources() if sources is None else sources
    source_keys = [_progress_key(source) for source in sources]
    if not sources or len(set(source_keys)) != len(sources) or any(source["category"] not in TOPICS for source in sources):
        raise ValueError("Use distinct supported source queries")
    client = CommonsClient(max_requests, delay) if client is None else client
    connection = _connect(db_path, dry_run)
    started = datetime.now(timezone.utc).isoformat()
    report = {"provider": "wikimedia", "started_at": started, "dry_run": dry_run,
              "target": limit, "imported": 0, "would_import": 0, "existing": 0,
              "duplicates": 0, "skipped": 0, "skip_reasons": {}, "errors": [],
              "requests": 0, "target_reached": False, "stop_reason": None,
              "license_counts": {}, "search_history": [],
              "source_urls": [API, "https://www.mediawiki.org/wiki/Extension:TimedMediaHandler/API",
                              "https://commons.wikimedia.org/wiki/Commons:Reusing_content_outside_Wikimedia"]}
    count = lambda: connection.execute("SELECT COUNT(*) FROM videos WHERE source='wikimedia'").fetchone()[0]
    report["existing"] = count()
    # A wider duration policy must revisit items skipped by an earlier narrow one.
    progress_name = f"{PROGRESS_KEY}:duration={max_duration:g}"
    row = connection.execute("SELECT value FROM catalog_meta WHERE key=?", (progress_name,)).fetchone()
    try:
        progress = json.loads(row[0]) if row else {}
        if not isinstance(progress, dict):
            raise ValueError()
    except (ValueError, TypeError):
        connection.close()
        raise ValueError("Stored Commons import progress is invalid")
    skipped, licenses = Counter(), Counter()
    failed = set()
    try:
        while count() < limit:
            active = [source for source in sources if _progress_key(source) not in failed
                      and not progress.get(_progress_key(source), {}).get("exhausted", False)]
            if not active:
                report["stop_reason"] = "source_errors" if failed else "sources_exhausted"
                break
            for source in active:
                if count() >= limit:
                    break
                key = _progress_key(source)
                saved = progress.get(key, {})
                params = {"action": "query", "format": "json", "formatversion": 2,
                          "maxlag": 5, "generator": "search", "gsrnamespace": 6,
                          "gsrsearch": "filetype:video " + source["query"],
                          "gsrlimit": min(10, limit - count()), "prop": "videoinfo",
                          "viprop": "url|size|mime|derivatives|extmetadata", "viurlwidth": 320,
                          "viextmetadatafilter": "Artist|ObjectName|ImageDescription|LicenseShortName|LicenseUrl|Copyrighted|License|AttributionRequired"}
                continuation = _continuation(saved.get("continue", {}))
                params.update(continuation)
                try:
                    response = client.query(params)
                    pages, next_page = _response_pages(response)
                except RequestBudgetReached:
                    report["stop_reason"] = "request_budget"
                    raise
                except SourceError as error:
                    report["errors"].append({"category": source["category"], "query": source["query"], "message": str(error)[:240]})
                    failed.add(key)
                    continue
                normalized = []
                for page in pages:
                    try:
                        normalized.append(normalize_page(page, source["category"], source["query"], max_duration))
                    except SkipVideo as error:
                        skipped[str(error)] += 1
                new_progress = {"continue": next_page, "exhausted": not bool(next_page)}
                with connection:
                    for post in normalized:
                        search_text = " ".join([post["creator"], post["handle"], post["title"], post["description"], post["category"], *post["tags"]]).lower()
                        cursor = connection.execute(INSERT, (post["id"], "wikimedia", post["source"]["id"], post["category"], post["handle"], search_text, json.dumps(post, ensure_ascii=False, allow_nan=False), started))
                        if cursor.rowcount:
                            report["would_import" if dry_run else "imported"] += 1
                            licenses[post["source"]["license"]["name"]] += 1
                        else:
                            report["duplicates"] += 1
                    updated = {**progress, key: new_progress}
                    connection.execute("INSERT INTO catalog_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (progress_name, json.dumps(updated)))
                progress = updated
                report["search_history"].append({**source, "offset": continuation.get("gsroffset", 0), "candidates": len(pages), "accepted": len(normalized), "exhausted": new_progress["exhausted"]})
                print(f"catalog source={source['category']} query={source['query']!r} total={count()}/{limit}", file=sys.stderr, flush=True)
        if count() >= limit:
            report["stop_reason"] = "target_reached"
    except RequestBudgetReached:
        pass
    except KeyboardInterrupt:
        report["stop_reason"] = "interrupted"
    finally:
        report.update(total_source_items=count(), target_reached=count() >= limit,
                      requests=client.requests, skipped=sum(skipped.values()),
                      skip_reasons=dict(skipped), license_counts=dict(licenses),
                      completed_at=datetime.now(timezone.utc).isoformat())
        connection.close()
    if report_path is not None or not dry_run:
        destination = Path(report_path) if report_path else Path(db_path).parent / "import-report.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
        temporary.replace(destination)
    return report


def backfill_sources(db_path=ROOT / "data/catalog/catalog.sqlite", *, max_requests=100,
                     delay=0.5, dry_run=False, client=None):
    """Add ordered sources to stored Commons items; remove items without a Safari source.

    Every page is read before anything is written, and the writes share one
    transaction, so an upstream failure leaves the catalog unchanged.
    """
    if type(max_requests) is not int or not 1 <= max_requests <= 2000:
        raise ValueError("max_requests must be an integer from 1 to 2000")
    if not isinstance(delay, (int, float)) or not math.isfinite(delay) or not 0 <= delay <= 30:
        raise ValueError("delay must be a finite number from 0 to 30 seconds")
    client = CommonsClient(max_requests, delay) if client is None else client
    connection = _connect(db_path, dry_run)
    report = {"provider": "wikimedia", "dry_run": dry_run, "checked": 0, "updated": 0,
              "unchanged": 0, "removed": 0, "remove_reasons": {}, "requests": 0}
    try:
        stored = connection.execute("SELECT id, source_id, payload_json FROM videos "
                                    "WHERE source='wikimedia' ORDER BY seq").fetchall()
        updates, removals, reasons = [], [], Counter()
        for start in range(0, len(stored), 50):
            batch = stored[start:start + 50]
            response = client.query({"action": "query", "format": "json", "formatversion": 2,
                                     "maxlag": 5, "pageids": "|".join(row[1] for row in batch),
                                     "prop": "videoinfo", "viprop": "url|size|mime|derivatives"})
            pages, _ = _response_pages(response)
            by_id = {str(page.get("pageid")): page for page in pages if isinstance(page, dict)}
            for item_id, source_id, payload_json in batch:
                page = by_id.get(source_id)
                if page is None:
                    raise SourceError("Commons API omitted a requested page; nothing was changed")
                revisions = page.get("videoinfo")
                try:
                    if page.get("missing") or not isinstance(revisions, list) or not revisions or not isinstance(revisions[0], dict):
                        raise SkipVideo("missing_video_info")
                    video, media_type, sources = select_sources(revisions[0])
                except SkipVideo as error:
                    reasons[str(error)] += 1
                    removals.append(item_id)
                    continue
                payload = json.loads(payload_json)
                updated = {**payload, "video": video, "sources": sources,
                           "source": {**payload["source"], "mediaType": media_type}}
                if updated == payload:
                    report["unchanged"] += 1
                else:
                    updates.append((json.dumps(updated, ensure_ascii=False, allow_nan=False), item_id))
        with connection:
            connection.executemany("UPDATE videos SET payload_json=? WHERE id=?", updates)
            connection.executemany("DELETE FROM videos WHERE id=?", [(item_id,) for item_id in removals])
        report.update(checked=len(stored), updated=len(updates), removed=len(removals),
                      remove_reasons=dict(reasons))
    finally:
        report["requests"] = client.requests
        connection.close()
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=ROOT / "data/catalog/catalog.sqlite")
    parser.add_argument("--limit", type=int, default=1000, help="Target total number of Commons items, maximum 5000")
    parser.add_argument("--max-requests", type=int, default=400, help="Maximum API attempts, including retries")
    parser.add_argument("--delay", type=float, default=0.5, help="Minimum seconds between serial API requests")
    parser.add_argument("--max-duration", type=float, default=120, help="Maximum clip duration in seconds")
    parser.add_argument("--dry-run", action="store_true", help="Use an in-memory copy without changing the catalog")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--backfill-sources", action="store_true",
                        help="Add Safari-compatible sources to stored items instead of importing")
    args = parser.parse_args(argv)
    if args.backfill_sources:
        try:
            report = backfill_sources(args.db, max_requests=args.max_requests,
                                      delay=args.delay, dry_run=args.dry_run)
        except (OSError, ValueError, sqlite3.Error, SourceError) as error:
            parser.exit(2, f"Catalog source backfill failed: {error}\n")
        print(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False))
        return 0
    try:
        report = run_import(args.db, limit=args.limit, max_requests=args.max_requests,
                            delay=args.delay, max_duration=args.max_duration,
                            dry_run=args.dry_run, report_path=args.report)
    except (OSError, ValueError, sqlite3.Error, SourceError) as error:
        parser.exit(2, f"Catalog import failed: {error}\n")
    print(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False))
    if report["stop_reason"] == "interrupted":
        return 130
    return 0 if report["target_reached"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
