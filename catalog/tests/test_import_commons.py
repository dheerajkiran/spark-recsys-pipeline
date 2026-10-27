"""Catalog licensing/provenance, bounded source reads and resumable transactions."""

import copy
import hashlib
import io
import json
from pathlib import Path
import sqlite3
from urllib.error import HTTPError

import pytest

from catalog import import_commons as importer


SOURCES = [{"category": "Nature", "query": "forest"}]


def test_tracked_query_manifest_is_bounded_diverse_and_keeps_original_resume_keys():
    sources = importer.load_sources()
    assert 85 <= len(sources) <= 100
    assert len({(item["category"], item["query"]) for item in sources}) == len(sources)
    assert {item["category"] for item in sources} == importer.TOPICS
    assert sources[:5] == [
        {"category": "Nature", "query": "forest"},
        {"category": "Travel", "query": "mountain"},
        {"category": "Food", "query": "cooking"},
        {"category": "Design", "query": "pottery"},
        {"category": "Science", "query": "experiment"},
    ]
    assert all(len(item["query"]) <= 150 for item in sources)


def page(page_id=1):
    return {"pageid": page_id, "title": f"File:Forest_{page_id}.webm", "videoinfo": [{
        "duration": 21.5, "mime": "video/webm",
        "descriptionurl": f"https://commons.wikimedia.org/wiki/File:Forest_{page_id}.webm",
        "thumburl": f"https://thumb.wikimedia.org/wikipedia/commons/thumb/a/ab/Forest_{page_id}.webm/320px--Forest_{page_id}.webm.jpg",
        "extmetadata": {"Artist": {"value": '<a href="//commons.wikimedia.org/wiki/User:Example">River &amp; Reed</a>'},
                        "LicenseShortName": {"value": "CC BY-SA 4.0"},
                        "LicenseUrl": {"value": "https://creativecommons.org/licenses/by-sa/4.0/"},
                        "ObjectName": {"value": "A quiet forest"},
                        "ImageDescription": {"value": "A <em>peaceful</em> walk among trees."}},
        "derivatives": [
            {"src": f"https://upload.wikimedia.org/wikipedia/commons/a/ab/Forest_{page_id}.webm", "type": "video/webm", "width": 1920, "height": 1080},
            {"src": f"https://upload.wikimedia.org/wikipedia/commons/transcoded/a/ab/Forest_{page_id}.webm/Forest_{page_id}.webm.240p.vp9.webm", "type": 'video/webm; codecs="vp9, opus"', "transcodekey": "240p.vp9.webm", "width": 426, "height": 240, "bandwidth": 400000},
            {"src": f"https://upload.wikimedia.org/wikipedia/commons/transcoded/a/ab/Forest_{page_id}.webm/Forest_{page_id}.webm.360p.vp9.webm", "type": 'video/webm; codecs="vp9, opus"', "transcodekey": "360p.vp9.webm", "width": 640, "height": 360, "bandwidth": 700000},
            {"src": mov_url(page_id), "type": MOV_TYPE, "transcodekey": "360p.mpeg4.mov", "width": 640, "height": 360, "bandwidth": 1600000},
        ]}]}


MOV_TYPE = 'video/quicktime; codecs="mp4v"'


def mov_url(page_id=1):
    return f"https://upload.wikimedia.org/wikipedia/commons/transcoded/a/ab/Forest_{page_id}.webm/Forest_{page_id}.webm.360p.mpeg4.mov"


def response(*ids, offset=None):
    value = {"batchcomplete": True, "query": {"pages": [page(item) for item in ids]}}
    if offset is not None:
        value["continue"] = {"gsroffset": offset, "continue": "gsroffset||"}
    return value


class FakeClient:
    def __init__(self, replies, max_requests=100):
        self.replies, self.max_requests = list(replies), max_requests
        self.requests, self.params = 0, []

    def query(self, params):
        if self.requests >= self.max_requests:
            raise importer.RequestBudgetReached()
        self.requests += 1
        self.params.append(dict(params))
        value = self.replies.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def rows(db):
    with sqlite3.connect(db) as connection:
        return [json.loads(row[0]) for row in connection.execute("SELECT payload_json FROM videos ORDER BY seq")]


def test_real_provenance_is_separate_from_demo_and_training_ids():
    original = page()
    saved = copy.deepcopy(original)
    result = importer.normalize_page(original, "Nature", "forest")
    assert original == saved
    assert result["id"] == "commons:1"
    assert result["creator"] == "River & Reed"
    assert result["description"] == "A peaceful walk among trees."
    assert result["video"].endswith("360p.vp9.webm")
    assert result["source"] == {"provider": "wikimedia", "id": "1",
                                "url": "https://commons.wikimedia.org/wiki/File:Forest_1.webm",
                                "license": {"name": "CC BY-SA 4.0", "url": "https://creativecommons.org/licenses/by-sa/4.0/"},
                                "attribution": "River & Reed", "demo": False,
                                "kuairandId": None, "mediaType": "video/webm"}
    assert result["likes"] == 0 and result["comments"] == []
    assert "probability" not in result
    assert "label" not in result


def test_upstream_markup_is_never_imported_as_html():
    item = page()
    item["videoinfo"][0]["extmetadata"]["Artist"]["value"] = '<script>secret()</script><b>Jane</b> &amp; <i>John</i>'
    item["videoinfo"][0]["extmetadata"]["ImageDescription"]["value"] = '<img src=x onerror=alert(1)> Nice <style>bad()</style> scene'
    result = importer.normalize_page(item, "Nature", "forest")
    assert result["creator"] == "Jane & John"
    assert result["description"] == "Nice scene"
    assert "<" not in json.dumps(result)


def test_full_plain_attribution_is_preserved_beyond_the_creator_display_limit():
    item = page()
    authors = "First Contributor; " * 30 + "Final Contributor"
    item["videoinfo"][0]["extmetadata"]["Artist"]["value"] = f"<div>{authors}</div>"
    result = importer.normalize_page(item, "Nature", "forest")
    assert len(result["creator"]) <= 160
    assert result["source"]["attribution"] == authors
    assert result["handle"] == "commons-" + hashlib.sha256(authors.casefold().encode()).hexdigest()[:12]
    item["videoinfo"][0]["extmetadata"]["Artist"]["value"] = f"<div>{authors} Extra Author</div>"
    assert importer.normalize_page(item, "Nature", "forest")["handle"] != result["handle"]


def test_oversized_attribution_is_skipped_instead_of_silently_losing_credit():
    item = page()
    item["videoinfo"][0]["extmetadata"]["Artist"]["value"] = "A" * 16_001
    with pytest.raises(importer.SkipVideo, match="attribution_too_long"):
        importer.normalize_page(item, "Nature", "forest")


@pytest.mark.parametrize("title,description", [
    ("Animal dissection demonstration", "A recording."),
    ("A recording", "Contains explicit sexual material."),
    ("A recording", "Graphic violence and gunfire."),
])
def test_obvious_sexual_medical_or_violent_subjects_are_excluded(title, description):
    item = page()
    item["videoinfo"][0]["extmetadata"]["ObjectName"]["value"] = title
    item["videoinfo"][0]["extmetadata"]["ImageDescription"]["value"] = description
    with pytest.raises(importer.SkipVideo, match="excluded_subject"):
        importer.normalize_page(item, "Nature", "forest")


@pytest.mark.parametrize("kind", ["no-license", "wrong-license", "mismatched-license", "no-author", "long-duration", "not-video", "foreign-source", "foreign-poster", "svg-poster", "original-only", "wrong-derivative-host", "foreign-port", "too-high-bitrate"])
def test_unusable_or_unlicensed_items_are_skipped(kind):
    item = page()
    info = item["videoinfo"][0]
    if kind == "no-license":
        info["extmetadata"].pop("LicenseUrl")
    elif kind == "wrong-license":
        info["extmetadata"]["LicenseUrl"]["value"] = "https://example.com/license"
    elif kind == "mismatched-license":
        info["extmetadata"]["LicenseShortName"]["value"] = "All rights reserved"
    elif kind == "no-author":
        info["extmetadata"].pop("Artist")
    elif kind == "long-duration":
        info["duration"] = 200
    elif kind == "not-video":
        info["mime"] = "audio/ogg"
    elif kind == "foreign-source":
        info["descriptionurl"] = "https://evil.example/File:clip.webm"
    elif kind == "foreign-poster":
        info["thumburl"] = "https://evil.example/cover.jpg"
    elif kind == "svg-poster":
        info["thumburl"] = "https://upload.wikimedia.org/unsafe.svg"
    elif kind == "original-only":
        info["derivatives"] = info["derivatives"][:1]
    else:
        info["derivatives"] = info["derivatives"][1:]
        for value in info["derivatives"]:
            if kind == "wrong-derivative-host":
                value["src"] = value["src"].replace("upload.wikimedia.org", "evil.example")
            elif kind == "foreign-port":
                value["src"] = value["src"].replace("upload.wikimedia.org", "upload.wikimedia.org:444")
            else:
                value["bandwidth"] = 3_000_000
    with pytest.raises(importer.SkipVideo):
        importer.normalize_page(item, "Nature", "forest")


@pytest.mark.parametrize("name,url,expected", [
    ("CC BY 3.0", "http://creativecommons.org/licenses/by/3.0/", "CC BY 3.0"),
    ("CC0", "https://creativecommons.org/publicdomain/zero/1.0/deed.en", "CC0 1.0"),
    ("Public domain", "https://creativecommons.org/publicdomain/mark/1.0/", "Public domain"),
])
def test_recognized_license_variants_keep_real_terms(name, url, expected):
    item = page()
    info = item["videoinfo"][0]
    info["extmetadata"]["LicenseShortName"]["value"] = name
    info["extmetadata"]["LicenseUrl"]["value"] = url
    license_info = importer.normalize_page(item, "Nature", "forest")["source"]["license"]
    assert license_info["name"] == expected
    assert license_info["url"].startswith("https://creativecommons.org/")


def test_sources_list_the_safari_derivative_before_the_webm():
    result = importer.normalize_page(page(), "Nature", "forest")
    assert result["sources"] == [
        {"src": mov_url(), "type": MOV_TYPE},
        {"src": result["video"], "type": 'video/webm; codecs="vp9, opus"'},
    ]
    assert result["source"]["mediaType"] == "video/webm"


@pytest.mark.parametrize("change", ["remove", "other-codec", "not-mov-path", "too-tall"])
def test_items_without_a_safari_derivative_are_skipped(change):
    item = page()
    derivatives = item["videoinfo"][0]["derivatives"]
    if change == "remove":
        derivatives.pop()
    elif change == "other-codec":
        derivatives[-1]["type"] = 'video/quicktime; codecs="mjpeg"'
    elif change == "not-mov-path":
        derivatives[-1]["src"] = derivatives[-1]["src"].replace(".mov", ".qt")
    else:
        derivatives[-1]["height"] = 1080
    with pytest.raises(importer.SkipVideo, match="no_safari_derivative"):
        importer.normalize_page(item, "Nature", "forest")


def test_unrecognized_codec_parameters_keep_only_the_media_type():
    item = page()
    item["videoinfo"][0]["derivatives"][2]["type"] = 'video/webm; codecs="<b>"'
    assert importer.normalize_page(item, "Nature", "forest")["sources"][1]["type"] == "video/webm"


def lookup(*pages):
    return {"batchcomplete": True, "query": {"pages": list(pages)}}


def legacy_catalog(tmp_path, ids):
    """A catalog written before sources existed: WebM only, no sources field."""
    db = tmp_path / "catalog.sqlite"
    importer.run_import(db, limit=len(ids), sources=SOURCES, client=FakeClient([response(*ids)]))
    with sqlite3.connect(db) as connection:
        for row_id, payload in connection.execute("SELECT id, payload_json FROM videos").fetchall():
            value = json.loads(payload)
            value.pop("sources")
            connection.execute("UPDATE videos SET payload_json=? WHERE id=?", (json.dumps(value), row_id))
    return db


def revision(db):
    with sqlite3.connect(db) as connection:
        return connection.execute("SELECT value FROM catalog_meta WHERE key='content_revision'").fetchone()[0]


def test_backfill_adds_sources_and_removes_items_without_a_safari_derivative(tmp_path):
    db = legacy_catalog(tmp_path, [1, 2, 3])
    no_mov = page(2)
    no_mov["videoinfo"][0]["derivatives"].pop()
    client = FakeClient([lookup(page(1), no_mov, {"pageid": 3, "missing": True})])
    report = importer.backfill_sources(db, client=client)
    assert client.params[0]["pageids"] == "1|2|3"
    assert report["checked"] == 3 and report["updated"] == 1 and report["removed"] == 2
    assert report["remove_reasons"] == {"no_safari_derivative": 1, "missing_video_info": 1}
    [kept] = rows(db)
    assert kept["id"] == "commons:1" and kept["sources"][0] == {"src": mov_url(1), "type": MOV_TYPE}
    before = revision(db)
    again = importer.backfill_sources(db, client=FakeClient([lookup(page(1))]))
    assert again["unchanged"] == 1 and again["updated"] == 0 and revision(db) == before


def test_backfill_changes_nothing_when_a_page_is_omitted_or_on_dry_run(tmp_path):
    db = legacy_catalog(tmp_path, [1, 2])
    digest = hashlib.sha256(db.read_bytes()).hexdigest()
    with pytest.raises(importer.SourceError):
        importer.backfill_sources(db, client=FakeClient([lookup(page(1))]))
    dry = importer.backfill_sources(db, client=FakeClient([lookup(page(1), page(2))]), dry_run=True)
    assert dry["updated"] == 2
    assert hashlib.sha256(db.read_bytes()).hexdigest() == digest
    assert all("sources" not in row for row in rows(db))


def test_backfill_reads_pages_in_bounded_batches(tmp_path):
    db = legacy_catalog(tmp_path, list(range(1, 61)))
    client = FakeClient([lookup(*[page(i) for i in range(1, 51)]), lookup(*[page(i) for i in range(51, 61)])])
    report = importer.backfill_sources(db, client=client)
    assert report["requests"] == 2 and report["updated"] == 60
    assert [len(params["pageids"].split("|")) for params in client.params] == [50, 10]


def test_import_persists_pages_and_resumes_without_duplicating_or_reordering(tmp_path):
    db = tmp_path / "catalog.sqlite"
    first = FakeClient([response(1, 2, offset=2)])
    report = importer.run_import(db, limit=2, sources=SOURCES, client=first)
    assert report["target_reached"] and report["imported"] == 2
    assert first.params[0]["gsrsearch"] == "filetype:video forest"
    assert first.params[0]["gsrnamespace"] == 6
    assert first.params[0]["gsrlimit"] == 2
    unchanged = FakeClient([])
    again = importer.run_import(db, limit=2, sources=SOURCES, client=unchanged)
    assert again["requests"] == 0 and again["imported"] == 0
    more = FakeClient([response(2, 3, offset=4), response(4)])
    final = importer.run_import(db, limit=4, sources=SOURCES, client=more)
    assert more.params[0]["gsroffset"] == 2
    assert final["target_reached"] and final["duplicates"] == 1
    assert [row["id"] for row in rows(db)] == ["commons:1", "commons:2", "commons:3", "commons:4"]
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT value FROM catalog_meta WHERE key='content_revision'").fetchone()[0] == "0"


def test_dry_run_does_not_create_a_database_or_output_directory(tmp_path):
    db = tmp_path / "not-created" / "catalog.sqlite"
    report = importer.run_import(db, limit=1, sources=SOURCES, client=FakeClient([response(1)]), dry_run=True)
    assert report["dry_run"] and report["would_import"] == 1 and report["imported"] == 0
    assert not db.parent.exists()


def test_dry_run_copies_existing_state_without_changing_catalog(tmp_path):
    db = tmp_path / "catalog.sqlite"
    importer.run_import(db, limit=1, sources=SOURCES, client=FakeClient([response(1, offset=1)]))
    digest = hashlib.sha256(db.read_bytes()).hexdigest()
    report = importer.run_import(db, limit=2, sources=SOURCES, client=FakeClient([response(2)]), dry_run=True)
    assert report["existing"] == 1 and report["would_import"] == 1
    assert hashlib.sha256(db.read_bytes()).hexdigest() == digest
    assert len(rows(db)) == 1


def test_request_budget_keeps_completed_pages_resumable(tmp_path):
    db = tmp_path / "catalog.sqlite"
    first = FakeClient([response(1, offset=1)], max_requests=1)
    report = importer.run_import(db, limit=2, sources=SOURCES, client=first)
    assert not report["target_reached"] and report["stop_reason"] == "request_budget"
    assert report["requests"] == 1 and len(rows(db)) == 1
    resumed = FakeClient([response(2)])
    assert importer.run_import(db, limit=2, sources=SOURCES, client=resumed)["target_reached"]
    assert resumed.params[0]["gsroffset"] == 1


def test_unknown_licenses_and_duplicate_search_hits_are_reported(tmp_path):
    item = page(2)
    item["videoinfo"][0]["extmetadata"]["LicenseUrl"]["value"] = "https://example.com/no-license"
    reply = response(1, 1)
    reply["query"]["pages"].append(item)
    report = importer.run_import(tmp_path / "catalog.sqlite", limit=4, sources=SOURCES, client=FakeClient([reply]))
    assert report["imported"] == 1 and report["duplicates"] == 1 and report["skipped"] == 1
    assert report["skip_reasons"] == {"missing_license_url": 1}
    assert report["license_counts"] == {"CC BY-SA 4.0": 1}
    assert report["stop_reason"] == "sources_exhausted"


def test_bad_upstream_metadata_fails_explicitly_without_false_exhaustion(tmp_path):
    db = tmp_path / "catalog.sqlite"
    report = importer.run_import(db, limit=2, sources=SOURCES, client=FakeClient([{"query": {"pages": "bad"}}]))
    assert not report["target_reached"] and report["stop_reason"] == "source_errors"
    assert report["errors"] and not rows(db)
    retry = FakeClient([response(1, 2)])
    assert importer.run_import(db, limit=2, sources=SOURCES, client=retry)["target_reached"]
    assert "gsroffset" not in retry.params[0]


def test_a_wider_duration_policy_revisits_previously_skipped_items(tmp_path):
    db = tmp_path / "catalog.sqlite"
    item = page()
    item["videoinfo"][0]["duration"] = 150
    reply = {"batchcomplete": True, "query": {"pages": [item]}}
    small = importer.run_import(db, limit=1, max_duration=120, sources=SOURCES, client=FakeClient([reply]))
    assert small["skipped"] == 1
    large = importer.run_import(db, limit=1, max_duration=180, sources=SOURCES, client=FakeClient([reply]))
    assert large["target_reached"]


class BytesResponse(io.BytesIO):
    def __init__(self, body, headers=None):
        super().__init__(json.dumps(body).encode())
        self.headers = headers or {}


def test_api_client_only_fetches_serial_metadata_with_a_meaningful_user_agent():
    requests = []
    def opener(request, timeout):
        requests.append(request)
        return BytesResponse(response(1))
    client = importer.CommonsClient(max_requests=2, delay=.5, opener=opener, sleeper=lambda seconds: None)
    assert client.query({"action": "query"})["query"]["pages"][0]["pageid"] == 1
    assert requests[0].full_url.startswith(importer.API)
    assert requests[0].get_header("User-agent") == importer.USER_AGENT
    assert client.requests == 1


def test_retries_count_against_the_shared_request_budget():
    attempts = []
    def opener(request, timeout):
        attempts.append(request)
        raise HTTPError(request.full_url, 429, "Busy", {"Retry-After": "1"}, None)
    client = importer.CommonsClient(max_requests=1, opener=opener, sleeper=lambda seconds: None)
    with pytest.raises(importer.RequestBudgetReached):
        client.query({"action": "query"})
    assert client.requests == len(attempts) == 1


@pytest.mark.parametrize("retry_after,minimum_pause", [(None, 5), ("7", 7)])
def test_json_search_busy_retries_with_backoff_or_retry_after_then_recovers(retry_after, minimum_pause):
    requests, sleeps = [], []
    replies = [BytesResponse({"error": {"code": "cirrussearch-too-busy-error", "info": "Search is busy."}},
                             {"Retry-After": retry_after} if retry_after else {}),
               BytesResponse(response(1))]
    def opener(request, timeout):
        requests.append(request)
        return replies.pop(0)
    client = importer.CommonsClient(max_requests=2, delay=0, opener=opener, sleeper=sleeps.append)
    result = client.query({"action": "query"})
    assert result["query"]["pages"][0]["pageid"] == 1
    assert client.requests == len(requests) == 2
    assert sleeps[0] >= minimum_pause


def test_json_search_busy_stops_after_bounded_attempts():
    sleeps = []
    def opener(request, timeout):
        return BytesResponse({"error": {"code": "cirrussearch-too-busy-error"}})
    client = importer.CommonsClient(max_requests=20, delay=0, retries=2,
                                    opener=opener, sleeper=sleeps.append)
    with pytest.raises(importer.SourceError, match="bounded retries"):
        client.query({"action": "query"})
    assert client.requests == 3
    assert [seconds for seconds in sleeps if seconds] == [5, 10]


def test_json_search_busy_cannot_bypass_the_global_request_budget():
    def opener(request, timeout):
        return BytesResponse({"error": {"code": "cirrussearch-too-busy-error"}})
    client = importer.CommonsClient(max_requests=1, delay=0, opener=opener,
                                    sleeper=lambda seconds: pytest.fail("No request budget remains for a retry"))
    with pytest.raises(importer.RequestBudgetReached):
        client.query({"action": "query"})
    assert client.requests == 1


def test_long_retry_after_is_not_ignored_or_retried_early():
    def opener(request, timeout):
        raise HTTPError(request.full_url, 503, "Busy", {"Retry-After": "120"}, None)
    client = importer.CommonsClient(opener=opener, sleeper=lambda seconds: pytest.fail("Must not retry before requested long pause"))
    with pytest.raises(importer.SourceError, match="longer pause"):
        client.query({"action": "query"})
    assert client.requests == 1


@pytest.mark.parametrize("options", [{"limit": 5001}, {"max_requests": 0}, {"delay": float("nan")}, {"max_duration": 601}])
def test_import_bounds_are_checked_before_creating_files(tmp_path, options):
    db = tmp_path / "catalog.sqlite"
    with pytest.raises(ValueError):
        importer.run_import(db, **options)
    assert not db.exists()
