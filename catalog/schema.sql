-- Shared by the Node catalog reader and the bounded Python importer.
-- New rows are append-only for stable paginated browsing. Updating/deleting
-- existing rows changes content_revision and invalidates older query cursors.
CREATE TABLE IF NOT EXISTS catalog_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT OR IGNORE INTO catalog_meta(key, value) VALUES ('schema_version', '1');
INSERT OR IGNORE INTO catalog_meta(key, value) VALUES ('content_revision', '0');

CREATE TABLE IF NOT EXISTS videos (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE CHECK(length(id) BETWEEN 1 AND 160),
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    category TEXT NOT NULL CHECK(category IN ('Nature', 'Travel', 'Food', 'Design', 'Science')),
    creator_key TEXT NOT NULL CHECK(length(creator_key) BETWEEN 1 AND 160),
    search_text TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
    imported_at TEXT NOT NULL,
    UNIQUE(source, source_id)
);
CREATE INDEX IF NOT EXISTS videos_category_seq ON videos(category, seq);
CREATE INDEX IF NOT EXISTS videos_creator_seq ON videos(creator_key, seq);
CREATE INDEX IF NOT EXISTS videos_source_seq ON videos(source, seq);

CREATE TRIGGER IF NOT EXISTS videos_update_revision
AFTER UPDATE ON videos
BEGIN
    UPDATE catalog_meta SET value = CAST(value AS INTEGER) + 1
    WHERE key = 'content_revision';
END;
CREATE TRIGGER IF NOT EXISTS videos_delete_revision
AFTER DELETE ON videos
BEGIN
    UPDATE catalog_meta SET value = CAST(value AS INTEGER) + 1
    WHERE key = 'content_revision';
END;
