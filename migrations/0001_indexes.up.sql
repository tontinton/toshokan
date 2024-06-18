CREATE TABLE IF NOT EXISTS indexes(
    name TEXT PRIMARY KEY,
    config JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS index_files(
    id VARCHAR(36) PRIMARY KEY,
    index_name TEXT NOT NULL REFERENCES indexes(name) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    footer_len BIGINT NOT NULL
);
