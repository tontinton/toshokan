CREATE TABLE IF NOT EXISTS indexes(
    id VARCHAR(36) PRIMARY KEY,
    file_name TEXT NOT NULL,
    footer_len BIGINT NOT NULL
);
