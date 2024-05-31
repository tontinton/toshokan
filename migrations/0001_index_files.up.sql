CREATE TABLE IF NOT EXISTS index_files(
    id VARCHAR(36) PRIMARY KEY,
    file_name TEXT NOT NULL,
    footer_len BIGINT NOT NULL
);
