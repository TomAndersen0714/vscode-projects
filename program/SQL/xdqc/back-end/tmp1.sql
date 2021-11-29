SELECT database,
    table,
    formatReadableSize(size) AS size,
    formatReadableSize(bytes_on_disk) AS bytes_on_disk,
    formatReadableSize(data_uncompressed_bytes) AS data_uncompressed_bytes,
    formatReadableSize(data_compressed_bytes) AS data_compressed_bytes,
    compress_rate,
    rows,
    days,
    formatReadableSize(avgDaySize) AS avgDaySize
FROM (
        SELECT database,
            table,
            sum(bytes) AS size,
            sum(rows) AS rows,
            min(min_date) AS min_date,
            max(max_date) AS max_date,
            sum(bytes_on_disk) AS bytes_on_disk,
            sum(data_uncompressed_bytes) AS data_uncompressed_bytes,
            sum(data_compressed_bytes) AS data_compressed_bytes,
(data_compressed_bytes / data_uncompressed_bytes) * 100 AS compress_rate,
            max_date - min_date AS days,
            size / (max_date - min_date) AS avgDaySize
        FROM system.parts
        WHERE active
        GROUP BY database,
            table
        ORDER BY size DESC
    )