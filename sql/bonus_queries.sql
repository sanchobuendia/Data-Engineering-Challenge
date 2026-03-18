-- 1) From the two most commonly appearing regions, which is the latest datasource?
WITH region_counts AS (
    SELECT region, COUNT(*) AS trip_count
    FROM trips_clean
    WHERE region IS NOT NULL
    GROUP BY region
    ORDER BY trip_count DESC
    LIMIT 2
), ranked_sources AS (
    SELECT
        t.region,
        t.datasource,
        t.trip_datetime,
        ROW_NUMBER() OVER (PARTITION BY t.region ORDER BY t.trip_datetime DESC) AS rn
    FROM trips_clean t
    INNER JOIN region_counts rc ON rc.region = t.region
)
SELECT region, datasource AS latest_datasource, trip_datetime
FROM ranked_sources
WHERE rn = 1
ORDER BY region;

-- 2) What regions has the "cheap_mobile" datasource appeared in?
SELECT DISTINCT region
FROM trips_clean
WHERE datasource = 'cheap_mobile'
  AND region IS NOT NULL
ORDER BY region;
