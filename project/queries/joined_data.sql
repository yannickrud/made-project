SELECT *
FROM weather_phenomena LEFT JOIN description
WHERE weather_phenomena.STATIONS_ID == description.Stations_id