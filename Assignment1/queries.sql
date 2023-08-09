SELECT m.c_id as campaignId, floor(d.event_time/10000) as eventTime, count(*) as count 
FROM  data d INNER JOIN mappings m ON 
m.a_id = d.a_id WHERE d.event_type = 'click' 
GROUP BY m.c_id, floor(d.event_time/10000) 
ORDER BY m.c_id, eventTime asc;

-- USING EXPLAIN ANALYZE - Find the execution time
-- SELECT count(*)/14.073 AS throughtput
-- FROM data d;

