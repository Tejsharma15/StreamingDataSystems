SELECT m.c_id as campaignId, floor(d.event_time/10000) as eventTime, count(*) as count 
FROM  original_data d INNER JOIN mappings m ON 
m.a_id = d.a_id WHERE d.event_type = 'click' 
GROUP BY m.c_id, eventTime 
ORDER BY m.c_id, eventTime asc;