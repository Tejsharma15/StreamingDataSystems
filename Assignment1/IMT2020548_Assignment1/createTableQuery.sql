CREATE DATABASE event_data;
\c event_data;

CREATE TABLE mappings(
a_id VARCHAR (50) PRIMARY KEY,
c_id bigint
);
COPY mappings FROM '/home/tejas/Desktop/Academics/Sem7/SDS/StreamingDataSystems/Assignment1/data/mappings.tsv';

CREATE TABLE original_data(
event_time BIGINT NOT NULL,  
w_id integer,
rank integer,
iteration integer,
event_type VARCHAR NOT NULL,
a_id VARCHAR NOT NULL
);

COPY original_data FROM '/home/tejas/Desktop/Academics/Sem7/SDS/StreamingDataSystems/Assignment1/data/data0.tsv';
COPY original_data FROM '/home/tejas/Desktop/Academics/Sem7/SDS/StreamingDataSystems/Assignment1/data/data1.tsv';



CREATE INDEX click_index on data(event_type);



