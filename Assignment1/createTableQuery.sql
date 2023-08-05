CREATE TABLE mappings(
a_id VARCHAR (50) PRIMARY KEY,
c_id bigint
);
COPY mappings FROM '/home/tejas/Desktop/Academics/Sem7/SDS/StreamingDataSystems/Assignment1/data/mappings.tsv';


CREATE TABLE data(
event_time BIGINT NOT NULL,  
w_id integer,
rank integer,
iteration integer,
event_type VARCHAR NOT NULL,
a_id VARCHAR NOT NULL,
PRIMARY KEY (event_time, rank, iteration)
);
COPY data FROM '/home/tejas/Desktop/Academics/Sem7/SDS/StreamingDataSystems/Assignment1/data/data0.tsv';
COPY data FROM '/home/tejas/Desktop/Academics/Sem7/SDS/StreamingDataSystems/Assignment1/data/data1.tsv';
COPY data FROM '/home/tejas/Desktop/Academics/Sem7/SDS/StreamingDataSystems/Assignment1/data/data2.tsv';