import psycopg2
import time

try:
    connection = psycopg2.connect(
        host="localhost",
        database="event_data",
        user="postgres",
        password="tej123four"
    )
    cursor = connection.cursor()
    start_timestamp =  1691571404000
    end_timestamp = 1691572058999
    interval_milliseconds = 10000
    curr_timestamp = start_timestamp
    start_time = time.time()
    overall_processing_time = start_time
    query_processing_time = 0
    # time.sleep((end_timestamp - start_timestamp)/1000)
    time.sleep(1)
    print("FULL DATA ARRIVED AT: ", time.time()-start_time)
    query = """CREATE TABLE main_data AS 
        SELECT event_time, w_id, rank, iteration, event_type, a_id
        FROM original_data"""
    # print("LOADED DATA AT: ", time.time()-start_time)
    cursor.execute(query)
    print("done BRO FUCK")
    query = """SELECT m.c_id as campaignId, floor(d.event_time/10000) as eventTime, count(*) as count 
                FROM  main_data d INNER JOIN mappings m ON 
                m.a_id = d.a_id WHERE d.event_type = 'click' 
                GROUP BY m.c_id, eventTime 
                ORDER BY m.c_id, eventTime asc;"""
    print("done")
    cursor.execute(query)
    rows = cursor.fetchall()
    # Open the file for writing
    with open('full.txt', 'a') as file:
        for row in rows:
            # Convert the row data to a string representation
            row_str = ', '.join(str(item) for item in row)
            
            # Write the row to the file
            file.write(row_str + '\n')
    print("TIME TAKEN TO LOAD AND PROCESS WHOLE DATASET: ", time.time()-start_time)

except psycopg2.Error as e:
    print("Error connecting to the database:", e)

finally:
    if connection:
        connection.close()


  
