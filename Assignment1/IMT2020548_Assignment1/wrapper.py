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
    end_timestamp = 1691572058000
    interval_milliseconds = 10000
    curr_timestamp = start_timestamp
    start_time = time.time()
    overall_processing_time = start_time
    query_processing_time = 0
    upper_time = start_time
    loadProcess_time = start_time
    # print('STARTING TIME: %f, UPPER_TIME: %f', start_time, upper_time)
    while(curr_timestamp <= end_timestamp):
        start_time = time.time()
        if(start_time <= upper_time + 10):  continue
        upper_time += 10
        startQuery_time = time.time()
        print("Starting next query")
        query = """CREATE TABLE main_data_%s AS 
            SELECT event_time, w_id, rank, iteration, event_type, a_id
            FROM original_data
            WHERE event_time >= %s AND event_time <%s"""
        cursor.execute(query, (curr_timestamp, curr_timestamp, curr_timestamp+interval_milliseconds))
        query = """SELECT m.c_id as campaignId, %s as event_time, count(*) as count 
            FROM main_data_%s d 
            INNER JOIN mappings m ON m.a_id = d.a_id 
            WHERE d.event_type = 'click'
            GROUP BY m.c_id
            ORDER BY m.c_id"""
        cursor.execute(query, (curr_timestamp/10000, curr_timestamp))
        endProcess_time = time.time()
        query_processing_time += endProcess_time - startQuery_time
        curr_timestamp += interval_milliseconds
        rows = cursor.fetchall()
        output_file = 'output.txt'

        # Open the file for writing
        with open(output_file, 'a') as file:
            for row in rows:
                # Convert the row data to a string representation
                row_str = ', '.join(str(item) for item in row)
                
                # Write the row to the file
                file.write(row_str + '\n')
        # print('STARTING TIME: UPPER TIME FOR NEXT:', time.time(), upper_time+10)
        # print('NEXT STARTING TIME: %f, UPPER_TIME: %f', time.time(), upper_time+2)
        print("PROCESSING THIS TABLE TOOK: QUERY PROCESSING TIME: ", endProcess_time - loadProcess_time, endProcess_time-startQuery_time)
        loadProcess_time = time.time()
    overall_end_processing_time = time.time()
    print("FINAL PROCESSING IS: ", overall_end_processing_time-overall_processing_time)
    print("THE LATENCY (ONLY TO COMPUTE THE QUERY IS)", query_processing_time)

except psycopg2.Error as e:
    print("Error connecting to the database:", e)

finally:
    if connection:
        connection.close()


  
