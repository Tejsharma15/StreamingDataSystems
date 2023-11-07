import random
import time
import csv
import threading

tempsecond = threading.Event()
tensecond = threading.Event()
finish = threading.Event()


def generate_stream_data(regex, genProb, throughput_per_second, duration, maxWindowSize):

    #Initializing global variables
    remove = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for i in regex:
        print(i)
        remove = remove.replace(i,"")
    print(remove)
    start_time = time.time()
    end_time = start_time + duration
    event_count, avg_throughput, total_events = 0,0,0
    file_counter=1
    eventList = []

    #Starting infiite loop
    startCompute = False
    while time.time() <=end_time:
        while(event_count < throughput_per_second and time.time()-1<=start_time):
            timestamp = int(time.time())
            random_integer = random.randint(1, 10)
            random_number = random.random()
            if(random_number > genProb):
                random_char = random.choice(remove)
            else:
                random_char = random.choice(regex)

            #Create event
            event = {
                'timestamp': timestamp,
                'integer': random_integer,
                'char': random_char
            }
            filename = f'data/{file_counter}.csv'
            # filename= 'stream_data.csv'
            eventList.append(event)
            event_count += 1

        #Calculate throughput for file
        actual_throughput = event_count
        total_events+=event_count
        # Check if a second has passed

        # Writing events to csv file
        with open(filename, mode='a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['timestamp', 'integer', 'char'])
            if file.tell() == 0:
                writer.writeheader()
            writer.writerows(eventList)
        
        with open('data/stream_data.csv', mode='a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['timestamp', 'integer', 'char'])
            if file.tell() == 0:
                writer.writeheader()
            writer.writerows(eventList)

        # print(f'Throughput: {actual_throughput} tuples per second')
        eventList = []
        if(file_counter >= maxWindowSize):
            tempsecond.set()
        if(file_counter%10 == 0):
            print('setting 10 second window')
            tensecond.set()
            print('10 second window is set. Should call the consumer function')
        elapsed_time = time.time() - start_time
        if(elapsed_time < 1):
            print('sleeping for sometime to match throughput...')
            time.sleep(1-elapsed_time)
        print(f'Average Throughput of Generator: {total_events/file_counter} tuples per second')
        file_counter+=1
        event_count = 0
        start_time = time.time()
    print('setting finish')
    if(file_counter%10 > 1 or file_counter%10 == 0):
        print(file_counter)
        tensecond.set()
    finish.set()


if __name__ == "__main__":
    generate_stream_data(10,20)
