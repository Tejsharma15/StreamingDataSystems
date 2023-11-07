import time
import threading
import concurrent.futures
from consumer import find_pattern_in_window, consumer_done, consumer_temp, consumer_fin
from generator import generate_stream_data, tempsecond, tensecond, finish

def start_system(regex_pattern, genProb, duration, throughput_per_sec):
    print(regex_pattern, duration)
    regex, bounds = refineInput(regex_pattern)
    maxWindowSize = bounds[0][1]
    start_system_time = time.time()
    generator_thread = threading.Thread(target=generate_stream_data, args=(regex, genProb, throughput_per_sec, duration, maxWindowSize))
    generator_thread.start()

    # Method 1

    # generator_thread.join()
    # print('done with generating full data')
    # consumer_thread = threading.Thread(target=find_pattern_in_window, args = (regex, bounds, "thread_1", 1, duration,True,True))
    # consumer_thread.start()
    # consumer_thread.join()
    # if consumer_done.is_set():
    #     result = consumer_done.result
    #     print(result)
    #     print('Time taken to generate and process: ', time.time()-start_system_time)

    # #Method 2

    # i=1
    # with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    #     while(finish.is_set() == False):
    #         if tensecond.is_set() == True and finish.is_set() == False:
    #             print('running 10 sec')
    #             thread_name = f"consumer_thread_{i}"
    #             tensecond.clear()
    #             # Submit the task to the thread pool
    #             future = executor.submit(find_pattern_in_window, regex, bounds, thread_name, i, i+maxWindowSize-1, False, False)
    #             i = i+10
    #             result = future.result()
    #             print("The result for the window - {} is {}".format(result[1].split("_")[2], result[0]))
    # with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    #     if(tensecond.is_set()):
    #         print('Running for the last ten second window')
    #         tensecond.clear() 
    #         future = executor.submit(find_pattern_in_window, regex, bounds, thread_name, i, i+maxWindowSize-1, False, False)
    #         i = i+10
    #         result = future.result()
    #         print("The result for the window - {} is {}".format(result[1].split("_")[2], result[0]))            
    # print('Time taken to generate and process ', time.time()-start_system_time)

    # Method 3

    i = 1
    mapping = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        while not finish.is_set():
            if tempsecond.is_set():
                tempsecond.clear()
                print('running individual sec')
                thread_name = f"consumer_thread_{i}"
                # Submit the task to the thread pool
                future = executor.submit(find_pattern_in_window, regex, bounds, thread_name, i, i+maxWindowSize-1, False, False)
                i = i + 1
                futures.append(future)  # Store the Future object
        
                # Periodically check for completed threads and retrieve their results
                for future in futures:
                    if future.done():
                        result = future.result()
                        key = int((int(result[1].split("_")[2])+maxWindowSize-1)/10)
                        if((int(result[1].split("_")[2])+maxWindowSize-1)%10 == 0): key = key-1
                        if key not in mapping:
                            print("{} is not there in map".format(key))
                            mapping[key%10] = [result[0], 1]
                        else:
                            print("{} is there in map".format(key))
                            mapping[key][0] += result[0]
                            mapping[key][1] += 1
                            print(mapping[key][1])
                        if(mapping[key][1] == 10-maxWindowSize+1):
                            print(key, mapping[key][0])
                        futures.remove(future)

    with concurrent.futures.ThreadPoolExecutor(max_workers = 1) as executor:
        if tempsecond.is_set():
            thread_name = f"consumer_thread_{i}"
            tempsecond.clear()
            # Submit the task to the thread pool
            future = executor.submit(find_pattern_in_window, regex, bounds, thread_name, i, i+maxWindowSize-1, False, False)
            i = i + 1
            result = future.result()
            key = int((int(result[1].split("_")[2])+maxWindowSize-1)/10)
            if((int(result[1].split("_")[2])+maxWindowSize-1)%10 == 0): key = key-1
            if key not in mapping:
                print("{} is not there in map".format(key))
                mapping[key%10] = [result[0], 1]
            else:
                print("{} is there in map".format(key))
                mapping[key][0] += result[0]
                mapping[key][1] += 1
            if((key == 0 and mapping[key][1] == 10-maxWindowSize+1) or (key >= 1 and mapping[key][1] == 10)):
                print(key, mapping[key][0])

    print('Time taken to generate and process ', time.time()-start_system_time)


def refineInput(regex_pattern):
    regex = ""
    bounds = []
    temp = []
    for i in range(len(regex_pattern)):
        if(regex_pattern[i].isalpha()):
            regex += regex_pattern[i]
            temp.append(regex_pattern[i])
        else:
            if(regex_pattern[i].isdigit()):
                temp.append(int(regex_pattern[i]))
                bounds.append(temp)
                temp = []
    return regex, bounds

if __name__ == '__main__':
    regex_pattern = "A[<=2]B[<=1]"
    duration = 20
    window_size = 2
    throughput_per_sec = 1000
    start_system(regex_pattern=regex_pattern, genProb=0.005, duration=duration, throughput_per_sec = throughput_per_sec)        
    
