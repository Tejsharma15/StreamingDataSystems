import re
import time
import copy
import os
import math
import pandas as pd
import threading 

consumer_done = threading.Event()
consumer_temp = threading.Event()
consumer_fin = threading.Event()

# from generator import onesecond, tensecond

def find_pattern_in_window(regex, bounds, thread_name, window_start_id=1, window_end_id=2, checkFull=False, checkPrev=False):
    print(window_start_id, window_end_id)
    print(regex, bounds)
    start_time = time.time()
    if(checkFull == True):
        #release results for every 10 second data
        df = pd.DataFrame(columns = ['timestamp', 'integer', 'char'])
        count = 0
        i = window_start_id
        while(i <= window_end_id):
            if os.path.exists('./data/{}.csv'.format(i)):
                df1 = pd.read_csv('./data/{}.csv'.format(i))
                df = pd.concat([df, df1], axis = 0)
            if(i%10 == 0 or i > window_end_id):  
                df = df.reset_index()
                rawData = ''.join(df['char'])
                pairs = findPairs(regex, rawData, bounds, df)
                if(len(pairs.keys()) == len(regex)):
                    result = findCount(pairs[len(pairs)-1], pairs[len(pairs)-2], len(pairs)-2, pairs)
                else:
                    result = 0
                count += result
                print('The {}th window result is: {}'.format(math.ceil(i/10), result))
                df = pd.DataFrame()
            i+=1
        consumer_done.result = count
        consumer_done.set()
        print("Consumer process has computed on the whole dataset in...{} seconds".format(time.time() - start_time))
        return count
    elif(checkPrev == False):
        #Releases the count for a particular window
        df = pd.DataFrame()
        i = window_start_id
        while(i <= window_end_id):
            if os.path.exists('./data/{}.csv'.format(i)):
                df1 = pd.read_csv('./data/{}.csv'.format(i))
                df = pd.concat([df, df1])
            i+=1
        df = df.reset_index()
        rawData = ''.join(df['char'])
        result = 0
        pairs = findPairs(regex, rawData, bounds, df)
        if(len(pairs.keys()) == len(regex)):
            result = findCount(pairs[len(pairs)-1], pairs[len(pairs)-2], len(pairs)-2, pairs)
        else:
            result = 0
        print('The result for this window is: {} and thread is {}'.format(result, thread_name))
        consumer_temp.result = result
        return [result, thread_name]


def findPairs(pattern, s, bounds, df):
    n = len(pattern)
    # print(s)
    m = len(s)
    pairs = {}
    for i in range(len(bounds)):
        for j in range(len(s)):
            if(s[j] == bounds[i][0]):
                k = j+1
                while(k<len(s) and df.loc[k,'timestamp'] <= bounds[i][1]+df.loc[j, 'timestamp']):
                    if(s[k] == bounds[i][0]):
                        if i not in pairs:
                            pairs[i] = []
                            pairs[i].append((j,k))
                        else:
                            pairs[i].append((j, k))
                    k+=1
                # print("found all in the range of this")
    # print(pairs)
    return pairs


def findCount(curr, prev, k, pairs):
    # print("Intial Searching between: {} and {}".format(curr, prev))
    if(prev == []):
        return len(curr)
    temp = copy.deepcopy(prev)
    ans = 0
    for i in curr:
        # print("Searching between: {} and {}".format(i, prev))
        executed = 0
        for j in prev:
            executed+=1
            if(i[0]>=j[0] and i[1]<=j[1]):
                pass
                # print("Keeping {} and {}".format(i, j))
            else:
                # print(j)
                temp.remove(j)
        # print('The overlapping intervals: {}'.format(temp))
        if(k-1 < 0):
            ans += findCount(temp, [], k-1, pairs)
        else:
            ans += findCount(temp, pairs(k-1), k-1, pairs)
        temp = copy.deepcopy(prev)
        # print('Temp is: {}'.format(temp))
        # print('At the end of this recursive iteration the partial answer is: {}'.format(ans))
    return ans
# if(len(pairs.keys()) == len(S2)):
#     print(findCount(pairs[len(pairs)-1], pairs[len(pairs)-2], len(pairs)-2))
# else:
#     print(0)






if __name__ == "__main__":
    file_name = 'stream_data.csv'  # Use the same filename generated by the generator program
    # regex_pattern = "[BCDFGHJKLMNPQRSTVWXYZ][AEIOU]+[BCDFGHJKLMNPQRSTVWXYZ]?"
    regex = "AB"
    bounds = [['A', 3], ['B', 2]]
    window_duration = 2
    duration = 2
    print(find_pattern_in_window(regex, bounds, 1, 10, False, False))
