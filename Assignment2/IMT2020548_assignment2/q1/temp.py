import re
import copy
# pattern = r'A\.*B\.*B\.*A'
# pattern = "ABBA"
# s = "AXLLXB"
# s = "AJHQOHSWKKOUBZUTDZLUNABXXENDFRWBGJDYWQANNWIAZABTXNEMTDZKDACFMPFDBRFHDZMEAMZMQKUHHBJGZCPRJIOACNQQQJYL"
# print(list(re.finditer(pattern, s)))
# match = re.findall(pattern, s)
# for matches in match:
#     print(matches)

 
# S1 = "AJHQOHSWKKOUBZUTDZLUNABXXENDFRWBGJDYWQANNWIAZABTXNEMTDZKDACFMPFDBRFHDZMEAMZMQKUHHBJGZCPRJIOACNQQQJYL"
# S2 = "ABBA"
S1 = "AXBALBAIRXGABAE"
S2 = "AB"
bounds = [('A', 100), ('B', 100)]

#For each char in the pallindrome (1st half considered only), find all possible windows. 
#Starting from the first char of the pallindrome, merge the results i.e merge all windows of the second character
#which are completely contained in a the particular window of the second character. 

def findPairs(pattern, s, bounds):
    n = len(pattern)
    m = len(s)
    pairs = {}
    for i in range(len(bounds)):
        for j in range(len(s)):
            if(s[j] == bounds[i][0]):
                k = j+1
                while(k<len(s) and k <= bounds[i][1]+j):
                    # print(k, s[k])
                    if(s[k] == bounds[i][0]):
                        if i not in pairs:
                            pairs[i] = []
                            pairs[i].append((j,k))
                        else:
                            pairs[i].append((j, k))
                    k+=1
                # print("found all in the range of this")
    return pairs

pairs = findPairs(S2, S1, bounds)
print(pairs)
#Check if for each char in pallindrome, there is at least one pair found. Else count = 0
def findCount(curr, prev, k):
    print("Intial Searching between: {} and {}".format(curr, prev))
    if(prev == []):
        return len(curr)
    temp = copy.deepcopy(prev)
    ans = 0
    for i in curr:
        print("Searching between: {} and {}".format(i, prev))
        for j in prev:
            if(i[0]>=j[0] and i[1]<=j[1]):
                pass
            else:
                temp.remove(j)
        if(k-1 < 0):
            ans += findCount(temp, [], k-1)
        else:
            ans += findCount(temp, pairs(k-1), k-1)
        temp = copy.deepcopy(prev)
        print('Temp is: {}'.format(temp))
        print('At the end of this iteration the partial answer is: {}'.format(ans))
    return ans
if(len(pairs.keys()) == len(S2)):
    print(findCount(pairs[len(pairs)-1], pairs[len(pairs)-2], len(pairs)-2))
else:
    print(0)
