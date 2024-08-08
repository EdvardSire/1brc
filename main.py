#!/usr/bin/env python3
import time
import sys
from collections import defaultdict


HARD=True

def a():
    """ 10.09311842918396 """
    start_time = time.time()
    with open("./measurements_1e8.txt", "r" ) as file:
        i = 0 
        for line in file.readlines():
            i += 1
            if i % 1e7 == 0:
                print(line, i)
                sys.stdout.flush()
        print(time.time() - start_time)

def b():
    from concurrent.futures import ThreadPoolExecutor
    def process(lines):
        print("in process")
        sys.stdout.flush()
        # avg, min, max
        data = defaultdict(lambda: [float(0), float("inf"), float("-inf")])
        
        start_time = time.time()
        for line in lines:
            place, temp = line.split(b';')

            temp = float(temp)
            data[place][0] = (data[place][0] + temp)/2
             
            if temp < data[place][1]:
                data[place][1] = temp
            if temp > data[place][2]:
                data[place][2] = temp

        print("hotloop: ", time.time()- start_time) 
        sys.stdout.flush()
        return data

    """ 
    86.07834482192993

       ncalls  tottime  percall  cumtime  percall filename:lineno(function)
          8/1   99.848   12.481    0.081    0.081 main.py:23(process)
    100000000   29.044    0.000   29.044    0.000 {method 'split' of 'bytes' objects}
            1    4.091    4.091    4.091    4.091 {method 'readlines' of '_io._IOBase' objects}
       102/79    1.126    0.011  392.650    4.970 {method 'acquire' of '_thread.lock' objects}
          8/1    0.699    0.087  131.812  131.812 threading.py:999(run)
            1    0.653    0.653  136.614  136.614 main.py:1(<module>)
          8/1    0.293    0.037    0.091    0.091 thread.py:53(run)
          8/1    0.229    0.029  131.260  131.260 thread.py:69(_worker)
    3304/2889    0.199    0.000    0.234    0.000 main.py:28(<lambda>)
    """

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=(max_workers := 8)) as executor:
        with open("./measurements_1e8.txt", "rb" ) as file:
            lines = file.readlines()
            file_lenght = len(lines) #4.318 {method 'readlines' of '_io._IOBase' objects}                    
            increment = file_lenght/max_workers
            assert increment == int(increment)
            increment = int(increment)
            print("starting")
            sys.stdout.flush()
            

            futures = list()
            for i in range(max_workers):
                 futures.append(executor.submit(process, lines[increment*(i):increment*(i+1)]))

            for place in sorted(futures[0].result()):
                average = float(0)
                min = float("inf")
                max = float("-inf")
                for future in futures:
                    average = (future.result()[place][0] + average)/2
                    if min > future.result()[place][1]:
                        min = future.result()[place][1]
                    if max < future.result()[place][2]:
                        max = future.result()[place][2]

                print(str(place), average, min, max)

            print(time.time() - start_time)


def c():
    from concurrent.futures import ThreadPoolExecutor
    def process(lines):
        print("in process")
        sys.stdout.flush()
        # avg, min, max
        data = defaultdict(lambda: [int(0), int(100), int(-100)])
        
        start_time = time.time()
        for line in lines:
            place, temp = line.split(b';')

            temp = int(float(temp)*10)
            data[place][0] += temp

            if temp < data[place][1]:
                data[place][1] = temp
            if temp > data[place][2]:
                data[place][2] = temp

        print("hotloop: ", time.time()- start_time) 
        sys.stdout.flush()
        return data

    """ 
    """

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=(max_workers := 8)) as executor:
        with open("./measurements_1e8.txt", "rb" ) as file:
            lines = file.readlines()
            file_lenght = len(lines) #4.318 {method 'readlines' of '_io._IOBase' objects}                    
            increment = file_lenght/max_workers
            assert increment == int(increment)
            increment = int(increment)
            print("starting")
            sys.stdout.flush()
            

            futures = list()
            for i in range(max_workers):
                 futures.append(executor.submit(process, lines[increment*(i):increment*(i+1)]))

            for place in sorted(futures[0].result()):
                average = int(0)
                min = int(100)
                max = int(-100)
                for future in futures:
                    average += future.result()[place][0]
                    if min > future.result()[place][1]:
                        min = future.result()[place][1]
                    if max < future.result()[place][2]:
                        max = future.result()[place][2]

                print(str(place), average, min, max)

            print(time.time() - start_time)

if __name__ == "__main__":
    # a()
    # b()
    c()
