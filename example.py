#!/usr/bin/env python

# Copyright (c) 2011 John Reese
# Licensed under the MIT License

import multiprocessing
import os
import signal
import time

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def run_worker():
    time.sleep(15)
    
def main():
    print "Initializng 5 workers"
    pool = multiprocessing.Pool(5, init_worker)

    print "Starting 3 jobs of 15 seconds each"
    for i in range(3):
        pool.apply_async(run_worker)

    try:
        print "Waiting 10 seconds"
        time.sleep(10)

    except KeyboardInterrupt:
        print "Caught KeyboardInterrupt, terminating workers"
        pool.terminate()
        pool.join()

    else:
        print "Quitting normally"
        pool.close()
        pool.join()

if __name__ == "__main__":
    main()
