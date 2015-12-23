#!/usr/bin/env python

# Copyright (c) 2011 John Reese
# Licensed under the MIT License

import datetime
import multiprocessing
import signal
import time


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def run_worker():
    time.sleep(15)


def poolExample():
    now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print "{0} Initializing 5 workers (each sleeps for 15 seconds)".format(now)
    pool = multiprocessing.Pool(5, init_worker)

    running = dict()
    for i in range(3):
        result_obj = pool.apply_async(run_worker)
        running[result_obj] = result_obj.ready()

    try:
        now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print "{0} Waiting until all workers have completed".format(now)
        while False in running.values():
            for result_obj in running:
                running[result_obj] = result_obj.ready()

    except KeyboardInterrupt:
        now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print
        print "{0} Caught KeyboardInterrupt, terminating workers".format(now)
        pool.terminate()
        pool.join()
        return

    else:
        now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print "{0} Quitting normally".format(now)
        pool.close()
        pool.join()


class ConsumerProcess(multiprocessing.Process):
    def __init__(self, q, *args, **kwargs):
        self.q = q
        super(ConsumerProcess, self).__init__(*args, **kwargs)
        self.start()

    def run(self):
        init_worker()
        ps = list()
        for d in iter(self.q.get, None):
            if d == "killjobs":
                for p in ps:
                    p.terminate()
            else:
                ps.append(multiprocessing.Process(target=run_worker))
                ps[-1].daemon = True
                ps[-1].start()

        for p in ps:
            p.join()


def processExample():
    now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print "{0} Initializing consumer process".format(now)
    q = multiprocessing.Queue()

    p = ConsumerProcess(q)

    now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print "{0} Starting 3 jobs (each sleeps for 15 seconds)".format(now)
    for i in range(3):
        q.put(i)

    try:
        now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print "{0} Waiting 10 seconds".format(now)
        time.sleep(10)

    except KeyboardInterrupt:
        now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print
        print "{0} Caught KeyboardInterrupt, terminating consumer".format(now)
        q.put("killjobs")

    else:
        now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print "{0} Quitting normally".format(now)

    finally:
        q.put(None)
        q.close()
        p.join()


if __name__ == "__main__":
    poolExample()
    processExample()
    now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print "{0} Done".format(now)
