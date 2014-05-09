#!/usr/bin/env python

# Copyright (c) 2011 John Reese
# Licensed under the MIT License

import multiprocessing
import signal
import time

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def run_worker():
    time.sleep(15)
    
def poolExample():
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


class ConsumerProcess( multiprocessing.Process ):
    def __init__( self, q, *args, **kwargs ):
        self.q = q
        super( ConsumerProcess, self ).__init__( *args, **kwargs )
        self.start()

    def run( self ):
        init_worker()
        ps = []
        for d in iter( self.q.get, None ):
            if( d == 'killjobs' ):
                for p in ps:
                    p.terminate()

            else:    
                ps.append( multiprocessing.Process( target=run_worker ) )
                ps[-1].daemon = True
                ps[-1].start()

        for p in ps:
            p.join()


def processExample():
    print "Initializing consumer process"
    q = multiprocessing.Queue()

    p = ConsumerProcess( q )

    print "Starting 3 jobs of 15 seconds each"
    for i in range(3):
        q.put( i )

    try:
        print "Waiting 10 seconds"
        time.sleep( 10 )

    except KeyboardInterrupt:
        print "Caught KeyboardInterrupt, terminating consumer"
        q.put( 'killjobs' )

    else:
        print "Quitting normally"

    finally:
        q.put( None )
        q.close()
        p.join()


if __name__ == "__main__":
    poolExample()
    processExample()
