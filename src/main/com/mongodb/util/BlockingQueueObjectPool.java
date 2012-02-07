/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.mongodb.util;


import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * A LinkedBlockingQueue based object pool implementation, <br>
 * intended to be concurrent and performant while still thread-safe.
 *
 * Based upon the similarly named implementation from
 * the Apache 2.0 licensed "Furious Object Pool",
 * http://code.google.com/p/furious-objectpool/
 *
 * * TODO - Should we keep a Soft or Weak Reference list of items we have on loan?
 *
 * @author Brendan W. McAdams <brendan@10gen.com>
 */
public abstract class BlockingQueueObjectPool<T> implements ObjectPool<T> {

    /**
     * See full constructor docs
     */
    public BlockingQueueObjectPool( String name , int maxToKeep , int maxTotal ){
        this( name , maxToKeep , maxTotal , false , false );
    }

    /** Initializes a new pool of objects.
     * @param name name for the pool
     * @param maxToKeep max to hold to at any given time. if < 0 then no limit
     * @param maxTotal max to have allocated at any point.  if there are no more, get() will block
     * @param trackLeaks if leaks should be tracked
     */
    public BlockingQueueObjectPool( String name , int maxToKeep , int maxTotal , boolean trackLeaks , boolean debug ){
        _poolName = name;
        _maxIdle = maxToKeep;
        _maxSize = maxTotal;
        _trackLeaks = trackLeaks;
        _debug = true;
        init();
    }

    /** Initialize pool */
    protected void init() {
        for (int n = 0; n < _minSize; n++) 
            newInstance();
    }
    
    protected void newInstance() {
       T obj = createNew();
       totalSize.incrementAndGet();
       queue.add( obj );
    }
    
    
    public void done(final T t) {
        done( t, validate( t ) ); 
    }
    
    void done(final T obj, boolean valid) {
        if ( !valid ){
            totalSize.decrementAndGet();
        } else {
            if (_maxSize < 0 || numIdle() < _maxIdle) {
                queue.offer( obj );
            } else {
                totalSize.decrementAndGet(); // clean up and move on
            }
        }
    }

    /**
     * Explicitly remove an object from the pool.
     * @param t
     */
    public void remove( T t ){
        done( t, false );
    }

    /*
    public void clearIdle(int nObjects) {
        for( int n = 0; n < nObjects; n++) {
            T obj = queue.poll();
            if (obj != null)
                remove( obj );
        }
    }*/
    
    public T get(){
        return get( _maxWait );
    }

    public T get( long waitTime ){
        // commented out and refactored as This may have a race issue where after we create a new instance for ourselves we don't get it back?
        /*if (queue.size() == 0 && totalSize.get() < _maxSize)
            newInstance();
        */

        T obj = null;
        // For thread safety / no racing increment total size, create an item but DO NOT put it in the queue at all.
        // TODO - Is this joint operation properly atomic?
        if (queue.size() == 0 && totalSize.get() < _maxSize) {
            obj = createNew();
            totalSize.incrementAndGet();
        } else {
            try {
                obj = queue.poll( _maxWait, TimeUnit.SECONDS );
            } catch (InterruptedException ie){
                throw new  IllegalStateException( "Unable to provide a pooled object.", ie );
            }
        }

        return obj;

    }

    public int numActive() {
        return totalSize.get() - queue.size();
    }
    
    public int numIdle() {
        return queue.size();
    }

    /*public void validateIdle() {

        for ( T obj : queue ) {
            if (obj != null) {
                if ( validate( obj ) )

            }
        }
    }*/

    /** Creates a new object of this pool's type.
     * @return the new object.
     */
    protected abstract T createNew();

    /**
     * callback to determine if an object is validate to be added back to the pool or used
     * will be called when something is put back into the queue and when it comes out
     * @return true if the object is validate to be added back to pool
     */
    public abstract boolean validate( T t );

    /**
     * override this if you need to do any cleanup
     */
    public void cleanup( T t ){}

    /**
     * Time to wait in seconds before a get fails
     */
    public static final int DEFAULT_MAX_WAIT_SECS = -1; // wait forever by default
    /**
     * Minimum pool objects to keep by default
     */
    public static final int DEFAULT_MIN_KEEP = 0; // default to none to fit our existing implementation
    /**
     * Max pool objects to keep allocated at any given time. If < 0 then no limit
     */
    public static final int DEFAULT_MAX_KEEP = -1;

    /** Pool settings */
    public final String _poolName;
    private int _maxWait = DEFAULT_MAX_WAIT_SECS;
    private int _minSize;
    private int _maxSize;
    private int _maxIdle;
    
    /** State */
    final AtomicInteger totalSize = new AtomicInteger( 0 );
    final LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<T>(  );


    /** Debug mode */
    private boolean _debug = true;
    // todo - implement leak tracking / JMX
    private boolean _trackLeaks = false;

}
