package com.mongodb.util;

import java.util.*;

/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
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

public interface ObjectPool<T> {
    /**
     * callback to determine if an object is validate to be added back to the pool or used will be called when something
     * is put back into the queue and when it comes out
     *
     * @return true if the object is validate to be added back to pool
     */
    boolean validate( T t );

    /**
     * override this if you need to do any cleanup
     */
    void cleanup( T t );

    /**
     * call done when you are done with an object form the pool if there is room and the object is validate will get
     * added
     *
     * @param t Object to add
     */
    void done( T t );

    void remove( T t );

    /**
     * Gets an object from the pool - will block if none are available
     *
     * @return An object from the pool
     */
    T get();

    /**
     * Gets an object from the pool - will block if none are available
     *
     * @param waitTime negative - forever 0        - return immediately no matter what positive ms to wait
     *
     * @return An object from the pool
     */
    T get( long waitTime );


}
