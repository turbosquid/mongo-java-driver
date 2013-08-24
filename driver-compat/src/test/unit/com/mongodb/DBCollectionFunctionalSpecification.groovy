/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import spock.lang.Ignore

class DBCollectionFunctionalSpecification extends FunctionalSpecification {

    //TODO: test fixed but I think the functionality needs to be changed to get this test to work the way it used to

    def setupSpec() {
        Map.metaClass.bitwiseNegate = { new BasicDBObject(delegate) }
    }

    def setup() {
        collection.insert(new BasicDBObject('a', new BasicDBObject()).append('b', new BasicDBObject()));
        collection.setObjectClass(BasicDBObject)
    }

    def 'should use top-level class for findAndModify'() {
        given:
        collection.setObjectClass(ClassA)
        def originalValue = ~['a': 'value']
        collection.insert(originalValue);

        when:
        DBObject document = collection.findAndModify(originalValue, new BasicDBObject('c', 1).append('_id', originalValue.get('_id')))

        then:
        document instanceof ClassA

    }

    @Ignore('functionality has changed, needs fixing.')
    def 'should use internal classes for findAndModify'() {
        given:
        collection.setInternalClass('a', ClassA);
        collection.setInternalClass('b', ClassB);

        when:
        DBObject document = collection.findAndModify(new BasicDBObject(), new BasicDBObject('c', 1))

        then:
        document.get('a') instanceof ClassA
        document.get('b') instanceof ClassB
    }

    def 'should support index options'() {
        given:
        def options = ~[
                'sparse': true,
                'background': true,
                'expireAfterSeconds': 42,
                'somethingOdd': 'jeff'
        ]

        when:
        collection.ensureIndex(~['y': 1], options);

        then:
        collection.getIndexInfo().size() == 2

        DBObject document = collection.getIndexInfo()[1]
        document.get('expireAfterSeconds') == 42
        document.get('somethingOdd') == 'jeff'
        document.get('background') == true
        !document.containsField('dropDups')
    }

    def 'should should provided decoder factory for findAndModify'() {
        given:
        DBDecoder decoder = Mock()
        DBDecoderFactory factory = Mock()
        factory.create() >> decoder
        collection.setDBDecoderFactory(factory)
        def originalValue = ~['z': 'value']
        collection.insert(originalValue);

        when:
        collection.findAndModify(originalValue, new BasicDBObject('c', 1).append('_id', originalValue.get('_id')))

        then:
        1 * decoder.decode(_ as byte[], collection)
    }

    static class ClassA extends BasicDBObject { }
    static class ClassB extends BasicDBObject { }

}
