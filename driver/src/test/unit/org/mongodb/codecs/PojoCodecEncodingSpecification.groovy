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

package org.mongodb.codecs

import org.bson.BSONWriter
import org.mongodb.codecs.pojo.Address
import org.mongodb.codecs.pojo.Name
import org.mongodb.codecs.pojo.ObjectWithArray
import org.mongodb.codecs.pojo.ObjectWithMapOfStrings
import org.mongodb.codecs.pojo.Person
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Subject

import static org.junit.Assert.fail

class PojoCodecEncodingSpecification extends Specification {
    private final BSONWriter bsonWriter = Mock();

    @Subject
    private final PojoCodec<Object> pojoCodec = new PojoCodec<Object>(Codecs.createDefault(), null);

    def 'should encode simple pojo'() {
        given:
        String valueInSimpleObject = 'MyName';

        when:
        pojoCodec.encode(bsonWriter, new SimpleObject(valueInSimpleObject));

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('name');
        then:
        1 * bsonWriter.writeString(valueInSimpleObject);
        then:
        1 * bsonWriter.writeEndDocument();
    }

    def 'should encode pojo containing other pojos'() {
        given:
        String anotherName = 'AnotherName';

        when:
        pojoCodec.encode(bsonWriter, new NestedObject(new SimpleObject(anotherName)));

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('mySimpleObject');
        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('name');
        then:
        1 * bsonWriter.writeString(anotherName);
        then:
        2 * bsonWriter.writeEndDocument();
    }

    def 'should encode pojo containing other pojos and fields'() {
        when:
        pojoCodec.encode(bsonWriter, new NestedObjectWithFields(98, new SimpleObject('AnotherName')));

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('intValue');
        then:
        1 * bsonWriter.writeInt32(98);
        then:
        1 * bsonWriter.writeName('mySimpleObject');
        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('name');
        then:
        1 * bsonWriter.writeString('AnotherName');
        then:
        2 * bsonWriter.writeEndDocument();
    }

    def 'should support arrays'() {
        when:
        pojoCodec.encode(bsonWriter, new ObjectWithArray());

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('theStringArray');
        then:
        1 * bsonWriter.writeStartArray();
        then:
        1 * bsonWriter.writeString('Uno');
        then:
        1 * bsonWriter.writeString('Dos');
        then:
        1 * bsonWriter.writeString('Tres');
        then:
        1 * bsonWriter.writeEndArray();
        then:
        1 * bsonWriter.writeEndDocument();
    }

    def 'should encode maps of primitive types'() {
        when:
        pojoCodec.encode(bsonWriter, new ObjectWithMapOfStrings());

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('theMap');
        then:
        1 * bsonWriter.writeStartDocument();

        1 * bsonWriter.writeName('first');
        1 * bsonWriter.writeString('the first value');
        1 * bsonWriter.writeName('second');
        1 * bsonWriter.writeString('the second value');

        then:
        2 * bsonWriter.writeEndDocument();
    }

    //TODO: trish
    @Ignore('FIX ME')
    def 'should encode maps of objects'() {
        given:
        String simpleObjectValue = 'theValue';

        when:
        pojoCodec.encode(bsonWriter, new ObjectWithMapOfObjects(simpleObjectValue));

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('theMap');
        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('first');
        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('name');
        then:
        1 * bsonWriter.writeString(simpleObjectValue);
        then:
        3 * bsonWriter.writeEndDocument();
    }

    def 'should encode maps of maps'() {
        when:
        pojoCodec.encode(bsonWriter, new ObjectWithMapOfMaps());

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('theMap');
        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('theMapInsideTheMap');
        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('innerMapField');
        then:
        1 * bsonWriter.writeString('theInnerMapFieldValue');
        then:
        3 * bsonWriter.writeEndDocument();
    }

    def 'should not encode special fields like jacoco data'() {
        given:
        JacocoDecoratedObject jacocoDecoratedObject = new JacocoDecoratedObject('thisName');

        when:
        pojoCodec.encode(bsonWriter, jacocoDecoratedObject);

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeEndDocument();
    }

    def 'should encode complex pojo'() {
        given:
        Address address = new Address();
        Name name = new Name();
        Person person = new Person(address, name);

        when:
        pojoCodec.encode(bsonWriter, person);

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('address');

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('address1');
        then:
        1 * bsonWriter.writeString(address.getAddress1());
        then:
        1 * bsonWriter.writeName('address2');
        then:
        1 * bsonWriter.writeString(address.getAddress2());
        then:
        1 * bsonWriter.writeEndDocument();

        then:
        1 * bsonWriter.writeName('name');
        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('firstName');
        then:
        1 * bsonWriter.writeString(name.getFirstName());
        then:
        1 * bsonWriter.writeName('surname');
        then:
        1 * bsonWriter.writeString(name.getSurname());

        then:
        2 * bsonWriter.writeEndDocument();
    }

    def 'should ignore transient fields'() {
        given:
        String value = 'some value';

        when:
        pojoCodec.encode(bsonWriter, new ObjWithTransientField(value));

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeEndDocument();

        0 * bsonWriter.writeName('transientField');
        0 * bsonWriter.writeString(value);
    }

    @Ignore('not implemented')
    def 'should encode ids'() {
        given:
        fail('Not implemented');
    }

    @Ignore('not implemented')
    def 'should throw an exception when it cannot encode a field'() {
        given:
        fail('Not implemented');
    }

    @Ignore('not implemented')
    def 'should encode enums as strings'() {
        given:
        fail('Not implemented');
    }

    private static class JacocoDecoratedObject {
        @SuppressWarnings('FieldName')
        private final String $name;

        JacocoDecoratedObject(String name) {
            this.$name = name;
        }
    }

    private static class SimpleObject {
        private final String name;

        SimpleObject(String name) {
            this.name = name;
        }
    }

    private static class ObjWithTransientField {
        @SuppressWarnings('UnnecessaryTransientModifier')
        private final transient String transientField;

        private ObjWithTransientField(String transientField) {
            this.transientField = transientField;
        }
    }

    private static class NestedObject {
        private final SimpleObject mySimpleObject;

        NestedObject(SimpleObject mySimpleObject) {
            this.mySimpleObject = mySimpleObject;
        }
    }

    private static class NestedObjectWithFields {
        private final int intValue;
        private final SimpleObject mySimpleObject;

        NestedObjectWithFields(int intValue, SimpleObject mySimpleObject) {
            this.intValue = intValue;
            this.mySimpleObject = mySimpleObject;
        }
    }

    private static final class ObjectWithMapOfObjects {
        private final Map<String, SimpleObject> theMap = [:];

        private ObjectWithMapOfObjects(String theValue) {
            theMap.put('first', new SimpleObject(theValue));
        }
    }

    private static final class ObjectWithMapOfMaps {
        def theMap = ['theMapInsideTheMap': ['innerMapField': 'theInnerMapFieldValue']];
    }

}
