package org.mongodb.codecs.validators;

import org.mongodb.Document;

import static java.lang.String.format;

public class FindAndUpdateValidator implements Validator<Document> {
    @Override
    public void validate(final Document value) {
        for (String field : value.keySet()) {
            if (field.startsWith("$")) {
                return;
            }
        }
        throw new IllegalArgumentException(format("Find and update requires an update operator (beginning with '$') in the update "
                                                  + "Document: %s", value));
    }
}
