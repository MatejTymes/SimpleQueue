package mtymes.legacy.simplequeue.domain;

import javafixes.object.Microtype;

public class WorkId extends Microtype<String> {

    /**
     * Constructor of {@code WorkId} initialized with value to wrap
     *
     * @param value value that should be wrapped and will be accessible from
     *              created {@code WorkId} instance
     * @throws IllegalArgumentException if {@code null} is passed as input parameter
     */
    public WorkId(String value) {
        super(value);
    }

    public static WorkId workId(String value) {
        return new WorkId(value);
    }
}
