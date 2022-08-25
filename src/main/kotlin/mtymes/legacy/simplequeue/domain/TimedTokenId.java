package mtymes.legacy.simplequeue.domain;

import javafixes.object.Microtype;

import java.util.UUID;

public class TimedTokenId extends Microtype<UUID> {

    /**
     * Constructor of {@code TokenId} initialized with value to wrap
     *
     * @param value value that should be wrapped and will be accessible from
     *              created {@code TokenId} instance
     * @throws IllegalArgumentException if {@code null} is passed as input parameter
     */
    public TimedTokenId(UUID value) {
        super(value);
    }

    public static TimedTokenId tokenId(UUID value) {
        return new TimedTokenId(value);
    }
}
