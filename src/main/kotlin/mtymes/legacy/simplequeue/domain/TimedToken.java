package mtymes.legacy.simplequeue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;

public class TimedToken extends DataObject {

    public final WorkId workId;
    public final TimedTokenId tokenId;
    public final ZonedDateTime validUntil;

    public TimedToken(WorkId workId, TimedTokenId tokenId, ZonedDateTime validUntil) {
        this.workId = workId;
        this.tokenId = tokenId;
        this.validUntil = validUntil;
    }

    public WorkId getWorkId() {
        return workId;
    }

    public TimedTokenId getTokenId() {
        return tokenId;
    }

    public ZonedDateTime getValidUntil() {
        return validUntil;
    }
}
