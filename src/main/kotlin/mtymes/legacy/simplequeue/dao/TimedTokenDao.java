package mtymes.legacy.simplequeue.dao;

import mtymes.legacy.simplequeue.domain.TimedToken;
import mtymes.legacy.simplequeue.domain.TimedTokenId;
import mtymes.legacy.simplequeue.domain.WorkId;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public interface TimedTokenDao {

    Optional<TimedToken> tryToAcquireToken(WorkId workId, long forDuration, TimeUnit unit);

    TimedToken tryToProlongToken(WorkId workId, TimedTokenId tokenId, long forDuration, TimeUnit unit);

    void releaseToken(WorkId workId, TimedTokenId tokenId);

}
