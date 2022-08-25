package mtymes.legacy.simplequeue.worker;

import mtymes.legacy.simplequeue.dao.TimedTokenDao;
import mtymes.legacy.simplequeue.domain.TimedToken;

import java.time.Duration;
import java.util.Optional;

public class TokenBasedWorker extends GenericSingleTaskWorker<TimedToken> {

    private final TimedTokenDao timedTokenDao;

    protected TokenBasedWorker(
            TimedTokenDao timedTokenDao,
            Duration waitDurationIfNoTaskAvailable,
            Duration heartBeatPeriod
    ) {
        super(waitDurationIfNoTaskAvailable, heartBeatPeriod);
        this.timedTokenDao = timedTokenDao;
    }

    @Override
    public Optional<TimedToken> pickNextTask() throws Exception {
        return Optional.empty();
    }

    @Override
    public void updateHeartBeat(TimedToken timedToken) throws Exception {

    }

    @Override
    public void executeTask(TimedToken timedToken) throws Exception {

    }

    @Override
    public void handleExecutionFailure(TimedToken timedToken, Exception exception) throws Exception {

    }
}
