package mtymes.tasks.distributedLock.dao

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.common.mongo.MongoCollectionExt.insert
import mtymes.tasks.common.time.Clock
import mtymes.tasks.common.time.UTCClock
import mtymes.tasks.distributedLock.domain.LockId
import org.bson.Document
import java.time.Duration

class DistributedLockDao(
    val coll: MongoCollection<Document>,
    val clock: Clock = UTCClock()
) {

    companion object {

        val LOCK_RELEASER: WorkerId = WorkerId("LockReleaser")

        const val LOCK_ID = "_id"
        const val WORKER_ID = "workerId"
        const val LOCKED_UNTIL = "lockedUntil"
    }

    fun tryToAcquireALock(
        lockId: LockId,
        workerId: WorkerId,
        holdOnItFor: Duration
    ): Boolean {
        val now = clock.now()

        val ableToInsert = coll.insert(
            doc(
                LOCK_ID to lockId,
                WORKER_ID to workerId,
                LOCKED_UNTIL to now.plus(holdOnItFor)
            )
        )

        if (ableToInsert) {
            return true
        } else {
            val result = coll.updateOne(
                doc(
                    LOCK_ID to lockId,
                    "\$or" to doc(
                        WORKER_ID to workerId,
                        LOCKED_UNTIL to doc("\$lt" to now)
                    )
                ),
                doc(
                    "\$set" to doc(
                        WORKER_ID to workerId,
                        LOCKED_UNTIL to now.plus(holdOnItFor)
                    )
                )
            )

            return result.modifiedCount == 1L
        }
    }

    fun prolongLockDuration(
        lockId: LockId,
        workerId: WorkerId,
        holdOnItFor: Duration
    ): Boolean {
        val now = clock.now()

        val result = coll.updateOne(
            doc(
                LOCK_ID to lockId,
                WORKER_ID to workerId,
                LOCKED_UNTIL to doc("\$gte", now)
            ),
            doc(
                "\$set" to doc(
                    LOCKED_UNTIL to now.plus(holdOnItFor)
                )
            )
        )

        return result.modifiedCount == 1L
    }

    fun releaseLock(
        lockId: LockId,
        workerId: WorkerId,
        makeAvailableAfter: Duration
    ): Boolean {
        val now = clock.now()

        val result = coll.updateOne(
            doc(
                LOCK_ID to lockId,
                WORKER_ID to workerId,
                LOCKED_UNTIL to doc("\$gte", now)
            ),
            doc(
                "\$set" to doc(
                    WORKER_ID to LOCK_RELEASER,
                    LOCKED_UNTIL to now.plus(makeAvailableAfter)
                )
            )
        )

        return result.modifiedCount == 1L
    }
}