package mtymes.tasks.distributedLock.dao

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.check.ValidityChecks.expectNonNegativeDuration
import mtymes.tasks.common.check.ValidityChecks.expectPositiveDuration
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.MongoCollectionExt.insert
import mtymes.tasks.common.mongo.builder.WithCoreDocBuilder
import mtymes.tasks.common.time.Clock
import mtymes.tasks.common.time.UTCClock
import mtymes.tasks.distributedLock.domain.LockId
import org.bson.Document
import java.time.Duration

class DistributedLockDao(
    val coll: MongoCollection<Document>,
    val clock: Clock = UTCClock
) : WithCoreDocBuilder {

    companion object {

        val LOCK_RELEASER: WorkerId = WorkerId("LockReleaser")

        const val LOCK_ID = "_id"
        const val WORKER_ID = "workerId"
        const val ACQUIRED_AT = "acquiredAt"
        const val PROLONGED_AT = "prolongedAt"
        const val LOCKED_UNTIL = "lockedUntil"
        const val DELETABLE_AFTER = "deletableAfter"
    }

    fun tryToAcquireALock(
        lockId: LockId,
        workerId: WorkerId,
        holdOnItFor: Duration,
        ttl: Duration? = null
    ): Boolean {
        expectPositiveDuration("holdOnItFor", holdOnItFor)

        val now = clock.now()

        val ableToInsert = coll.insert(
            docBuilder()
                .putAll(
                    LOCK_ID to lockId,
                    WORKER_ID to workerId,
                    ACQUIRED_AT to now,
                    LOCKED_UNTIL to now.plus(holdOnItFor)
                )
                .putIf(ttl != null) {
                    DELETABLE_AFTER to now.plus(ttl!!)
                }
                .build()
        )

        if (ableToInsert) {
            return true
        } else {
            val result = coll.updateOne(
                doc(
                    LOCK_ID to lockId,
                    "\$or" to listOf(
                        doc(WORKER_ID to workerId),
                        doc(LOCKED_UNTIL to doc("\$lt" to now))
                    )
                ),
                doc(
                    "\$set" to docBuilder()
                        .putAll(
                            WORKER_ID to workerId,
                            LOCKED_UNTIL to now.plus(holdOnItFor)
                        )
                        .putIf(ttl != null) {
                            DELETABLE_AFTER to now.plus(ttl!!)
                        }
                        .build()
                )
            )

            return result.modifiedCount == 1L
        }
    }

    fun prolongLockDuration(
        lockId: LockId,
        workerId: WorkerId,
        holdOnItFor: Duration,
        ttl: Duration? = null
    ): Boolean {
        expectPositiveDuration("holdOnItFor", holdOnItFor)

        val now = clock.now()

        val result = coll.updateOne(
            doc(
                LOCK_ID to lockId,
                WORKER_ID to workerId,
                PROLONGED_AT to now,
                LOCKED_UNTIL to doc("\$gte", now)
            ),
            doc(
                "\$set" to docBuilder()
                    .putAll(
                        LOCKED_UNTIL to now.plus(holdOnItFor)
                    )
                    .putIf(ttl != null) {
                        DELETABLE_AFTER to now.plus(ttl!!)
                    }
                    .build()
            )
        )

        return result.modifiedCount == 1L
    }

    fun releaseLock(
        lockId: LockId,
        workerId: WorkerId,
        makeAvailableAfter: Duration,
        ttl: Duration? = null
    ): Boolean {
        expectNonNegativeDuration("makeAvailableAfter", makeAvailableAfter)

        val now = clock.now()

        val result = coll.updateOne(
            doc(
                LOCK_ID to lockId,
                WORKER_ID to workerId,
                LOCKED_UNTIL to doc("\$gte", now)
            ),
            doc(
                "\$set" to docBuilder()
                    .putAll(
                        WORKER_ID to LOCK_RELEASER,
                        LOCKED_UNTIL to now.plus(makeAvailableAfter)
                    )
                    .putIf(ttl != null) {
                        DELETABLE_AFTER to now.plus(ttl!!)
                    }
                    .build(),
                "\$unset" to doc(
                    ACQUIRED_AT to 1,
                    PROLONGED_AT to 1
                )
            )
        )

        return result.modifiedCount == 1L
    }
}