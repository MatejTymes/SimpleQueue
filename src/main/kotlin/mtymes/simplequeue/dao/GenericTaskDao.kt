package mtymes.simplequeue.dao

import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import mtymes.common.mongo.DocBuilder.Companion.doc
import mtymes.common.mongo.DocBuilder.Companion.docBuilder
import mtymes.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.common.network.HostUtil.longLocalHostName
import mtymes.common.time.Clock
import org.bson.Document
import java.time.Duration
import java.util.*

// todo: add client
// todo: turn into features: heartBeat, retry count
abstract class GenericTaskDao(val clock: Clock) {

    private enum class TaskProgress {
        available,
        inProgress,
        done,
        resubmitted
    }

    protected fun submit(
            coll: MongoCollection<Document>,
            data: Document
    ): Boolean {
        val now = clock.now()
        return coll.insert(docBuilder()
                .putAll(data)
                .putAll(
                        "_id" to UUID.randomUUID(),
                        "createdAt" to now,
                        "progress" to TaskProgress.available,
                        "lastUpdatedAt" to now
                )
                .build())
    }

    protected fun pickNextAvailable(
            coll: MongoCollection<Document>,
            query: Document,
            deadIfNoHeartBeatFor: Duration,
            sortBy: Document = emptyDoc()
    ): Optional<Document> {
        var options = FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
        if (sortBy.isNotEmpty()) {
            options = options.sort(sortBy)
        }

        val now = clock.now()
        val workDoc = coll.findOneAndUpdate(
                doc("\$or" to listOf(
                        docBuilder()
                                .putAll(query)
                                .put(
                                        "progress" to doc("\$in" to listOf(TaskProgress.available, TaskProgress.resubmitted))
                                )
                                .build(),
                        docBuilder()
                                .putAll(query)
                                .putAll(
                                        "progress" to TaskProgress.inProgress,
                                        "lastHeartBeatAt" to doc("\$lte" to now.minus(deadIfNoHeartBeatFor))
                                )
                                .build()
                )),
                doc("\$set" to doc(
                        "workerHost" to longLocalHostName(),
                        "progress" to TaskProgress.inProgress,
                        "lastHeartBeatAt" to now,
                        "lastUpdatedAt" to now
                )),
                options
        )

        return Optional.ofNullable(workDoc)
    }

    protected fun updateHeartBeat(
            coll: MongoCollection<Document>,
            taskId: UUID
    ): Boolean {
        val now = clock.now()
        val updateResult = coll.updateOne(
                doc(
                        "_id" to taskId,
                        "progress" to TaskProgress.inProgress
                ),
                doc("\$set" to doc(
                        "workerHost" to longLocalHostName(),
                        "lastHeartBeatAt" to now,
                        "lastUpdatedAt" to now
                ))

        )

        return updateResult.modifiedCount == 1L
    }

    protected fun markAsDone(
            coll: MongoCollection<Document>,
            taskId: UUID,
            data: Document = emptyDoc()
    ): Boolean {
        val now = clock.now()
        val updateResult = coll.updateOne(
                doc(
                        "_id" to taskId,
                        "progress" to TaskProgress.inProgress
                ),
                doc("\$set" to docBuilder()
                        .putAll(data)
                        .putAll(
                                "progress" to TaskProgress.done,
                                "workerHost" to longLocalHostName(),
                                "lastHeartBeatAt" to now,
                                "lastUpdatedAt" to now
                        )
                        .build()
                )
        )

        return updateResult.modifiedCount == 1L
    }

    protected fun markForRetry(
            coll: MongoCollection<Document>,
            taskId: UUID,
            data: Document = emptyDoc()
    ): Boolean {
        val now = clock.now()
        val updateResult = coll.updateOne(
                doc(
                        "_id" to taskId,
                        "progress" to TaskProgress.inProgress
                ),
                doc("\$set" to docBuilder()
                        .putAll(data)
                        .putAll(
                                "progress" to TaskProgress.resubmitted,
                                "workerHost" to longLocalHostName(),
                                "lastHeartBeatAt" to now,
                                "lastUpdatedAt" to now
                        )
                        .build()
                )
        )

        return updateResult.modifiedCount == 1L
    }

    private val DUPLICATE_CODE = 11000

    private fun <T> MongoCollection<T>.insert(document: T): Boolean {
        try {
            this.insertOne(document)
            return true
        } catch (e: MongoWriteException) {
            if (e.error.code == DUPLICATE_CODE) {
                return false
            } else {
                throw e
            }
        }
    }
}