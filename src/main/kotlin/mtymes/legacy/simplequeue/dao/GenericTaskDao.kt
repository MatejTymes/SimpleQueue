package mtymes.legacy.simplequeue.dao

import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.ReturnDocument
import mtymes.legacy.common.mongo.DocBuilder.Companion.doc
import mtymes.legacy.common.mongo.DocBuilder.Companion.docBuilder
import mtymes.legacy.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.legacy.common.network.HostUtil.longLocalHostName
import mtymes.legacy.common.time.Clock
import org.bson.Document
import java.time.Duration
import java.util.*

// todo: add client
// todo: store custom data into field "data" - generate proper update on status change
// todo: turn into features: heartBeat, retry count
abstract class GenericTaskDao(val clock: Clock = Clock()) {

    private enum class ProgressState {
        available,
        inProgress,
        done,
        resubmitted
    }

    protected fun createPickingIndex(
            queryFields: Document,
            sortBy: Document,
            coll: MongoCollection<Document>,
            isPartialIndexSupported: Boolean = true
    ) {
        val indexFields = docBuilder()
                .putAll(sortBy)
                .putAll(
                        "progress" to 1,
                        "lastHeartBeatAt" to 1 // todo: only if heart beat enabled
                )
                .putAll(queryFields)
                .build()

        val indexOptions = IndexOptions().background(true)
        if (isPartialIndexSupported) {
            indexOptions.partialFilterExpression(doc(
                    "progress" to (doc("\$in", listOf(
                            ProgressState.available.name,
                            ProgressState.inProgress.name,
                            ProgressState.resubmitted.name
                    )))
            ))
        }

        coll.createIndex(indexFields, indexOptions)
    }

    protected fun submit(
        coll: MongoCollection<Document>,
        id: UUID,
        data: Document
    ): Boolean {
        val now = clock.now()
        return coll.insert(docBuilder()
            .putAll(data)
            .putAll(
                "_id" to id,
                "createdAt" to now,
                "progress" to ProgressState.available,
                "lastUpdatedAt" to now
            )
            .build())
    }

    protected fun submit(
        coll: MongoCollection<Document>,
        data: Document
    ): Boolean {
        return submit(coll, UUID.randomUUID()!!, data)
    }

    protected fun pickNextAvailable(
            coll: MongoCollection<Document>,
            query: Document,
            deadIfNoHeartBeatFor: Duration,
            data: Document = emptyDoc(),
            sortBy: Document = emptyDoc()
    ): Optional<Document> {
        var options = FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
        if (sortBy.isNotEmpty()) {
            options = options.sort(sortBy)
        }

        val now = clock.now()
        val workDoc = coll.findOneAndUpdate(
                doc("\$and" to listOf(
                        query,
                        doc("\$or" to listOf(
                                doc("progress" to doc("\$in" to listOf(
                                    ProgressState.available,
                                    ProgressState.resubmitted
                                ))),
                                doc(
                                        "progress" to ProgressState.inProgress,
                                        "lastHeartBeatAt" to doc("\$lte" to now.minus(deadIfNoHeartBeatFor))
                                )
                        ))
                )),
                doc("\$set" to docBuilder()
                        .putAll(data)
                        .putAll(
                                "workerHost" to longLocalHostName(),
                                "progress" to ProgressState.inProgress,
                                "lastHeartBeatAt" to now,
                                "lastUpdatedAt" to now
                        )
                        .build()),
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
                        "progress" to ProgressState.inProgress
                ),
                doc("\$set" to doc(
                        "workerHost" to longLocalHostName(),
                        "lastHeartBeatAt" to now,
                        "lastUpdatedAt" to now
                ))

        )

        return updateResult.modifiedCount == 1L
    }

    // todo: add - updateProgressData

    protected fun markAsDone(
            coll: MongoCollection<Document>,
            taskId: UUID,
            data: Document = emptyDoc()
    ): Boolean {
        return setStatus(coll, taskId, data, ProgressState.done)
    }

    protected fun markForRetry(
            coll: MongoCollection<Document>,
            taskId: UUID,
            data: Document = emptyDoc()
    ): Boolean {
        return setStatus(coll, taskId, data, ProgressState.resubmitted)
    }

    private fun setStatus(coll: MongoCollection<Document>, taskId: UUID, data: Document, newStatus: ProgressState): Boolean {
        val now = clock.now()
        val updateResult = coll.updateOne(
                doc(
                        "_id" to taskId,
                        "progress" to ProgressState.inProgress
                ),
                doc("\$set" to docBuilder()
                        .putAll(data)
                        .putAll(
                                "progress" to newStatus,
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