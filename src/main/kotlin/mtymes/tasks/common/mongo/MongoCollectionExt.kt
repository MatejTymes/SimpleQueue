package mtymes.tasks.common.mongo

import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.UpdateResult
import org.bson.conversions.Bson

object MongoCollectionExt {

    private val DUPLICATE_CODE = 11000

    fun <T> MongoCollection<T>.insert(document: T): Boolean {
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

    fun <T> MongoCollection<T>.findTheOnlyOne(query: Bson): T? {
        val iterator = find(query).limit(2).iterator()

        val result: T? = if (iterator.hasNext()) iterator.next() else null

        if (iterator.hasNext()) {
            throw IllegalStateException("Found more than one result for query: ${query}")
        }

        return result
    }

    fun <T> MongoCollection<T>.findFirst(query: Bson): T? = find(query).first()

    fun <T> MongoCollection<T>.upsert(query: Bson, update: Bson): UpdateResult {
        try {
            return this.updateOne(query, update, UpdateOptions().upsert(true))
        } catch (e: MongoWriteException) {
            // this can happen if two threads try to insert new document with the same id at the same time
            if (e.error.code == DUPLICATE_CODE) {
                // retry can easily fix this
                return this.updateOne(query, update, UpdateOptions().upsert(true))
            } else {
                throw e
            }
        }
    }
}