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

    // todo: mtymes - should we fail if has more than one ???
    fun <T> MongoCollection<T>.findOne(filter: Bson): T? = find(filter).first()

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