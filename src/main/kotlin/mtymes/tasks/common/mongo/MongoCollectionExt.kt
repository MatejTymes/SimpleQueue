package mtymes.tasks.common.mongo

import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
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

    // todo: mtymes - should we fail if has more that one ???
    fun <T> MongoCollection<T>.findOne(filter: Bson): T? = find(filter).first()

}