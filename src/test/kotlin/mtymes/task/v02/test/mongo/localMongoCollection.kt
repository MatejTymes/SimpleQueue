package mtymes.task.v02.test.mongo

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder.Companion.emptyDoc
import org.bson.Document


fun localCollection(collectionName: String): MongoCollection<Document> {
    val mongo = LocalTestMongo()

    val db = mongo.database()
    val coll = db.getCollection(collectionName)
    return coll
}

fun emptyLocalCollection(collectionName: String): MongoCollection<Document> {
    val coll = localCollection(collectionName)
    coll.deleteMany(emptyDoc())
    return coll
}
