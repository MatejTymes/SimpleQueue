package mtymes.tasks.test.mongo

import com.mongodb.client.MongoCollection
import org.bson.Document


fun localCollection(collectionName: String): MongoCollection<Document> {
    val mongo = LocalTestMongo()

    val db = mongo.database()
    val coll = db.getCollection(collectionName)
    return coll
}

fun emptyLocalCollection(collectionName: String): MongoCollection<Document> {
    val mongo = LocalTestMongo()

    val db = mongo.database()
    db.getCollection(collectionName).drop()
    return db.getCollection(collectionName)
}
