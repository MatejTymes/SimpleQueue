package mtymes.task.v02.test.mongo

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder
import org.bson.Document


fun emptyLocalCollection(collectionName: String): MongoCollection<Document> {
    val mongo = LocalTestMongo()

    val db = mongo.database()
    val coll = db.getCollection(collectionName)
    coll.deleteMany(DocBuilder.emptyDoc())
    return coll
}
