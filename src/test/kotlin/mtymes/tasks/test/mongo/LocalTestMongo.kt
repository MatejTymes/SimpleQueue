package mtymes.tasks.test.mongo

import com.mongodb.MongoClient
import com.mongodb.client.MongoDatabase

class LocalTestMongo : TestMongo {

    val client = lazy {
        MongoClient("localhost", 27017)
    }

    override fun database(): MongoDatabase {
        return client.value.getDatabase("sampleDB")
    }
}