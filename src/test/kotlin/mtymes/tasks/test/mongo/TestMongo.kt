package mtymes.tasks.test.mongo

import com.mongodb.client.MongoDatabase

interface TestMongo {

    fun database(): MongoDatabase
}