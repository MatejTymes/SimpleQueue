package mtymes.task.v02.test.mongo

import com.mongodb.client.MongoDatabase

interface TestMongo {

    fun database(): MongoDatabase
}