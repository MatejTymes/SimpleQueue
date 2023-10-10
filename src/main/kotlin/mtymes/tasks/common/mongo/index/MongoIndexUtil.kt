package mtymes.tasks.common.mongo.index

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.mongo.DocumentExt.getDocument
import mtymes.tasks.common.mongo.index.IndexOperation.Companion.addIndexOp
import mtymes.tasks.common.mongo.index.IndexOperation.Companion.keepIndexOp
import mtymes.tasks.common.mongo.index.IndexOperation.Companion.removeIndexOp
import org.bson.Document
import java.util.function.BiFunction

object MongoIndexUtil {

    fun listAllIndexes(
        collection: MongoCollection<Document>,
        ignore_IDIndex: Boolean
    ): List<IndexDefinition> {

        val indexDefinitions = findIndexes(collection).map { entry ->
            val keysDoc = entry.key
            val optionsDoc = entry.value

            val keys = keysDoc.entries.map { keyEntry ->
                IndexKey(
                    keyEntry.key as String, (keyEntry.value as Number).toInt()
                )
            }

            IndexDefinition(keys,
                background = extractNullableValue(optionsDoc, "background", Document::getBoolean),
                unique = extractNullableValue(optionsDoc, "unique", Document::getBoolean),
                sparse = extractNullableValue(optionsDoc, "sparse", Document::getBoolean),
                expireAfterSeconds = extractNullableValue(
                    optionsDoc, "expireAfterSeconds", { doc, fieldName -> (doc.get(fieldName) as Number).toLong() }
                ),
                partialFilterExpression = extractNullableValue(
                    optionsDoc, "partialFilterExpression", { doc, fieldName -> doc.getDocument(fieldName) }
                )
            )
        }.filter { indexDefinition ->
            !ignore_IDIndex || !indexDefinition.is_IDIndex()
        }

        return indexDefinitions
    }

    fun listOperationsToGetToExpectedState(
        collection: MongoCollection<Document>,
        expectedIndexes: List<IndexDefinition>
    ): List<IndexOperation> {
        val currentIndexes = listAllIndexes(collection, true)

        val indexesToAdd = mutableListOf<IndexDefinition>()
        val indexesToRemove = mutableListOf<IndexDefinition>()
        val indexesToKeep = mutableListOf<IndexDefinition>()

        for (expectedIndex in expectedIndexes) {
            if (!expectedIndex.is_IDIndex()) {
                if (currentIndexes.contains(expectedIndex)) {
                    indexesToKeep.add(expectedIndex)
                } else {
                    indexesToAdd.add(expectedIndex)
                }
            }
        }

        for (currentIndex in currentIndexes) {
            if (!expectedIndexes.contains(currentIndex)) {
                indexesToRemove.add(currentIndex)
            }
        }

        val allOperations = mutableListOf<IndexOperation>()
        indexesToKeep.forEach {
            allOperations.add(keepIndexOp(it))
        }

        var conflictingOperations = mutableListOf<IndexOperation>()
        while (!indexesToAdd.isEmpty()) {
            val indexToAdd = indexesToAdd.removeAt(0)

            var isConflicting = false

            for (conflictingOp in conflictingOperations) {
                if (conflictingOp.action == IndexAction.REMOVE && !indexToAdd.canCoExistWith(conflictingOp.index)) {
                    isConflicting = true
                    break
                }
            }

            for (i in indexesToRemove.size - 1 downTo 0) {
                val indexToRemove = indexesToRemove[i]
                if (!indexToAdd.canCoExistWith(indexToRemove)) {
                    conflictingOperations.add(removeIndexOp(indexesToRemove.removeAt(i)))
                    isConflicting = true
                }
            }

            if (isConflicting) {
                conflictingOperations.add(addIndexOp(indexToAdd))
            } else {
                allOperations.add(addIndexOp(indexToAdd))
            }
        }
        allOperations.addAll(conflictingOperations)

        indexesToRemove.forEach {
            allOperations.add(removeIndexOp(it))
        }

        return allOperations
    }

    fun applyIndexOperations(
        collection: MongoCollection<Document>,
        operations: List<IndexOperation>
    ) {
        TODO("implement")
    }

    private fun findIndexes(
        collection: MongoCollection<Document>
    ): Map<Document, Document> {
        val indexes = LinkedHashMap<Document, Document>()

        for (indexInfo in collection.listIndexes()) {
            val keys = indexInfo.get("key", Document::class.java)

            keys.remove("key")
            keys.remove("v")
            keys.remove("ns")

            indexes.put(keys, indexInfo)
        }

        return indexes
    }

    private fun <T> extractNullableValue(
        optionsDoc: Document,
        fieldName: String,
        valueGetter: BiFunction<Document, String, T?>
    ): T? {
        if (optionsDoc.contains(fieldName)) {
            return valueGetter.apply(optionsDoc, fieldName)
        }
        return null
    }
}