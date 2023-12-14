package mtymes.tasks.common.mongo.index

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.mongo.DocumentExt.getDocument
import mtymes.tasks.common.mongo.index.IndexOperation.Companion.addIndexOp
import mtymes.tasks.common.mongo.index.IndexOperation.Companion.keepIndexOp
import mtymes.tasks.common.mongo.index.IndexOperation.Companion.removeIndexOp
import org.bson.Document
import org.slf4j.LoggerFactory
import java.util.function.BiFunction

object MongoIndexUtil {

    private val LOG = LoggerFactory.getLogger(this.javaClass)

    fun printIndexDifferences(
        collectionName: String,
        envNameA: String,
        envNameB: String,
        indexesA: List<IndexDefinition>,
        indexesB: List<IndexDefinition>
    ) {
        val shared = linkedSetOf<IndexDefinition>()
        val onlyInA = linkedSetOf<IndexDefinition>()
        val onlyInB = linkedSetOf<IndexDefinition>()

        for (indexA in indexesA) {
            if (!indexesB.contains(indexA)) {
                onlyInA.add(indexA)
            } else {
                shared.add(indexA)
            }
        }
        for (indexB in indexesB) {
            if (!indexesA.contains(indexB)) {
                onlyInB.add(indexB)
            } else {
                // we should ignore this, right ???
                shared.add(indexB)
            }
        }

        println("${collectionName}:")
        if (shared.isNotEmpty()) {
            println("- shared")
            for (index in shared) {
                println("  - ${index.toShortString()}")
            }
        }
        if (onlyInA.isNotEmpty()) {
            println("- only in ${envNameA}")
            for (index in onlyInA) {
                println("  - ${index.toShortString()}")
            }
        }
        if (onlyInB.isNotEmpty()) {
            println("- only in ${envNameB}")
            for (index in onlyInB) {
                println("  - ${index.toShortString()}")
            }
        }
        println()
    }

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

        return listOperationsToGetToExpectedState(expectedIndexes, currentIndexes)
    }

    fun listOperationsToGetToExpectedState(
        expectedIndexes: List<IndexDefinition>,
        currentIndexes: List<IndexDefinition>
    ): MutableList<IndexOperation> {
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
        val collectionName = collection.namespace.collectionName

        LOG.info("Applying index operations to collection: $collectionName")
        for (operation in operations) {
            val index = operation.index
            val actionType = operation.action
            when (actionType) {
                IndexAction.ADD -> {
                    LOG.info("- creating: ${collectionName}.${index}")
                    collection.createIndex(
                        index.keysDocument(),
                        index.indexOptions()
                    )
                }

                IndexAction.REMOVE -> {
                    LOG.info("- removing: ${collectionName}.${index}")
                    collection.dropIndex(
                        index.keysDocument()
                    )
                }

                IndexAction.KEEP -> {
                    LOG.info("- keeping: ${collectionName}.${index}")
                }
            }
        }
    }

    fun ensureOnlyDefinedIndexesArePresent(
        collection: MongoCollection<Document>,
        expectedIndexes: List<IndexDefinition>
    ) {
        val operations = listOperationsToGetToExpectedState(collection, expectedIndexes)
        applyIndexOperations(collection, operations)
    }

    fun addNonConflictingExpectedIndexes(
        collection: MongoCollection<Document>,
        expectedIndexes: List<IndexDefinition>
    ) {
        val allOperations = listOperationsToGetToExpectedState(collection, expectedIndexes)

        val nonDestructiveOperations = mutableListOf<IndexOperation>()
        for (operation in allOperations) {
            if (operation.action.isDestructive) {
                break
            }
            nonDestructiveOperations.add(operation)
        }

        applyIndexOperations(collection, nonDestructiveOperations)
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