package mtymes.tasks.test.task

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.mongo.DocBuilder.Companion.emptyDoc
import org.bson.Document

object TaskViewer {

    fun displayTasksSummary(
        collection: MongoCollection<Document>,
        canDisplayField: (String) -> Boolean = { true }
    ) {
        val query = emptyDoc()

        collection
            .find(query)
            .forEach { taskDoc -> displayTaskDoc(taskDoc, canDisplayField) }
    }

    fun displayTinyTasksSummary(
        collection: MongoCollection<Document>,
        additionalFieldMatches: Set<String> = emptySet()
    ) {
        displayTasksSummary(collection) { field ->
            setOf(
                "_id",
                "data",
                "status",
                "executions",
                "executions.id",
                "executions.data",
                "executions.status",
                "executions.workerId"
            ).contains(field) || additionalFieldMatches.contains(field) || setOf(
                "data.",
                "executions.data."
            ).any { path -> field.startsWith(path) }
        }
    }

    private fun displayTaskDoc(
        taskDoc: Document,
        canDisplayField: (String) -> Boolean
    ) {
        println()
        println("Task = {")
        displayMap("  ", "", taskDoc, canDisplayField)
        println("}")
    }

    private fun displayMap(
        displayPrefix: String,
        fieldPrefix: String,
        entries: Map<String, Any>,
        canDisplayField: (String) -> Boolean
    ) {
        for (entry in entries) {
            val fieldName = entry.key
            val fieldPath = fieldPrefix + fieldName
            if (!canDisplayField.invoke(fieldPath)) {
                continue
            }


            val value = entry.value

            if (value is Document) {
                println("${displayPrefix}$fieldName = {")
                displayMap("  ${displayPrefix}", fieldPath + ".", value, canDisplayField)
                println("${displayPrefix}}")
            } else if (value is List<*>) {
                println("${displayPrefix}$fieldName = [")
                displayListItems("  ${displayPrefix}", fieldPath + ".", value, canDisplayField)
                println("${displayPrefix}]")
            } else {
                println("${displayPrefix}$fieldName = $value")
            }
        }
    }

    private fun displayListItems(
        displayPrefix: String,
        fieldPath: String,
        items: List<*>,
        canDisplayField: (String) -> Boolean
    ) {
        for (item in items) {
            if (item is Document) {
                println("${displayPrefix}{")
                displayMap("  ${displayPrefix}", fieldPath, item, canDisplayField)
                println("${displayPrefix}}")
            } else if (item is List<*>) {
                println("${displayPrefix}[")
                displayListItems("  ${displayPrefix}", fieldPath, item, canDisplayField)
                println("${displayPrefix}]")
            } else {
                println("${displayPrefix}${item}")
            }
        }
    }
}