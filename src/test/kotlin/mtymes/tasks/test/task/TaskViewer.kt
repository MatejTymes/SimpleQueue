package mtymes.tasks.test.task

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.mongo.DocumentUtil.emptyDoc
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DATA
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.PREVIOUS_EXECUTIONS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.TASK_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.WORKER_ID
import org.bson.Document
import java.util.concurrent.atomic.AtomicInteger

object TaskViewer {

    fun displayTasksSummary(
        collection: MongoCollection<Document>,
        query: Document = emptyDoc(),
        canDisplayField: (String) -> Boolean = { true }
    ) {
        val totalCount = AtomicInteger(0)
        collection
            .find(query)
            .forEach { taskDoc ->
                totalCount.incrementAndGet()
                displayTaskDoc(taskDoc, canDisplayField)
            }
        println("\ntotal task count = ${totalCount.get()}\n")
    }

    fun displayTinyTasksSummary(
        collection: MongoCollection<Document>,
        additionalFieldMatches: Set<String> = emptySet()
    ) {
        displayTasksSummary(collection) { field ->
            setOf(
                TASK_ID,
                DATA,
                STATUS,
                PREVIOUS_EXECUTIONS,
                PREVIOUS_EXECUTIONS + "." + EXECUTION_ID,
                PREVIOUS_EXECUTIONS + "." + DATA,
                PREVIOUS_EXECUTIONS + "." + STATUS,
                PREVIOUS_EXECUTIONS + "." + WORKER_ID,
                LAST_EXECUTION,
                LAST_EXECUTION + "." + EXECUTION_ID,
                LAST_EXECUTION + "." + DATA,
                LAST_EXECUTION + "." + STATUS,
                LAST_EXECUTION + "." + WORKER_ID,
            ).contains(field) || additionalFieldMatches.contains(field) || setOf(
                DATA + ".",
                PREVIOUS_EXECUTIONS + "." + DATA + ".",
                LAST_EXECUTION + "." + DATA + "."
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