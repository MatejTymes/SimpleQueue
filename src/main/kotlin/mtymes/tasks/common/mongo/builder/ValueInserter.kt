package mtymes.tasks.common.mongo.builder

import org.bson.Document


interface ValueInserter {
    fun insertValue(value: Any?)
}


data class MapValueInserter(
    private val map: MutableMap<String, in Any?>,
    private var fieldName: String? = null
) : ValueInserter {

    override fun insertValue(value: Any?) {
        val fieldToUse = fieldName ?: throw IllegalStateException("fieldName must be defined first")
        map.put(fieldToUse, value)
    }

    fun changeFieldName(newFieldName: String) {
        fieldName = newFieldName
    }
}


data class DocumentInserter(
    private val document: Document,
    private var fieldName: String? = null
) : ValueInserter {

    override fun insertValue(value: Any?) {
        val fieldToUse = fieldName ?: throw IllegalStateException("fieldName must be defined first")
        document.append(fieldToUse, value)
    }

    fun changeFieldName(newFieldName: String) {
        fieldName = newFieldName
    }
}


data class ListValueInserter(
    private val list: MutableList<in Any?>
) : ValueInserter {

    override fun insertValue(value: Any?) {
        list.add(value)
    }
}
