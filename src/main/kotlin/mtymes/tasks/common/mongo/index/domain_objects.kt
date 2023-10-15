package mtymes.tasks.common.mongo.index

import com.mongodb.client.model.IndexOptions
import mtymes.tasks.common.mongo.builder.BaseDocBuilder.docBuilder
import org.bson.Document
import java.util.*
import java.util.concurrent.TimeUnit

enum class IndexOrder {

    ASCENDING,
    DESCENDING;

    companion object {
        fun fromInt(value: Int): IndexOrder {
            return when (value) {
                1 -> ASCENDING
                -1 -> DESCENDING
                else -> throw IllegalArgumentException("Value can be only 1 or -1, but was $value instead")
            }
        }
    }
}

data class IndexKey(
    val name: String,
    val order: IndexOrder
) {
    constructor(name: String, order: Int) : this(name, IndexOrder.fromInt(order))
}

data class IndexDefinition(
    val keys: List<IndexKey>,

    val background: Boolean? = null,
    val unique: Boolean? = null,
    val sparse: Boolean? = null,
    val expireAfterSeconds: Long? = null,
    val partialFilterExpression: Document? = null
) {

    fun is_IDIndex(): Boolean {
        return keys.size == 1 && "_id" == keys[0].name
    }

    fun canCoExistWith(index: IndexDefinition): Boolean {
        return Objects.equals(this.fields(), index.fields())
    }

    fun keysDocument(): Document {
        val docBuilder = docBuilder()
        for (key in keys) {
            docBuilder.put(key.name, key.order)
        }
        return docBuilder.build()
    }

    fun indexOptions(): IndexOptions {
        var indexOptions = IndexOptions()
        if (background != null) {
            indexOptions = indexOptions.background(background)
        }
        if (unique != null) {
            indexOptions = indexOptions.unique(unique)
        }
        if (sparse != null) {
            indexOptions = indexOptions.sparse(sparse)
        }
        if (expireAfterSeconds != null) {
            indexOptions = indexOptions.expireAfter(expireAfterSeconds, TimeUnit.SECONDS)
        }
        if (partialFilterExpression != null) {
            indexOptions = indexOptions.partialFilterExpression(partialFilterExpression)
        }
        return indexOptions
    }

    // todo: mtymes - can be turned into lazy value (while not being included in the equals and hashCode)?
    private fun fields(): Set<String> {
        return keys.map { it.name }.toSet()
    }
}

enum class IndexAction(
    val isDestructive: Boolean
) {

    ADD(false),
    REMOVE(true),
    KEEP(false)
}

data class IndexOperation(
    val index: IndexDefinition,
    val action: IndexAction
) {
    companion object {
        fun addIndexOp(index: IndexDefinition) = IndexOperation(index, IndexAction.ADD)
        fun removeIndexOp(index: IndexDefinition) = IndexOperation(index, IndexAction.REMOVE)
        fun keepIndexOp(index: IndexDefinition) = IndexOperation(index, IndexAction.KEEP)
    }
}