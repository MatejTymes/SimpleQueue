package mtymes.tasks.beta.common.domain


sealed interface AValue<T> {
    fun isDefined(): Boolean
    fun isUndefined(): Boolean = !isDefined()
    fun isEqualTo(otherValue: T?): Boolean
    fun definedValue(): T?
    fun definedValueOrDefault(default: T?): T?
}


object UndefinedValue : AValue<Any?> {
    override fun isDefined(): Boolean = false
    override fun isEqualTo(otherValue: Any?): Boolean = false
    override fun definedValue(): Any? {
        throw IllegalStateException("This value is UNDEFINED")
    }
    override fun definedValueOrDefault(default: Any?): Any? = default
}


data class DefinedValue<T>(
    val value: T?
) : AValue<T> {
    override fun isDefined(): Boolean = true
    override fun isEqualTo(otherValue: T?): Boolean {
        return value == otherValue
    }
    override fun definedValue(): T? {
        return value
    }
    override fun definedValueOrDefault(default: T?): T? {
        return value
    }
}
