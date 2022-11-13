package mtymes.tasks.test.mongo

import org.bson.Document
import org.hamcrest.Description
import org.hamcrest.TypeSafeDiagnosingMatcher
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean


sealed interface AValue<T> {
    fun isDefined(): Boolean
    fun isUndefined(): Boolean = !isDefined()
    fun isEqualTo(otherValue: T?): Boolean
    fun value(): T?
    fun valueOrDefault(default: T?): T?
}

object UndefinedValue : AValue<Any?> {
    override fun isDefined(): Boolean = false
    override fun value(): Any? {
        throw IllegalStateException("This value is UNDEFINED")
    }

    override fun isEqualTo(otherValue: Any?): Boolean = false
    override fun valueOrDefault(default: Any?): Any? = default
}

data class DefinedValue<T>(
    val value: T?
) : AValue<T> {
    override fun isDefined(): Boolean = true
    override fun value(): T? {
        return value
    }

    override fun isEqualTo(otherValue: T?): Boolean {
        return value == otherValue
    }

    override fun valueOrDefault(default: T?): T? {
        return value
    }
}

interface IgnoreTheseMismatches {
    fun ignoreThisMismatch(fieldPath: String, actualValue: AValue<Any?>, expectedValue: AValue<Any?>): Boolean
}

class IgnoreTheseFields(
    val fieldsToIgnore: Collection<String>
) : IgnoreTheseMismatches {
    override fun ignoreThisMismatch(fieldPath: String, actualValue: AValue<Any?>, expectedValue: AValue<Any?>): Boolean {
        return fieldsToIgnore.contains(fieldPath)
    }
}

class DocumentMatcher(
    private val expectedDocument: Document,
    private val ignoreMismatches: IgnoreTheseMismatches?
) : TypeSafeDiagnosingMatcher<Document>() {

    override fun describeTo(description: Description?) {
        description?.appendText(expectedDocument.toString())
    }

    override fun matchesSafely(actualDocument: Document, mismatchDescription: Description): Boolean {
        mismatchDescription.appendText(actualDocument.toString())

        val matches = AtomicBoolean(true)
        val keyPrefix = ""

        findMismatches(expectedDocument, actualDocument, keyPrefix, matches, mismatchDescription)

        return matches.get()
    }

    private fun findMismatches(
        expectedDocument: Document,
        actualDocument: Document,
        keyPrefix: String,
        matches: AtomicBoolean,
        mismatchDescription: Description
    ) {
        val keys: MutableSet<String> = TreeSet()
        keys.addAll(expectedDocument.keys)
        keys.addAll(actualDocument.keys)

        for (key in keys) {
            val expectedValue: AValue<Any?> =
                if (expectedDocument.containsKey(key)) DefinedValue(expectedDocument.get(key)) else UndefinedValue
            val actualValue: AValue<Any?> =
                if (actualDocument.containsKey(key)) DefinedValue(actualDocument.get(key)) else UndefinedValue

            val keyPath = keyPrefix + key

            compare(expectedValue, actualValue, keyPath, matches, mismatchDescription)
        }
    }


    private fun findMismatches(
        expectedIterator: Iterator<Any?>,
        actualIterator: Iterator<Any?>,
        keyPrefix: String,
        matches: AtomicBoolean,
        mismatchDescription: Description
    ) {
        var itemIndex = 0
        while (expectedIterator.hasNext() || actualIterator.hasNext()) {
            val expectedValue: AValue<Any?> = if (expectedIterator.hasNext()) DefinedValue(expectedIterator.next())
                else UndefinedValue
            val actualValue: AValue<Any?> = if (actualIterator.hasNext()) DefinedValue(actualIterator.next())
                else UndefinedValue

            val keyPath = keyPrefix + "[${itemIndex}]"

            compare(expectedValue, actualValue, keyPath, matches, mismatchDescription)

            itemIndex++
        }
    }

    private fun compare(
        expectedValue: AValue<Any?>,
        actualValue: AValue<Any?>,
        keyPath: String,
        matches: AtomicBoolean,
        mismatchDescription: Description
    ) {
        if (expectedValue != actualValue) {
            if (ignoreMismatches != null && ignoreMismatches.ignoreThisMismatch(keyPath, actualValue, expectedValue)) {
                return
            }

            val expected = expectedValue.valueOrDefault(null)
            val actual = actualValue.valueOrDefault(null)
            if (expected is Document && actual is Document) {
                findMismatches(
                    expectedDocument = expected,
                    actualDocument = actual,
                    keyPrefix = keyPath + ".",
                    matches = matches,
                    mismatchDescription = mismatchDescription
                )
            } else if (expected is Iterable<*> && actual is Iterable<*>) {
                findMismatches(
                    expectedIterator = expected.iterator(),
                    actualIterator = actual.iterator(),
                    keyPrefix = keyPath + ".",
                    matches = matches,
                    mismatchDescription = mismatchDescription
                )
            } else {
                if (matches.get()) {
                    mismatchDescription.appendText("\nhad these mismatches:")
                    matches.set(false)
                }

                val hasTheSameClass = expected != null
                        && actual != null
                        && expected::class == actual::class

                mismatchDescription.appendText(
                    "\n- ${keyPath}:" +
                            (if (expectedValue.isDefined())
                                "\n  - expected = " + expected + (if (!hasTheSameClass && expected != null) " [${expected::class.simpleName}]" else "")
                            else
                                "\n  - expected value is not present") +
                            (if (actualValue.isDefined())
                                "\n  - actual = " + actual + (if (!hasTheSameClass && actual != null) " [${actual::class.simpleName}]" else "")
                            else
                                "\n  - actual value is not present")
                )
            }
        }
    }
}
