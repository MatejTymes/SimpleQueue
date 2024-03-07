package mtymes.tasks.common.check

import org.bson.Document
import java.time.Duration

object ValidityChecks {

    @Throws(
        IllegalArgumentException::class
    )
    fun expectAtLeastOne(fieldPath: String, value: Int) {
        if (value < 1) {
            throw IllegalArgumentException(
                "'${fieldPath}' MUST BE AT LEAST 1 but was ${value} instead"
            )
        }
    }

    @Throws(
        IllegalArgumentException::class
    )
    fun expectPositiveDuration(fieldPath: String, value: Duration) {
        if (value.isNegative || value.isZero) {
            throw IllegalArgumentException(
                "'${fieldPath}' MUST BE A POSITIVE DURATION but was ${value} instead"
            )
        }
    }

    @Throws(
        IllegalArgumentException::class
    )
    fun expectNullOrPositiveDuration(fieldPath: String, value: Duration?) {
        if (value != null && (value.isNegative || value.isZero)) {
            throw IllegalArgumentException(
                "'${fieldPath}' MUST BE NULL OR POSITIVE DURATION but was ${value} instead"
            )
        }
    }

    @Throws(
        IllegalArgumentException::class
    )
    fun expectNonNegativeDuration(fieldPath: String, value: Duration) {
        if (value.isNegative) {
            throw IllegalArgumentException(
                "'${fieldPath}' MUST BE ZERO OR A POSITIVE DURATION but was ${value} instead"
            )
        }
    }

    @Throws(
        IllegalArgumentException::class
    )
    fun expectNullOrNonNegativeDuration(fieldPath: String, value: Duration?) {
        if (value != null && value.isNegative) {
            throw IllegalArgumentException(
                "'${fieldPath}' MUST BE NULL, ZERO OR A POSITIVE DURATION but was ${value} instead"
            )
        }
    }

    @Throws(
        IllegalArgumentException::class
    )
    fun expectNonEmptyDocument(fieldPath: String, value: Document) {
        if (value.isEmpty()) {
            throw IllegalArgumentException(
                "'${fieldPath}' CAN NOT BE EMPTY but it is"
            )
        }
    }

    @Throws(
        IllegalArgumentException::class
    )
    fun expectAtLeastOneItem(fieldPath: String, value: List<*>) {
        if (value.isEmpty()) {
            throw IllegalArgumentException(
                "'${fieldPath}' MUST HAVE AT LEAST 1 item, but was empty"
            )
        }
    }
}