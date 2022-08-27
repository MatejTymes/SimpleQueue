package mtymes.tasks.common.mongo

import org.bson.Document

object DocumentExt {

    fun Document?.isDefined(): Boolean {
        return !this.isNullOrEmpty()
    }

    fun Document?.areDefined(): Boolean {
        return !this.isNullOrEmpty()
    }
}