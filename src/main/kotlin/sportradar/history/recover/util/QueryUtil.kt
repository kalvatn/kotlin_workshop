package sportradar.history.recover.util

import java.sql.PreparedStatement
import java.util.*

object QueryUtil {

    fun createPlaceHolders(coll: Collection<*>): String {
        return Collections.nCopies(coll.size, "?").joinToString(",")
    }

    fun createInsertQuery(tablename: String, fields: Collection<String>): String {
        return String.format(
            "insert ignore into %s (%s) VALUES (%s)",
            tablename, fields.joinToString(","), createPlaceHolders(fields)
        )
    }

    fun psToString(ps: PreparedStatement): String {
        val s = ps.toString()
        return s.substring(s.indexOf(':'))
    }

    fun psBatchUpdateToString(batchResult: IntArray): String {
        val total = batchResult.size
        val countUpdated = Arrays.stream(batchResult).boxed().filter { i -> i > 0 }.count()
        return String.format("%d inserted of %d total", countUpdated, total)
    }
}
