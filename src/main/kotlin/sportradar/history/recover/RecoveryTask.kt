package sportradar.history.recover

import com.google.common.base.MoreObjects

import java.time.Instant

data class RecoveryTask(
    val tournamentId: Int,
    val fromDate: Instant,
    val toDate: Instant
) {


    override fun toString(): String {
        return MoreObjects.toStringHelper(this)
            .add("tournamentId", tournamentId)
            .add("fromDate", fromDate)
            .add("toDate", toDate)
            .toString()
    }
}
