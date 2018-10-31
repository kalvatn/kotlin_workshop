package sportradar.history.recover.model

import java.sql.Timestamp

data class Event(
    val id: Long,
    val sourceId: Int,
    val matchId: Long,
    val eventType: Int,
    val time: Int,
    val matchTime: Int,
    val param1: String?,
    val param2: String?,
    val param3: String?,
    val param4: String?,
    val param5: String?,
    val text: String?,
    val eventDate: Timestamp?,
    val updateDate: Timestamp?,
    val isDisabled: Boolean,
    val injuryTime: Int,
    val injuryMatchTime: Int,
    val scoutEventId: Long
)
