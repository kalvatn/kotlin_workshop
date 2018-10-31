package sportradar.history.recover.model

import java.sql.Timestamp

data class ScoutEvent(
    val id: String?,
    val matchid: Long,
    val eventtype: Int,
    val time: Int,
    val param1: String?,
    val param2: String?,
    val param3: String?,
    val param4: String?,
    val text: String?,
    val eventdate: Timestamp?,
    val injurytime: Int
)
