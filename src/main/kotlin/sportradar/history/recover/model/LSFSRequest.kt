package sportradar.history.recover.model

import java.sql.Timestamp

data class LSFSRequest(
    val requestedat: Timestamp,
    val userid: Int,
    val from: String = "",
    val to: String = "",
    val target_type: Int,
    val target_details: String,
    val recipientid: Int,

    val matchids: String
)

