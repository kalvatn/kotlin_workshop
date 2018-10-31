package sportradar.history.recover.util

import sportradar.history.recover.model.LSFSRequest
import java.sql.Timestamp
import java.time.Instant

object RequestUtil {

    fun createMatchRequest(filesenderId: Int, matchId: Long): LSFSRequest {
//        return LSFSRequest.newLSFSRequest()
//            .userid(2586)
//            .recipientid(filesenderId)
//            .requestedat(Timestamp.from(Instant.now()))
//            .from("")
//            .to("")
//            .target_type(2)
//            .target_details(String.format("update-%d", matchId))
//            .matchids(matchId.toString())
//            .build()
        return LSFSRequest(
            userid = 2586,
            recipientid = filesenderId,
            requestedat = Timestamp.from(Instant.now()),
            target_type = 2,
            target_details = "update-$matchId",
            matchids = matchId.toString()
        )
    }
}
