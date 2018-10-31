package sportradar.history.recover

import com.betradar.database.HistoryDB
import com.betradar.database.SystemDB
import com.betradar.util.LogUtil
import org.apache.log4j.Logger
import sportradar.history.recover.util.RequestUtil
import sportradar.kotlin.workshop.examples._14LambdaWithReceiver.use
import java.sql.SQLException
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId


/**
 * converted from https://gitlab.sportradar.ag/livescore/common/tree/master/src/main/java/com/oddsarbitrage/livescore/history/recover
 * using mostly intellij's convert java file to kotlin ctrl + alt + shift + k
 * @author j.kalvatn
 */
class HistoryRecoveryTool {
    private val loader: Loader = Loader()
    private val saver: Saver = Saver()
    private val log = Logger.getLogger(HistoryRecoveryTool::class.java)


    fun runHBLRecovery() {
        // 3299, 212, 3468, 3469, 688, 95, 4473,16956
        val allStarGame = RecoveryTask(
            tournamentId = 3299,
            fromDate = LocalDateTime.of(2017, 2, 3, 0, 0).atZone(ZoneId.systemDefault()).toInstant(),
            toDate = Instant.now()
        )
//        val dhbPokal = RecoveryTask(
//            tournamentId = 212,
//            fromDate = LocalDateTime.of(2017, 8, 18, 0, 0).atZone(ZoneId.systemDefault()).toInstant(),
//            toDate = Instant.now()
//        )


        runRecoveryTasks(listOf(allStarGame))
    }

    private fun runRecoveryTasks(tasks: List<RecoveryTask>) {
        try {
            HistoryDB.instance().borrowConnection().use { historyConn ->
                SystemDB.instance().borrowConnection().use { writeConn ->
                    for (task in tasks) {
                        log.info(task)
                        val tournamentId = task.tournamentId
                        val fromDate = task.fromDate
                        val toDate = task.toDate

                        val matches = loader.loadMatches(historyConn, fromDate, toDate, tournamentId)
                        val matchIds = matches.keys

                        val matchExtras = loader.loadMatchExtra(historyConn, matchIds)
                        val scoutEvents = loader.loadScoutEvents(historyConn, matchIds)
                        val events = loader.loadEvents(historyConn, matchIds)

                        val eventIds = events.keys
                        val flags = loader.loadEventFlags(historyConn, eventIds)
                        val players = loader.loadEventPlayers(historyConn, eventIds)
                        val positions = loader.loadEventPositions(historyConn, eventIds)

                        with(log) {
                            info(String.format("livescore_match               : %d", matches.size))
                            info(String.format("livescore_match_extra         : %d", matchExtras.size))
                            info(String.format("livescore_scoutevent          : %d", scoutEvents.size))
                            info(String.format("livescore_event_top_backup    : %d", events.size))
                            info(String.format("livescore_event_top_flag      : %d", flags.size))
                            info(String.format("livescore_event_top_player    : %d", players.size))
                            info(String.format("livescore_event_top_position  : %d", positions.size))
                        }

                        with(saver) {
                            saveMatches(writeConn, matches)
                            saveMatchExtras(writeConn, matchExtras)
                            saveScoutEvents(writeConn, scoutEvents)
                            saveEvents(writeConn, events)
                            saveFlags(writeConn, flags)
                            savePlayers(writeConn, players)
                            savePositions(writeConn, positions)

                        }

                        log.info(String.format("generating requests for %d matches", matchIds.size))
                        val requests = matchIds
                            .map { matchId -> RequestUtil.createMatchRequest(5704, matchId) }
                        saver.saveRequests(writeConn, requests)
                    }
                }
            }
        } catch (e: SQLException) {
            log.error(e, e)
        }

    }

}

fun main(args: Array<String>) {
    LogUtil.setRootAppender();
    val historyRecoveryTool = HistoryRecoveryTool()
    historyRecoveryTool.runHBLRecovery()
}

