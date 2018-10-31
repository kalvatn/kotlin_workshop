package sportradar.history.recover

import org.apache.log4j.Logger
import sportradar.history.recover.model.*
import sportradar.history.recover.util.QueryUtil
import sportradar.kotlin.workshop.examples._14LambdaWithReceiver.use
import java.sql.Connection
import java.sql.SQLException
import java.util.*

internal class Saver {

    fun saveMatches(conn: Connection, matches: Map<Long, Match>) {
        log.info(String.format("saving %d livescore_match rows", matches.keys.size))
        if (matches.isEmpty()) {
            return
        }
        val fields = Arrays.asList(
            "matchid",
            "betradarmatchid",
            "sportid",
            "realcategoryid",
            "tournamentid",
            "teamidhome",
            "teamidaway",
            "dateofmatch",
            "started",
            "currentperiod",
            "currentperiod_time",
            "ended",
            "winner",
            "score",
            "period1",
            "period2",
            "period3",
            "period4",
            "period5",
            "period6",
            "period7",
            "period8",
            "period9",
            "period10",
            "period11",
            "period12",
            "period13",
            "normaltime",
            "extra1",
            "extra2",
            "overtime",
            "penalties",
            "cancelled",
            "postponed",
            "abandoned",
            "status",
            "has_injuries",
            "comment",
            "lastgoal_time",
            "lastgoal_team",
            "manualdate",
            "hidden",
            "lastupdate",
            "publish",
            "lineups_status",
            "formations_status",
            "isontv",
            "only_result",
            "multicast",
            "livetableid",
            "Stage",
            "fds_notes_checked",
            "scoutmatch",
            "lineup_manager_status",
            "lineup_team_official_status"
        )
        val sql = QueryUtil.createInsertQuery("livescore_match", fields)

        try {
            conn.prepareStatement(sql).use { ps ->
                for ((_, match) in matches) {
                    var i = 1
                    ps.setLong(i++, match.matchid)
                    ps.setLong(i++, match.betradarmatchid)
                    ps.setInt(i++, match.sportid)
                    ps.setInt(i++, match.realcategoryid)
                    ps.setInt(i++, match.tournamentid)
                    ps.setInt(i++, match.teamidhome)
                    ps.setInt(i++, match.teamidaway)
                    ps.setTimestamp(i++, match.dateofmatch)
                    ps.setBoolean(i++, match.isStarted)
                    ps.setInt(i++, match.currentperiod)
                    ps.setTimestamp(i++, match.currentperiod_time)
                    ps.setBoolean(i++, match.isEnded)
                    ps.setInt(i++, match.winner)
                    ps.setString(i++, match.score)
                    ps.setString(i++, match.period1)
                    ps.setString(i++, match.period2)
                    ps.setString(i++, match.period3)
                    ps.setString(i++, match.period4)
                    ps.setString(i++, match.period5)
                    ps.setString(i++, match.period6)
                    ps.setString(i++, match.period7)
                    ps.setString(i++, match.period8)
                    ps.setString(i++, match.period9)
                    ps.setString(i++, match.period10)
                    ps.setString(i++, match.period11)
                    ps.setString(i++, match.period12)
                    ps.setString(i++, match.period13)
                    ps.setString(i++, match.normaltime)
                    ps.setString(i++, match.extra1)
                    ps.setString(i++, match.extra2)
                    ps.setString(i++, match.overtime)
                    ps.setString(i++, match.penalties)
                    ps.setBoolean(i++, match.isCancelled)
                    ps.setBoolean(i++, match.isPostponed)
                    ps.setBoolean(i++, match.isAbandoned)
                    ps.setInt(i++, match.status)
                    ps.setBoolean(i++, match.isHas_injuries)
                    ps.setString(i++, match.comment)
                    ps.setTimestamp(i++, match.lastgoal_time)
                    ps.setInt(i++, match.lastgoal_team)
                    ps.setInt(i++, match.manualdate)
                    ps.setBoolean(i++, match.isHidden)
                    ps.setTimestamp(i++, match.lastupdate)
                    ps.setBoolean(i++, match.isPublish)
                    ps.setInt(i++, match.lineups_status)
                    ps.setBoolean(i++, match.isFormations_status)
                    ps.setBoolean(i++, match.isIsontv)
                    ps.setBoolean(i++, match.isOnly_result)
                    ps.setBoolean(i++, match.isMulticast)
                    ps.setInt(i++, match.livetableid)
                    ps.setInt(i++, match.stage)
                    ps.setBoolean(i++, match.isFds_notes_checked)
                    ps.setInt(i++, match.scoutmatch)
                    ps.setInt(i++, match.lineup_manager_status)
                    ps.setInt(i, match.lineup_team_official_status)
                    ps.addBatch()
                }
                log.info(QueryUtil.psBatchUpdateToString(ps.executeBatch()))
            }
        } catch (e: SQLException) {
            log.error("error inserting matches", e)
        }

    }

    fun saveMatchExtras(conn: Connection, matchExtras: Map<Long, MatchExtra>) {
        log.info(String.format("saving %d livescore_match_extra rows", matchExtras.keys.size))
        if (matchExtras.isEmpty()) {
            return
        }
        val fields = Arrays.asList(
            "matchid",
            "seasonid",
            "isontv_internal",
            "1st_leg",
            "visibility_change",
            "stadiumid",
            "startingtime_changestatus",
            "1st_leg_matchid",
            "shirtnumbers",
            "internal_test_match",
            "current_server",
            "current_game_points",
            "match_seconds",
            "match_start",
            "start_type",
            "tiebreak_scores",
            "conditions",
            "deeper_coverage",
            "ballspotting",
            "tactical_lineup",
            "time_info",
            "suspension_status",
            "scout_connection",
            "coverage_level",
            "manually_hidden",
            "media_coverage",
            "unavailable",
            "official_data_locked",
            "current_period_seconds_left"
        )
        val sql = QueryUtil.createInsertQuery("livescore_match_extra", fields)

        try {
            conn.prepareStatement(sql).use { ps ->
                for ((_, matchExtra) in matchExtras) {
                    var i = 1
                    ps.setLong(i++, matchExtra.matchid)
                    ps.setInt(i++, matchExtra.seasonid)
                    ps.setBoolean(i++, matchExtra.isIsontv_internal)
                    ps.setString(i++, matchExtra.first_leg)
                    ps.setTimestamp(i++, matchExtra.visibility_change)
                    ps.setInt(i++, matchExtra.stadiumid)
                    ps.setInt(i++, matchExtra.startingtime_changestatus)
                    ps.setLong(i++, matchExtra.first_leg_matchid)
                    ps.setBoolean(i++, matchExtra.isShirtnumbers)
                    ps.setBoolean(i++, matchExtra.isInternal_test_match)
                    ps.setInt(i++, matchExtra.current_server)
                    ps.setString(i++, matchExtra.current_game_points)
                    ps.setInt(i++, matchExtra.match_seconds)
                    ps.setInt(i++, matchExtra.match_start)
                    ps.setInt(i++, matchExtra.start_type)
                    ps.setString(i++, matchExtra.tiebreak_scores)
                    ps.setString(i++, matchExtra.conditions)
                    ps.setBoolean(i++, matchExtra.isDeeper_coverage)
                    ps.setBoolean(i++, matchExtra.isBallspotting)
                    ps.setBoolean(i++, matchExtra.isTactical_lineup)
                    ps.setString(i++, matchExtra.time_info)
                    ps.setString(i++, matchExtra.suspension_status)
                    ps.setBoolean(i++, matchExtra.isScout_connection)
                    ps.setInt(i++, matchExtra.coverage_level)
                    ps.setBoolean(i++, matchExtra.isManually_hidden)
                    ps.setBoolean(i++, matchExtra.isMedia_coverage)
                    ps.setBoolean(i++, matchExtra.isUnavailable)
                    ps.setBoolean(i++, matchExtra.isOfficial_data_locked)
                    ps.setInt(i, matchExtra.current_period_seconds_left)
                    ps.addBatch()
                }
                log.info(QueryUtil.psBatchUpdateToString(ps.executeBatch()))
            }
        } catch (e: SQLException) {
            log.error("error inserting match extras", e)
        }

    }

    fun saveEvents(conn: Connection, events: Map<Long, Event>) {
        log.info(String.format("saving %d livescore_event_top_backup rows", events.keys.size))
        if (events.isEmpty()) {
            return
        }
        val fields = Arrays.asList(
            "id",
            "sourceid",
            "matchid",
            "eventtype",
            "time",
            "matchtime",
            "param1",
            "param2",
            "param3",
            "param4",
            "param5",
            "text",
            "eventdate",
            "updatedate",
            "disabled",
            "injurytime",
            "injurymatchtime",
            "scouteventid"
        )
        val sql = QueryUtil.createInsertQuery("livescore_event_top_backup", fields)

        try {
            conn.prepareStatement(sql).use { ps ->
                for ((_, event) in events) {
                    var i = 1
                    ps.setLong(i++, event.id)
                    ps.setInt(i++, event.sourceId)
                    ps.setLong(i++, event.matchId)
                    ps.setInt(i++, event.eventType)
                    ps.setInt(i++, event.time)
                    ps.setInt(i++, event.matchTime)
                    ps.setString(i++, event.param1)
                    ps.setString(i++, event.param2)
                    ps.setString(i++, event.param3)
                    ps.setString(i++, event.param4)
                    ps.setString(i++, event.param5)
                    ps.setString(i++, event.text)
                    ps.setTimestamp(i++, event.eventDate)
                    ps.setTimestamp(i++, event.updateDate)
                    ps.setBoolean(i++, event.isDisabled)
                    ps.setInt(i++, event.injuryTime)
                    ps.setInt(i++, event.injuryMatchTime)
                    ps.setLong(i, event.scoutEventId)
                    ps.addBatch()
                }
                log.info(QueryUtil.psBatchUpdateToString(ps.executeBatch()))
            }
        } catch (e: SQLException) {
            log.error("error inserting events", e)
        }

    }

    fun saveScoutEvents(conn: Connection, events: Map<String, ScoutEvent>) {
        log.info(String.format("saving %d livescore_scoutevent rows", events.keys.size))
        if (events.isEmpty()) {
            return
        }
        val fields = Arrays.asList(
            "id",
            "matchid",
            "eventtype",
            "time",
            "param1",
            "param2",
            "param3",
            "param4",
            "text",
            "eventdate",
            "injurytime"
        )
        val sql = QueryUtil.createInsertQuery("livescore_scoutevent", fields)

        try {
            conn.prepareStatement(sql).use { ps ->
                for ((_, event) in events) {
                    var i = 1
                    ps.setString(i++, event.id)
                    ps.setLong(i++, event.matchid)
                    ps.setInt(i++, event.eventtype)
                    ps.setInt(i++, event.time)
                    ps.setString(i++, event.param1)
                    ps.setString(i++, event.param2)
                    ps.setString(i++, event.param3)
                    ps.setString(i++, event.param4)
                    ps.setString(i++, event.text)
                    ps.setTimestamp(i++, event.eventdate)
                    ps.setInt(i, event.injurytime)
                    ps.addBatch()
                }
                log.info(QueryUtil.psBatchUpdateToString(ps.executeBatch()))
            }
        } catch (e: SQLException) {
            log.error("error inserting scout events", e)
        }

    }

    fun saveFlags(conn: Connection, flags: List<Flag>) {
        log.info(String.format("saving %d livescore_event_top_flag rows", flags.size))
        if (flags.isEmpty()) {
            return
        }
        val fields = Arrays.asList("eventid", "flagtype")
        val sql = QueryUtil.createInsertQuery("livescore_event_top_flag", fields)

        try {
            conn.prepareStatement(sql).use { ps ->
                for (flag in flags) {
                    var i = 1
                    ps.setLong(i++, flag.eventId)
                    ps.setInt(i, flag.flagType)
                    ps.addBatch()
                }
                log.info(QueryUtil.psBatchUpdateToString(ps.executeBatch()))
            }
        } catch (e: SQLException) {
            log.error("error inserting flags", e)
        }

    }

    fun savePlayers(conn: Connection, players: List<Player>) {
        log.info(String.format("saving %d livescore_event_top_player rows", players.size))
        if (players.isEmpty()) {
            return
        }
        val fields = Arrays.asList("eventid", "playerid", "number")
        val sql = QueryUtil.createInsertQuery("livescore_event_top_player", fields)

        try {
            conn.prepareStatement(sql).use { ps ->
                for (player in players) {
                    var i = 1
                    ps.setLong(i++, player.eventId)
                    ps.setInt(i++, player.playerId)
                    ps.setInt(i, player.number)
                    ps.addBatch()
                }
                log.info(QueryUtil.psBatchUpdateToString(ps.executeBatch()))
            }
        } catch (e: SQLException) {
            log.error("error inserting players", e)
        }

    }

    fun savePositions(conn: Connection, positions: List<Position>) {
        log.info(String.format("saving %d livescore_event_top_position rows", positions.size))
        if (positions.isEmpty()) {
            return
        }
        val fields = Arrays.asList("eventid", "posx", "posy")
        val sql = QueryUtil.createInsertQuery("livescore_event_top_position", fields)

        try {
            conn.prepareStatement(sql).use { ps ->
                for (position in positions) {
                    var i = 1
                    ps.setLong(i++, position.eventId)
                    ps.setInt(i++, position.posX)
                    ps.setInt(i, position.posY)
                    ps.addBatch()
                }
                log.info(QueryUtil.psBatchUpdateToString(ps.executeBatch()))
            }
        } catch (e: SQLException) {
            log.error("error inserting positions", e)
        }

    }

    fun saveRequests(conn: Connection, requests: List<LSFSRequest>) {
        log.info(String.format("saving %d livescore_filesender_requests rows", requests.size))
        if (requests.isEmpty()) {
            return
        }
        val fields = Arrays.asList(
            "requestedat",
            "userid",
            "`from`",
            "`to`",
            "target_type",
            "target_details",
            "recipientid",
            "matchids"
        )
        val sql = QueryUtil.createInsertQuery("livescore_filesender_requests", fields)
        try {
            conn.prepareStatement(sql).use { ps ->
                for (request in requests) {
                    var i = 1
                    ps.setTimestamp(i++, request.requestedat)
                    ps.setInt(i++, request.userid)
                    ps.setString(i++, request.from)
                    ps.setString(i++, request.to)
                    ps.setInt(i++, request.target_type)
                    ps.setString(i++, request.target_details)
                    ps.setInt(i++, request.recipientid)
                    ps.setString(i, request.matchids)
                    ps.addBatch()
                }
                log.info(QueryUtil.psBatchUpdateToString(ps.executeBatch()))

            }
        } catch (e: SQLException) {
            log.error("error inserting requests", e)
        }

    }

    companion object {
        private val log = Logger.getLogger(Saver::class.java)
    }
}
