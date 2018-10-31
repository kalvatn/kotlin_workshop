package sportradar.history.recover

import org.apache.log4j.Logger
import sportradar.history.recover.model.*
import sportradar.history.recover.util.QueryUtil
import sportradar.kotlin.workshop.examples._14LambdaWithReceiver.use
import java.sql.Connection
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Instant
import java.util.*

internal class Loader {

    fun loadMatches(
        conn: Connection, fromDate: Instant, toDate: Instant, tournamentId: Int?
    ): Map<Long, Match> {
        log.info(
            String.format(
                "loading livescore_match for tournament %d between %s -> %s",
                tournamentId, fromDate, toDate
            )
        )

        val matches = HashMap<Long, Match>()

        try {
            conn.prepareStatement(QUERY_MATCHES).use { ps ->
                ps.setTimestamp(1, Timestamp.from(fromDate))
                ps.setTimestamp(2, Timestamp.from(toDate))
                ps.setInt(3, tournamentId!!)
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        val matchId = rs.getLong("matchid")

                        val match = Match(
                            matchid = matchId,
                            betradarmatchid = rs.getLong("betradarmatchid"),
                            sportid = rs.getInt("sportid"),
                            realcategoryid = rs.getInt("realcategoryid"),
                            tournamentid = rs.getInt("tournamentid"),
                            teamidhome = rs.getInt("teamidhome"),
                            teamidaway = rs.getInt("teamidaway"),
                            dateofmatch = rs.getTimestamp("dateofmatch"),
                            isStarted = rs.getBoolean("started"),
                            currentperiod = rs.getInt("currentperiod"),
                            currentperiod_time = rs.getTimestamp("currentperiod_time"),
                            isEnded = rs.getBoolean("ended"),
                            winner = rs.getInt("winner"),
                            score = rs.getString("score"),
                            period1 = rs.getString("period1"),
                            period2 = rs.getString("period2"),
                            period3 = rs.getString("period3"),
                            period4 = rs.getString("period4"),
                            period5 = rs.getString("period5"),
                            period6 = rs.getString("period6"),
                            period7 = rs.getString("period7"),
                            period8 = rs.getString("period8"),
                            period9 = rs.getString("period9"),
                            period10 = rs.getString("period10"),
                            period11 = rs.getString("period11"),
                            period12 = rs.getString("period12"),
                            period13 = rs.getString("period13"),
                            normaltime = rs.getString("normaltime"),
                            extra1 = rs.getString("extra1"),
                            extra2 = rs.getString("extra2"),
                            overtime = rs.getString("overtime"),
                            penalties = rs.getString("penalties"),
                            isCancelled = rs.getBoolean("cancelled"),
                            isPostponed = rs.getBoolean("postponed"),
                            isAbandoned = rs.getBoolean("abandoned"),
                            status = rs.getInt("status"),
                            isHas_injuries = rs.getBoolean("has_injuries"),
                            comment = rs.getString("comment"),
                            lastgoal_time = rs.getTimestamp("lastgoal_time"),
                            lastgoal_team = rs.getInt("lastgoal_team"),
                            manualdate = rs.getInt("manualdate"),
                            isHidden = rs.getBoolean("hidden"),
                            lastupdate = rs.getTimestamp("lastupdate"),
                            isPublish = rs.getBoolean("publish"),
                            lineups_status = rs.getInt("lineups_status"),
                            isFormations_status = rs.getBoolean("formations_status"),
                            isIsontv = rs.getBoolean("isontv"),
                            isOnly_result = rs.getBoolean("only_result"),
                            isMulticast = rs.getBoolean("multicast"),
                            livetableid = rs.getInt("livetableid"),
                            stage = rs.getInt("stage"),
                            isFds_notes_checked = rs.getBoolean("fds_notes_checked"),
                            scoutmatch = rs.getInt("scoutmatch"),
                            lineup_manager_status = rs.getInt("lineup_manager_status"),
                            lineup_team_official_status = rs.getInt("lineup_team_official_status")
                        )
                        matches[matchId] = match
                    }
                }
            }
        } catch (e: SQLException) {
            log.error("error loading matches", e)
        }

        return matches
    }

    fun loadMatchExtra(conn: Connection, matchIds: Set<Long>): Map<Long, MatchExtra> {
        log.info(String.format("loading livescore_match_extra for %d matches", matchIds.size))

        if (matchIds.isEmpty()) {
            return emptyMap()
        }

        val matchExtras = HashMap<Long, MatchExtra>()

        val sql = String.format(QUERY_MATCH_EXTRA, QueryUtil.createPlaceHolders(matchIds))

        try {
            conn.prepareStatement(sql).use { ps ->
                var i = 1
                for (matchId in matchIds) {
                    ps.setLong(i++, matchId)
                }
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        val matchId = rs.getLong("matchid")
                        val matchExtra = MatchExtra(
                            matchid = rs.getLong("matchid"),
                            seasonid = rs.getInt("seasonid"),
                            isIsontv_internal = rs.getBoolean("isontv_internal"),
                            first_leg = rs.getString("1st_leg"),
                            visibility_change = rs.getTimestamp("visibility_change"),
                            stadiumid = rs.getInt("stadiumid"),
                            startingtime_changestatus = rs.getInt("startingtime_changestatus"),
                            first_leg_matchid = rs.getLong("1st_leg_matchid"),
                            isShirtnumbers = rs.getBoolean("shirtnumbers"),
                            isInternal_test_match = rs.getBoolean("internal_test_match"),
                            current_server = rs.getInt("current_server"),
                            current_game_points = rs.getString("current_game_points"),
                            match_seconds = rs.getInt("match_seconds"),
                            match_start = rs.getInt("match_start"),
                            start_type = rs.getInt("start_type"),
                            tiebreak_scores = rs.getString("tiebreak_scores"),
                            conditions = rs.getString("conditions"),
                            isDeeper_coverage = rs.getBoolean("deeper_coverage"),
                            isBallspotting = rs.getBoolean("ballspotting"),
                            isTactical_lineup = rs.getBoolean("tactical_lineup"),
                            time_info = rs.getString("time_info"),
                            suspension_status = rs.getString("suspension_status"),
                            isScout_connection = rs.getBoolean("scout_connection"),
                            coverage_level = rs.getInt("coverage_level"),
                            isManually_hidden = rs.getBoolean("manually_hidden"),
                            isMedia_coverage = rs.getBoolean("media_coverage"),
                            isUnavailable = rs.getBoolean("unavailable"),
                            isOfficial_data_locked = rs.getBoolean("official_data_locked"),
                            current_period_seconds_left = rs.getInt("current_period_seconds_left")
                        )

                        matchExtras[matchId] = matchExtra
                    }
                }
            }
        } catch (e: SQLException) {
            log.error("error loading match extras", e)
        }

        return matchExtras
    }

    fun loadScoutEvents(conn: Connection, matchIds: Set<Long>): Map<String, ScoutEvent> {
        log.info(String.format("loading livescore_scoutevent for %d matches", matchIds.size))

        if (matchIds.isEmpty()) {
            return emptyMap()
        }

        val events = HashMap<String, ScoutEvent>()
        val sql = String.format(QUERY_SCOUT_EVENTS, QueryUtil.createPlaceHolders(matchIds))
        try {
            conn.prepareStatement(sql).use { ps ->
                var i = 1
                for (matchId in matchIds) {
                    ps.setLong(i++, matchId)
                }
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        val id = rs.getString("id")
                        val event = ScoutEvent(
                            id = id,
                            matchid = rs.getLong("matchid"),
                            eventtype = rs.getInt("eventtype"),
                            time = rs.getInt("time"),
                            param1 = rs.getString("param1"),
                            param2 = rs.getString("param2"),
                            param3 = rs.getString("param3"),
                            param4 = rs.getString("param4"),
                            text = rs.getString("text"),
                            eventdate = rs.getTimestamp("eventdate"),
                            injurytime = rs.getInt("injurytime")
                        )
                        events[id] = event
                    }
                }
            }
        } catch (e: SQLException) {
            log.error("error loading scout events", e)
        }

        return events
    }

    fun loadEvents(conn: Connection, matchIds: Set<Long>): Map<Long, Event> {
        log.info(String.format("loading livescore_event_top_backup for %d matches", matchIds.size))
        if (matchIds.isEmpty()) {
            return emptyMap<Long, Event>()
        }

        val eventIds = HashMap<Long, Event>()
        val sql = String.format(QUERY_EVENTS, QueryUtil.createPlaceHolders(matchIds))
        try {
            conn.prepareStatement(sql).use { ps ->
                var i = 1
                for (matchId in matchIds) {
                    ps.setLong(i++, matchId)
                }
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        val eventId = rs.getLong("id")
                        val event = Event(
                            id = eventId,
                            sourceId = rs.getInt("sourceid"),
                            matchId = rs.getLong("matchid"),
                            eventType = rs.getInt("eventtype"),
                            time = rs.getInt("time"),
                            matchTime = rs.getInt("matchtime"),
                            param1 = rs.getString("param1"),
                            param2 = rs.getString("param2"),
                            param3 = rs.getString("param3"),
                            param4 = rs.getString("param4"),
                            param5 = rs.getString("param5"),
                            text = rs.getString("text"),
                            eventDate = rs.getTimestamp("eventdate"),
                            updateDate = rs.getTimestamp("updatedate"),
                            isDisabled = rs.getBoolean("disabled"),
                            injuryTime = rs.getInt("injurytime"),
                            injuryMatchTime = rs.getInt("injurymatchtime"),
                            scoutEventId = rs.getLong("scouteventid")
                        )
                        eventIds[eventId] = event
                    }
                }
            }
        } catch (e: SQLException) {
            log.error("error loading events", e)
        }

        return eventIds
    }

    fun loadEventFlags(conn: Connection, eventIds: Set<Long>): List<Flag> {
        log.info(String.format("loading livescore_event_top_flag for %d events", eventIds.size))
        if (eventIds.isEmpty()) {
            return emptyList<Flag>()
        }

        val flags = ArrayList<Flag>()
        val sql = String.format(QUERY_EVENT_FLAGS, QueryUtil.createPlaceHolders(eventIds))
        try {
            conn.prepareStatement(sql).use { ps ->
                var i = 1
                for (eventId in eventIds) {
                    ps.setLong(i++, eventId)
                }
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        val flag = Flag(
                            eventId = rs.getLong("eventid"),
                            flagType = rs.getInt("flagtype")
                        )
                        flags.add(flag)
                    }
                }
            }
        } catch (e: SQLException) {
            log.error("error loading event flags", e)
        }

        return flags
    }

    fun loadEventPlayers(conn: Connection, eventIds: Set<Long>): List<Player> {
        log.info(String.format("loading livescore_event_top_player for %d events", eventIds.size))
        if (eventIds.isEmpty()) {
            return emptyList()
        }

        val players = ArrayList<Player>()
        val sql = String.format(QUERY_EVENT_PLAYERS, QueryUtil.createPlaceHolders(eventIds))
        try {
            conn.prepareStatement(sql).use { ps ->
                var i = 1
                for (eventId in eventIds) {
                    ps.setLong(i++, eventId)
                }
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        val player = Player(
                            eventId = rs.getLong("eventid"),
                            playerId = rs.getInt("playerid"),
                            number = rs.getInt("number")
                        )
                        players.add(player)
                    }
                }
            }
        } catch (e: SQLException) {
            log.error("error loading event players", e)
        }

        return players
    }

    fun loadEventPositions(conn: Connection, eventIds: Set<Long>): List<Position> {
        log.info(String.format("loading livescore_event_top_position for %d events", eventIds.size))
        if (eventIds.isEmpty()) {
            return emptyList()
        }

        val positions = ArrayList<Position>()
        val sql = String.format(QUERY_EVENT_POSITIONS, QueryUtil.createPlaceHolders(eventIds))
        try {
            conn.prepareStatement(sql).use { ps ->
                var i = 1
                for (eventId in eventIds) {
                    ps.setLong(i++, eventId)
                }
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        val position = Position(
                            eventId = rs.getLong("eventid"),
                            posX = rs.getInt("posx"),
                            posY = rs.getInt("posy")
                        )
                        positions.add(position)
                    }
                }
            }
        } catch (e: SQLException) {
            log.error("error loading event positions", e)
        }

        return positions
    }

    companion object {
        private val log = Logger.getLogger(Loader::class.java)
        private const val QUERY_MATCHES =
            "select * from livescore_match lm where lm.dateofmatch BETWEEN ? and ? and lm.tournamentid = ?"
        private const val QUERY_MATCH_EXTRA = "select * from livescore_match_extra lme where lme.matchid in (%s)"
        private const val QUERY_EVENTS = "select * from livescore_event_top_backup e where e.matchid in (%s)"
        private const val QUERY_SCOUT_EVENTS = "select * from livescore_scoutevent e where e.matchid in (%s)"
        private const val QUERY_EVENT_FLAGS = "select * from livescore_event_top_flag f where f.eventid in (%s)"
        private const val QUERY_EVENT_PLAYERS = "select * from livescore_event_top_player p where p.eventid in (%s)"
        private const val QUERY_EVENT_POSITIONS = "select * from livescore_event_top_position p where p.eventid in (%s)"
    }
}
