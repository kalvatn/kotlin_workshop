package sportradar.history.recover.model

import java.sql.Timestamp

data class MatchExtra(
    val matchid: Long,
    val seasonid: Int,
    val isIsontv_internal: Boolean,
    val first_leg: String?,
    val visibility_change: Timestamp?,
    val stadiumid: Int,
    val startingtime_changestatus: Int,
    val first_leg_matchid: Long,
    val isShirtnumbers: Boolean,
    val isInternal_test_match: Boolean,
    val current_server: Int, // 0, 1, 2
    val current_game_points: String?,
    val match_seconds: Int,
    val match_start: Int,
    val start_type: Int,
    val tiebreak_scores: String?,
    val conditions: String?,
    val isDeeper_coverage: Boolean,
    val isBallspotting: Boolean,
    val isTactical_lineup: Boolean,
    val time_info: String?,
    val suspension_status: String?,
    val isScout_connection: Boolean,
    val coverage_level: Int, // 0, 1, 2, 3, 4
    val isManually_hidden: Boolean,
    val isMedia_coverage: Boolean,
    val isUnavailable: Boolean,
    val isOfficial_data_locked: Boolean,
    val current_period_seconds_left: Int
)
