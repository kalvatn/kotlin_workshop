package sportradar.history.recover.model

import java.sql.Timestamp

data class Match(
    val matchid: Long,
    val betradarmatchid: Long,
    val sportid: Int,
    val realcategoryid: Int,
    val tournamentid: Int,
    val teamidhome: Int,
    val teamidaway: Int,
    val dateofmatch: Timestamp?,
    val isStarted: Boolean,
    val currentperiod: Int,
    val currentperiod_time: Timestamp?,
    val isEnded: Boolean,
    val winner: Int,
    val score: String?,
    val period1: String?,
    val period2: String?,
    val period3: String?,
    val period4: String?,
    val period5: String?,
    val period6: String?,
    val period7: String?,
    val period8: String?,
    val period9: String?,
    val period10: String?,
    val period11: String?,
    val period12: String?,
    val period13: String?,
    val normaltime: String?,
    val extra1: String?,
    val extra2: String?,
    val overtime: String?,
    val penalties: String?,
    val isCancelled: Boolean,
    val isPostponed: Boolean,
    val isAbandoned: Boolean,
    val status: Int,
    val isHas_injuries: Boolean,
    val comment: String?,
    val lastgoal_time: Timestamp?,
    val lastgoal_team: Int,
    val manualdate: Int, // 0, 1, 255 ???
    val isHidden: Boolean,
    val lastupdate: Timestamp?,
    val isPublish: Boolean,
    val lineups_status: Int,
    val isFormations_status: Boolean,
    val isIsontv: Boolean,
    val isOnly_result: Boolean,
    val isMulticast: Boolean,
    val livetableid: Int,
    val stage: Int,
    val isFds_notes_checked: Boolean,
    val scoutmatch: Int, // 0, 1, 2
    val lineup_manager_status: Int, // 0, 1, 2
    val lineup_team_official_status: Int // 0, 1, 2
)
