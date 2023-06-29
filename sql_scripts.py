CREATE_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS {RAW_TABLE}
             (
                          listened_at timestamp ,
                          listened_at_dt date ,
                          recording_msid                                    varchar,
                          user_name                                         varchar,
                          track_metadata_additional_info_release_msid       varchar,
                          track_metadata_additional_info_release_mbid       varchar,
                          track_metadata_additional_info_recording_mbid     varchar,
                          track_metadata_additional_info_release_group_mbid varchar,
                          track_metadata_additional_info_artist_mbids       varchar,
                          track_metadata_additional_info_tags               varchar,
                          track_metadata_additional_info_work_mbids         varchar,
                          track_metadata_additional_info_isrc               varchar,
                          track_metadata_additional_info_spotify_id         varchar,
                          track_metadata_additional_info_tracknumber        varchar,
                          track_metadata_additional_info_track_mbid         varchar,
                          track_metadata_additional_info_artist_msid        varchar,
                          track_metadata_additional_info_recording_msid     varchar,
                          track_metadata_artist_name                        varchar,
                          track_metadata_track_name                         varchar,
                          track_metadata_release_name                       varchar,
                          unique_key_col varchar,
                          PRIMARY KEY (unique_key_col)
             )

"""


INSERT_INTO_RAW = """
INSERT
or     REPLACE
into   {RAW_TABLE}
       (
              unique_key_col,
              listened_at ,
              listened_at_dt,
              recording_msid ,
              user_name ,
              track_metadata_additional_info_release_msid ,
              track_metadata_additional_info_release_mbid ,
              track_metadata_additional_info_recording_mbid ,
              track_metadata_additional_info_release_group_mbid ,
              track_metadata_additional_info_artist_mbids ,
              track_metadata_additional_info_tags ,
              track_metadata_additional_info_work_mbids ,
              track_metadata_additional_info_isrc ,
              track_metadata_additional_info_spotify_id ,
              track_metadata_additional_info_tracknumber ,
              track_metadata_additional_info_track_mbid ,
              track_metadata_additional_info_artist_msid ,
              track_metadata_additional_info_recording_msid ,
              track_metadata_artist_name ,
              track_metadata_track_name ,
              track_metadata_release_name
       )
SELECT concat(listened_at,recording_msid,user_name),
       COALESCE(listened_at,'') ,
       COALESCE(listened_at_dt,'') ,
       COALESCE(recording_msid,''),
       COALESCE(user_name,''),
       COALESCE(track_metadata_additional_info_release_msid,''),
       COALESCE(track_metadata_additional_info_release_mbid,''),
       COALESCE(track_metadata_additional_info_recording_mbid,''),
       COALESCE(track_metadata_additional_info_release_group_mbid,''),
       COALESCE(track_metadata_additional_info_artist_mbids,[]),
       COALESCE(track_metadata_additional_info_tags,[]),
       COALESCE(track_metadata_additional_info_work_mbids,[]),
       COALESCE(track_metadata_additional_info_isrc,''),
       COALESCE(track_metadata_additional_info_spotify_id,''),
       COALESCE(track_metadata_additional_info_tracknumber,''),
       COALESCE(track_metadata_additional_info_track_mbid,''),
       COALESCE(track_metadata_additional_info_artist_msid,''),
       COALESCE(track_metadata_additional_info_recording_msid,''),
       COALESCE(track_metadata_artist_name,''),
       COALESCE(track_metadata_track_name,''),
       COALESCE(track_metadata_release_name ,'')
FROM   df1
"""

TASK_2_A_1 = """
SELECT user_name,
       Count(1) AS total_listens
FROM   raw_listens
GROUP  BY user_name
ORDER  BY total_listens DESC
LIMIT  10 

"""

TASK_2_A_2 = """
SELECT Count(DISTINCT user_name) number_of_users_listening
FROM   raw_listens
WHERE  listened_at_dt = '2019-03-01' 

"""

TASK_2_A_3 = """
SELECT   user_name,
         track_metadata_track_name as first_track
FROM     raw_listens QUALIFY row_number() OVER (partition BY user_name ORDER BY listened_at) = 1

"""

TASK_2_B = """
WITH cte AS
(
         SELECT   user_name,
                  listened_at_dt,
                  count(1)          as number_of_listens
         FROM     raw_listens
         GROUP BY user_name,
                  listened_at_dt)
SELECT   *
FROM     cte qualify row_number() OVER (partition BY user_name ORDER BY number_of_listens DESC) <= 3
ORDER BY user_name,
         number_of_listens

"""

TASK_2_C = """
WITH cte_total_users
     AS (SELECT Count(DISTINCT a.user_name) AS total_users,
                a.listened_at_dt
         FROM   raw_listens a
         GROUP  BY a.listened_at_dt
         ORDER  BY a.listened_at_dt),
     cte_total_active_users
     AS (SELECT Count(DISTINCT a.user_name) AS total_active_users,
                a.listened_at_dt
         FROM   raw_listens a
                JOIN raw_listens b
                  ON a.unique_key_col <> b.unique_key_col
                     AND a.user_name = b.user_name
                     AND a.listened_at_dt >= b.listened_at_dt + 6
         GROUP  BY a.listened_at_dt
         ORDER  BY a.listened_at_dt)
SELECT a.listened_at_dt,
       total_active_users,
       total_users
FROM   cte_total_users a
       JOIN cte_total_active_users b
         ON a.listened_at_dt = b.listened_at_dt 

"""
