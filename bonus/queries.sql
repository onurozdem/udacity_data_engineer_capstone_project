/* Getting video trend data with video,channel,category information */
SELECT
*
  FROM video_trend_event vte
   JOIN video v ON vte.video_id = v.video_id
   JOIN channel cha ON vte.channel_id = cha.channel_id
   JOIN category cat ON vte.category_id = cat.id

/* Getting last trend data of each video with video information*/
SELECT
*
FROM ( SELECT video_id,
               MAX(view_count) as view_count,
               MAX(likes) as likes,
               MAX(dislikes) as dislikes,
               MAX(comment_count) as comment_count
          FROM video_trend_event
          GROUP BY video_id) vte
  JOIN video v ON vte.video_id = v.video_id
  JOIN channel cha ON vte.channel_id = cha.channel_id
  JOIN category cat ON vte.category_id = cat.id

/* Getting video number of each channel. */
SELECT vte.channel_id,
       cha.title,
       COUNT(vte.video_id)
FROM (SELECT DISTINCT video_id,
                      channel_id
        FROM video_trend_event) vte
  JOIN channel cha ON vte.channel_id = cha.channel_id
GROUP BY  vte.channel_id, cha.title
