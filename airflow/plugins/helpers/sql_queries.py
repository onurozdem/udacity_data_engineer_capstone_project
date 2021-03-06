class SqlQueries:

    video_table_insert = ("""
            SELECT 	video_id,
	                title,
	                TO_TIMESTAMP(publishedat, 'YYYY-MM-DDTHH:MI:SSZ') as publish_date,
	                thumbnail_link,
	                description,
	                tags
            FROM public.staging_video_trend_log
        """)

    channel_table_insert = ("""
            SELECT 	channelid as channel_id,
                    channeltitle as title
            FROM public.staging_video_trend_log
        """)

    category_table_insert = ("""
            SELECT 	id,
	                channelid as channel_id, 
	                kind,
	                etag,
	                title,
	                assignable
            FROM public.staging_category
        """)

    video_trend_event_table_insert = (""" (video_id, trending_date, channel_id, category_id, view_count, likes, dislikes, comment_count, comments_disabled, ratings_disabled) 
            SELECT 	video_id,
	                TO_TIMESTAMP(trending_date, 'YYYY-MM-DDTHH:MI:SSZ') as trending_date,
	                channelid as channel_id,
	                categoryid as category_id,
	                view_count,
	                likes,
	                dislikes,
	                comment_count,
	                comments_disabled,
	                ratings_disabled
            FROM public.staging_video_trend_log
        """)

    time_table_insert = ("""
        SELECT trending_date, extract(hour from trending_date), extract(day from trending_date), extract(week from trending_date), 
               extract(month from trending_date), extract(year from trending_date), extract(dayofweek from trending_date)
        FROM (SELECT TO_TIMESTAMP(trending_date, 'YYYY-MM-DDTHH:MI:SSZ') as trending_date FROM public.staging_video_trend_log)
    """)
