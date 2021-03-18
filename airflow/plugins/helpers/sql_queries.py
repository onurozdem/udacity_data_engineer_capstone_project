class SqlQueries:

    video_table_insert = ("""
            SELECT 	video_id,
	                title,
	                TO_DATE(publishAt, "YYYY-MM-DDTHH:MI:SSZ") as publish_date,
	                thumbnail_link,
	                description,
	                tags
            FROM public.staging_video_trend_log
        """)

    channel_table_insert = ("""
            SELECT 	channelID as channel_id,
                    channelTitle as title
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

    video_trend_event_table_insert = ("""
            SELECT 	video_id,
	                channelID as channel_id,
	                categoryId as category_id,
	                view_count,
	                likes,
	                dislikes,
	                comment_count,
	                comments_disabled,
	                ratings_disabled
            FROM public.staging_video_trend_log
        """)

    time_table_insert = ("""
        SELECT publish_date, extract(hour from publish_date), extract(day from publish_date), extract(week from publish_date), 
               extract(month from publish_date), extract(year from publish_date), extract(dayofweek from publish_date)
        FROM (SELECT TO_DATE(publishAt, "YYYY-MM-DDTHH:MI:SSZ") as publish_date FROM public.staging_video_trend_log)
    """)