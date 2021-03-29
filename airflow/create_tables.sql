CREATE TABLE IF NOT EXISTS public.video (
	video_id varchar(32) NOT NULL,
	title varchar(max),
	publish_date timestamp,
	thumbnail_link varchar(255),
	description varchar(max),
	tags varchar(max),
	CONSTRAINT video_pkey PRIMARY KEY (video_id)
);

CREATE TABLE  IF NOT EXISTS public.channel (
	channel_id varchar(32) NOT NULL,
	title varchar(max) NOT NULL,
	CONSTRAINT channel_pkey PRIMARY KEY (channel_id)
);

CREATE TABLE IF NOT EXISTS public.category (
	id numeric(18,0) NOT NULL,
	channel_id varchar(256),
	kind varchar(255),
	etag varchar(256),
	title varchar(max),
	assignable boolean,
	CONSTRAINT category_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.video_trend_event (
	trend_event_id INT IDENTITY(0,1),
	trending_date timestamp,
	video_id varchar(32),
	channel_id varchar(32),
	category_id numeric(18,0),
	view_count numeric(18,0),
	likes numeric(18,0),
	dislikes numeric(18,0),
	comment_count numeric(18,0),
	comments_disabled boolean,
    ratings_disabled boolean,
    CONSTRAINT video_trend_event_pkey PRIMARY KEY (trend_event_id)
);

CREATE TABLE IF NOT EXISTS public.staging_category (
	id numeric(18,0),
	channelid varchar(255),
	kind varchar(max),
	etag varchar(max),
	title varchar(max),
	assignable boolean
);

CREATE TABLE IF NOT EXISTS public.staging_video_trend_log (
	video_id varchar(32),
	title varchar(max),
	publishedAt varchar(50),
	channelId varchar(32),
	channelTitle varchar(max),
	categoryId numeric(18,0),
	trending_date varchar(32),
	tags varchar(max),
	view_count numeric(18,0),
	likes numeric(18,0),
	dislikes numeric(18,0),
	comment_count numeric(18,0),
    thumbnail_link varchar(255),
    comments_disabled boolean,
    ratings_disabled boolean,
    description varchar(max)
);

CREATE TABLE IF NOT EXISTS public."time" (
	trending_date timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (trending_date)
);
