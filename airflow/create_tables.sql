CREATE TABLE IF NOT EXISTS public.video (
	video_id varchar(32) NOT NULL,
	title varchar(255),
	publish_date timestamp,
	thumbnail_link varchar(255),
	description varchar(255),
	tags varchar(255),
	CONSTRAINT video_pkey PRIMARY KEY (video_id)
);

CREATE TABLE  IF NOT EXISTS public.channel (
	channel_id varchar(32) NOT NULL,
	title timestamp NOT NULL,
	CONSTRAINT channel_pkey PRIMARY KEY (channel_id)
);

CREATE TABLE IF NOT EXISTS public.category (
	id numeric(18,0) NOT NULL,
	channelid varchar(256),
	kind varchar(255),
	etag varchar(256),
	title varchar(256),
	assignable varchar(256),
	CONSTRAINT category_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.video_trend_event (
	trend_event_id numeric(18,0)
	video_id varchar(32),
	channel_id varchar(32),
	category_id numeric(18,0),
	view_count numeric(18,0),
	likes numeric(18,0),
	dislikes numeric(18,0),
	comment_count numeric(18,0),
	comments_disabled boolean,
    ratings_disabled boolean
);

CREATE TABLE IF NOT EXISTS public.staging_category (
	id numeric(18,0),
	channelid varchar(255),
	kind varchar(255),
	etag varchar(255),
	title varchar(255),
	assignable boolean
);

CREATE TABLE IF NOT EXISTS public.staging_video_trend_log (
	video_id varchar(32),
	title varchar(255),
	publishAt varchar(50),
	channelId varchar(32),
	channelTitle varchar(255),
	categoryId numeric(18,0),
	trending_date timestamp,
	tags varchar(255),
	view_count numeric(18,0),
	likes numeric(18,0),
	dislikes numeric(18,0),
	comment_count numeric(18,0),
    thumbnail_link varchar(255),
    comments_disabled boolean,
    ratings_disabled boolean,
    description varchar(255)
);

CREATE TABLE IF NOT EXISTS public."time" (
	trending_date timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);
