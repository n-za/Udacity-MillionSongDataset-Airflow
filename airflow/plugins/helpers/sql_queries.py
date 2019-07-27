class SqlQueries:
    insert_queries = {}
    create_queries = {}
    not_exists_subqueries = {}
    check_queries = {}
    create_queries['staging_events'] = ("""
		CREATE TABLE IF NOT EXISTS public.staging_events (
			artist varchar(256),
			auth varchar(256),
			firstname varchar(256),
			gender varchar(256),
			iteminsession varchar(256),
			lastname varchar(256),
			length varchar(256),
			"level" varchar(256),
			location varchar(256),
			"method" varchar(256),
			page varchar(256),
			registration varchar(256),
			sessionid varchar(256),
			song varchar(256),
			status varchar(256),
			ts varchar(256),
			useragent varchar(256),
			userid varchar(256)
		)
    """)
    
    create_queries['staging_songs'] = ("""
		CREATE TABLE IF NOT EXISTS public.staging_songs (
			num_songs varchar(256),
			artist_id varchar(256),
			artist_name varchar(256),
			artist_latitude varchar(256),
			artist_longitude varchar(256),
			artist_location varchar(256),
			song_id varchar(256),
			title varchar(256),
			duration varchar(256),
			"year" varchar(256)
		)
    """)
    
    
    create_queries['songplays'] = ("""
        CREATE TABLE IF NOT EXISTS public.songplays (
        	playkey int8 NOT NULL IDENTITY(1,1),
        	start_time timestamp NOT NULL,
        	userkey int8,
        	"level" varchar(256),
        	songkey int8,
        	artistkey int8,
        	sessionid int4,
            location varchar(256),
        	user_agent varchar(256),
        	CONSTRAINT songplays_pkey PRIMARY KEY (playkey)
        );
    """)
    
    create_queries['users'] = ("""
        CREATE TABLE IF NOT EXISTS public.users (
        	userkey int8 NOT NULL IDENTITY(1,1),
            userid varchar(256),
        	first_name varchar(256),
        	last_name varchar(256),
        	gender varchar(256),
        	"level" varchar(256),
        	CONSTRAINT users_pkey PRIMARY KEY (userkey)
        )
    """)
    
    create_queries['artists'] = ("""
        CREATE TABLE IF NOT EXISTS public.artists (
        	artistkey int8 NOT NULL IDENTITY(1,1),
            artistid varchar(256),
        	name varchar(256),
        	location varchar(256),
        	latitude varchar(256),
        	longitude varchar(256),
        	CONSTRAINT artists_pkey PRIMARY KEY (artistkey)
        )
""")
    
    create_queries['songs'] = ("""
        CREATE TABLE IF NOT EXISTS public.songs (
        	songkey int8 NOT NULL IDENTITY(1,1),
            songid varchar(256),
        	title varchar(256),
        	artistkey int8,
           	"year" int4,
            duration numeric(18,0),
        	CONSTRAINT songs_pkey PRIMARY KEY (songkey)
        ) 
    """)
    
    create_queries['time'] = ("""
        CREATE TABLE IF NOT EXISTS public.time(
        start_time timestamp not null primary key sortkey,
        hour numeric(2,0) not null,
        day numeric(2,0) not null,
        week numeric(2,0) not null,
        month numeric(2,0) not null,
        year numeric(4,0) not null,
        weekday varchar(20) not null
        )
    """)
    
    insert_queries['songplays'] = ("""
        INSERT INTO public.songplays(
        	start_time,
        	userkey,
        	"level",
        	songkey,
        	artistkey,
        	sessionid,
            location,
        	user_agent)
        SELECT
                events.start_time, 
                users.userkey, 
                events.level, 
                songs.songkey, 
                songs.artistkey, 
                events.sessionid::int4, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts::int8/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN artists
            ON events.artist = artists.name 
            LEFT JOIN songs
            ON events.song = songs.title 
            AND artists.artistkey = songs.artistkey
            AND events.length = songs.duration
            LEFT JOIN users
            ON events.firstname = users.first_name AND events.lastname = users.last_name
           WHERE extract(hour from start_time) = {}
    """)
    
    insert_queries['users'] = ("""
        INSERT INTO public.users(userid, first_name, last_name, gender, "level")
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events dnew
        WHERE page='NextSong' 
    """)
    
    insert_queries['songs'] = ("""
        INSERT INTO public.songs(songid, title, artistkey, "year", duration)
        SELECT distinct song_id, title, artists.artistkey, year::int4, duration::numeric(18, 0)
        FROM staging_songs dnew 
        LEFT OUTER JOIN artists ON dnew.artist_name = artists.name AND dnew.artist_location = artists.location
    """)
    
    insert_queries['artists'] = ("""
        INSERT INTO public.artists(artistid, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs dnew
    """)
    
    insert_queries['time'] = ("""
        INSERT INTO public.time(
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays dnew WHERE start_time IS NOT NULL
    """)

    insert_queries['users'] = ("""
        INSERT INTO public.users(userid, first_name, last_name, gender, "level")
        SELECT distinct userid::int4, firstname, lastname, gender, level
        FROM staging_events dnew
        WHERE page='NextSong' 
    """)
    
    not_exists_subqueries['songs'] = ("""
        WHERE NOT EXISTS (SELECT ''
        FROM songs dold WHERE dold.title = dnew.title AND artists.artistkey = dold.artistkey)
    """)
    
    not_exists_subqueries['artists'] = ("""
        WHERE NOT EXISTS (SELECT ''
        FROM artists dold 
        WHERE dold.name = dnew.artist_name AND dold.location = dnew.artist_location)
    """)
    
    not_exists_subqueries['time'] = ("""
        AND NOT EXISTS (SELECT ''
        FROM time dold WHERE dold.start_time = dnew.start_time)
    """)

    not_exists_subqueries['users'] = ("""
        AND NOT EXISTS (SELECT ''
        FROM users dold
        WHERE dold.first_name = dnew.firstname AND dold.last_name = lastname)
    """)

    check_queries['count_songs'] = ("""
        SELECT COUNT(*) FROM public.songs
    """)
    
    check_queries['count_artists'] = ("""
        SELECT COUNT(*) FROM public.artists
    """)
    
    check_queries['count_time'] = ("""
        SELECT COUNT(*) FROM public.time
    """)

    check_queries['count_users'] = ("""
        SELECT COUNT(*) FROM public.users
    """)

    check_queries['count_songplays'] = ("""
        SELECT COUNT(*) FROM public.songplays
    """)

    check_queries['dangling_songs'] = ("""
        SELECT COUNT(*) FROM songplays f WHERE NOT EXISTS (SELECT '' FROM public.songs dim WHERE dim.songkey = f.songkey)
    """)
    
    check_queries['dangling_artists'] = ("""
        SELECT COUNT(*) FROM songplays f WHERE NOT EXISTS (SELECT '' FROM public.artists dim WHERE dim.artistkey = f.artistkey)
    """)
    
    check_queries['dangling_time'] = ("""
        SELECT COUNT(*) FROM songplays f WHERE NOT EXISTS (SELECT '' FROM public.time dim WHERE dim.start_time = f.start_time)
    """)

    check_queries['dangling_users'] = ("""
        SELECT COUNT(*) FROM songplays f WHERE NOT EXISTS (SELECT '' FROM public.users dim WHERE dim.userkey = f.userkey)
    """)






