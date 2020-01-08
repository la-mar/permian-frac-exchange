create user fsec;
grant usage on schema fsec to fsec;
grant all privileges on all tables in schema fsec to fsec;
grant all privileges on all sequences in schema fsec to fsec;
alter default privileges in schema fsec grant all on tables to fsec;
alter default privileges in schema fsec grant all on sequences to fsec;
alter user fsec with password 'YOUR_PASSWORD_HERE';

create table if not exists fsec.frac_schedules
(
	id integer,
	api14 varchar(14) primary key,
	api10 varchar(10),
	wellname varchar,
    operator varchar,
	frac_start_date date,
	frac_end_date date,
	status varchar(25),
	tvd integer,
	target_formation varchar(50),
	shllat double precision,
	shllon double precision,
	bhllat double precision,
	bhllon double precision,
	created_at timestamp with time zone default CURRENT_TIMESTAMP not null,
	updated_at timestamp with time zone default CURRENT_TIMESTAMP not null,
	updated_by varchar default CURRENT_USER not null,
	shl geometry(Point,4326),
	bhl geometry(Point,4326),
	stick geometry(LineString,4326),
	shl_webmercator geometry(Point,3857),
	bhl_webmercator geometry(Point,3857),
	stick_webmercator geometry(LineString,3857)

);

alter schema fsec owner to fsec;



