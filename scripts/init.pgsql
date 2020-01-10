create table if not exists frac_schedules
(
	id serial not null,
	api14 varchar(14) not null,
	api10 varchar(10),
	wellname varchar,
	operator varchar,
	frac_start_date date not null,
	frac_end_date date not null,
	status varchar(25),
	tvd integer,
	target_formation varchar(50),
	shllat double precision not null,
	shllon double precision not null,
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
	stick_webmercator geometry(LineString,3857),
	constraint frac_schedules_pkey
		primary key (api14, frac_start_date, frac_end_date)
);

create index if not exists frac_schedules_api10_index
	on frac_schedules (api10);

create view fracx.frac_schedules_most_recent_by_api14 as
with most_recent as (
    select
        max(frac_schedules_1.id) as id
    from fracx.frac_schedules frac_schedules_1
    group by frac_schedules_1.api14
)
select
    frac_schedules.id,
    frac_schedules.api14,
    frac_schedules.api10,
    frac_schedules.operator,
    frac_schedules.wellname,
    frac_schedules.tvd,
    frac_schedules.frac_start_date,
    frac_schedules.frac_end_date,
    frac_schedules.shllat,
    frac_schedules.shllon,
    frac_schedules.bhllat,
    frac_schedules.bhllon,
    frac_schedules.created_at,
    frac_schedules.updated_at,
    frac_schedules.updated_by,
    frac_schedules.shl,
    frac_schedules.bhl,
    frac_schedules.stick,
    frac_schedules.shl_webmercator,
    frac_schedules.bhl_webmercator,
    frac_schedules.stick_webmercator
from fracx.frac_schedules
         join most_recent on frac_schedules.id = most_recent.id;



