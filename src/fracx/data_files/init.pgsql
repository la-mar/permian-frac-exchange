create extension if not exists postgis;
create table if not exists {DATABASE_SCHEMA}.{TABLE_NAME}
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
	constraint {TABLE_NAME}_pkey
		primary key (api14, frac_start_date, frac_end_date)
);

create index if not exists {TABLE_NAME}_api10_index
	on {DATABASE_SCHEMA}.{TABLE_NAME} (api10);

create or replace view {DATABASE_SCHEMA}.{TABLE_NAME}_most_recent_by_api10 as
with most_recent as (
    select
        max(fs.id) as id
    from {TABLE_NAME} fs
    group by fs.api10
)
select
    fcs.id,
    fcs.api14,
    fcs.api10,
    fcs.operator,
    fcs.wellname,
    fcs.tvd,
    fcs.frac_start_date,
    fcs.frac_end_date,
    fcs.shllat,
    fcs.shllon,
    fcs.bhllat,
    fcs.bhllon,
    fcs.created_at,
    fcs.updated_at,
    fcs.updated_by,
    fcs.shl,
    fcs.bhl,
    fcs.stick,
    fcs.shl_webmercator,
    fcs.bhl_webmercator,
    fcs.stick_webmercator
from {DATABASE_SCHEMA}.{TABLE_NAME} fcs
         join most_recent on fcs.id = most_recent.id;

