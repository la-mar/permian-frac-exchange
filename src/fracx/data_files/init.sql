create table {TABLE_NAME}
(
	id int identity,
	api14 varchar(14) not null,
	api10 varchar(10),
	operator varchar(100),
	wellname varchar(100),
	frac_start_date date not null,
	frac_end_date date not null,
	status varchar(100),
	tvd int,
	shllat float,
	shllon float,
	bhllat float,
	bhllon float,
	target_formation varchar(100),
	created_at datetime default CURRENT_TIMESTAMP not null,
	updated_at datetime default CURRENT_TIMESTAMP not null,
	updated_by varchar(100) default CURRENT_USER not null,
);

alter table {TABLE_NAME}
	add constraint pk_{TABLE_NAME}_api
		primary key (api14, frac_start_date, frac_end_date);


--

create view {TABLE_NAME}_most_recent_by_api10 as
    with most_recent as (
        select
            max({TABLE_NAME}_1.id) as id
        from {TABLE_NAME} {TABLE_NAME}_1
        group by {TABLE_NAME}_1.api10
    )
    select
        fs.id,
        fs.api14,
        fs.api10,
        fs.operator,
        fs.wellname,
        fs.tvd,
        fs.frac_start_date,
        fs.frac_end_date,
        fs.shllat,
        fs.shllon,
        fs.bhllat,
        fs.bhllon,
        fs.created_at,
        fs.updated_at,
        fs.updated_by,
        datediff(day,getdate(),[frac_start_date]) as days_to_frac_start_date,
        datediff(day,getdate(),[frac_end_date]) as days_to_frac_end_date,
        case when datediff(day,getdate(),[frac_start_date])>0 then 'Planned' when datediff(day,getdate(),[frac_end_date])>=0 then 'In-Progress' when datediff(day,getdate(),[frac_end_date])>(-30) then 'Completed in Last 30 Days' when datediff(day,getdate(),[frac_end_date])>(-60) then 'Completed in Last 60 Days' when datediff(day,getdate(),[frac_end_date])>(-90) then 'Completed in Last 90 Days' when datediff(day,getdate(),[frac_end_date])<=(-90) then 'Past Completion'  end as status,
        case when [shllon] IS NOT NULL AND [shllat] IS NOT NULL then [GEOMETRY]::Point([shllon],[shllat],4326)  end as shl,
        case when [bhllon] IS NOT NULL AND [bhllat] IS NOT NULL then [GEOMETRY]::Point([bhllon],[bhllat],4326)  end as bhl,
        case when [shllon] IS NOT NULL AND [shllat] IS NOT NULL AND [bhllon] IS NOT NULL AND [bhllat] IS NOT NULL then [Geometry]::STGeomFromText(((((((('LINESTRING ('+CONVERT([varchar],[shllon]))+' ')+CONVERT([varchar],[shllat]))+', ')+CONVERT([varchar],[bhllon]))+' ')+CONVERT([varchar],[bhllat]))+')',4326)  end as stick
    from {TABLE_NAME} fs
             join most_recent on fs.id = most_recent.id;


