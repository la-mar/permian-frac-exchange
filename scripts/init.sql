create table frac_schedule
(
	id int identity,
	api14 varchar(14) not null,
	api10 varchar(10),
	operator varchar(100),
	operator_alias varchar(50),
	wellname varchar(100),
	fracstartdate date,
	fracenddate date,
	tvd int,
	shllat float,
	shllon float,
	bhllat float,
	bhllon float,
	days_to_fracstartdate as datediff(day,getdate(),[fracstartdate]),
	days_to_fracenddate as datediff(day,getdate(),[fracenddate]),
	status as case when datediff(day,getdate(),[fracstartdate])>0 then 'Planned' when datediff(day,getdate(),[fracenddate])>=0 then 'In-Progress' when datediff(day,getdate(),[fracenddate])>(-30) then 'Completed in Last 30 Days' when datediff(day,getdate(),[fracenddate])>(-60) then 'Completed in Last 60 Days' when datediff(day,getdate(),[fracenddate])>(-90) then 'Completed in Last 90 Days' when datediff(day,getdate(),[fracenddate])<=(-90) then 'Past Completion'  end,
	updated datetime default getdate(),
	inserted datetime default getdate() not null,
	shl as case when [shllon] IS NOT NULL AND [shllat] IS NOT NULL then [GEOMETRY]::Point([shllon],[shllat],4326)  end,
	bhl as case when [bhllon] IS NOT NULL AND [bhllat] IS NOT NULL then [GEOMETRY]::Point([bhllon],[bhllat],4326)  end,
	stick as case when [shllon] IS NOT NULL AND [shllat] IS NOT NULL AND [bhllon] IS NOT NULL AND [bhllat] IS NOT NULL then [Geometry]::STGeomFromText(((((((('LINESTRING ('+CONVERT([varchar],[shllon]))+' ')+CONVERT([varchar],[shllat]))+', ')+CONVERT([varchar],[bhllon]))+' ')+CONVERT([varchar],[bhllat]))+')',4326)  end
)
go

alter table frac_schedule
	add constraint pk_frac_schedules_api
		primary key (api14)
go

