-- Calculated columns for MSSQL Frac Schedules
ALTER TABLE dbo.frac_schedule
    ADD days_to_fracstartdate AS datediff (day, getdate (),[fracstartdate]);

ALTER TABLE dbo.frac_schedule
    ADD days_to_fracenddate AS datediff (day, getdate (),[fracenddate]);

ALTER TABLE dbo.frac_schedule
    ADD status AS CASE WHEN datediff (day, getdate (),[fracstartdate]) > 0 THEN
        'Planned'
    WHEN datediff (day, getdate (),[fracenddate]) >= 0 THEN
        'In-Progress'
    WHEN datediff (day, getdate (),[fracenddate]) > (- 30) THEN
        'Completed in Last 30 Days'
    WHEN datediff (day, getdate (),[fracenddate]) > (- 60) THEN
        'Completed in Last 60 Days'
    WHEN datediff (day, getdate (),[fracenddate]) > (- 90) THEN
        'Completed in Last 90 Days'
    WHEN datediff (day, getdate (),[fracenddate]) <= (- 90) THEN
        'Past Completion'
    END;

ALTER TABLE dbo.frac_schedule
    ADD shl AS CASE WHEN[shllon] IS NOT NULL
        AND[shllat] IS NOT NULL THEN
[GEOMETRY]::Point([shllon],[shllat], 4326)
    END;

ALTER TABLE dbo.frac_schedule
    ADD bhl AS CASE WHEN[bhllon] IS NOT NULL
        AND[bhllat] IS NOT NULL THEN
[GEOMETRY]::Point([bhllon],[bhllat], 4326)
    END;

ALTER TABLE dbo.frac_schedule
    ADD stick AS CASE WHEN[shllon] IS NOT NULL
        AND[shllat] IS NOT NULL
        AND[bhllon] IS NOT NULL
        AND[bhllat] IS NOT NULL THEN
[Geometry]::STGeomFromText ('LINESTRING (' + CONVERT([varchar],[shllon]) + ' ' + CONVERT([varchar],[shllat]) + ', ' + CONVERT([varchar],[bhllon]) + ' ' + CONVERT([varchar],[bhllat]) + ')', 4326)
    END;

