-- dimbroker
truncate table master.dimbroker;
insert into master.dimbroker
	select 
	row_number() over(order by employeeid) as sk,
	employeeid as brokerid,
	managerid,
	employeefirstname,
	employeelastname,
	employeemi,
	employeebranch,
	employeeoffice,
	employeephone,
	true as iscurrent,
	1 as batchid,
	(select min(datevalue) FROM master.dimdate) as effectivedate,
	'9999-12-31'::date as enddate
	from staging.hr
	where employeejobcode = 314;