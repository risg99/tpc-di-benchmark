-- dimtime
truncate table master.dimtime;
insert into master.dimtime
	select 
	sk_timeid,
	timevalue::time, 
	hourid, 
	hourdesc, 
	minuteid, 
	minutedesc, 
	secondid, 
	seconddesc, 
	markethoursflag, 
	officehoursflag 
	from staging.time;