-- tradetype
truncate table master.tradetype;
insert into master.tradetype
	select * from staging.tradetype;