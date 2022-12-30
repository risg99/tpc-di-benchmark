-- statustype
truncate table master.statustype;
insert into master.statustype
	select * from staging.statustype;