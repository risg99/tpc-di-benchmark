-- taxrate
truncate table master.taxrate;
insert into master.taxrate
	select * from staging.taxrate;