-- industry
truncate table master.industry;
insert into master.industry
	select * from staging.industry;