-- dimessages alert for dimcompany
truncate table master.dimessages;
insert into master.dimessages
	select 
	now(),
	1 as batchid,
	'DimCompany' as messagesource,
	'Invalid SPRating' as messagetext,
	'Alert' as messagetype,
	'CO_ID = ' || cik::varchar || ', CO_SP_RATE = ' || sprating::varchar
	from staging.finwire_cmp
	where sprating not in ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D');