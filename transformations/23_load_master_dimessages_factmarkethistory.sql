-- dimessages alert for factmarkethistory
insert into master.dimessages
	select
	  now()
	, 1
	, 'FactMarketHistory'
	, 'No earnings for company'
	, 'Alert'
	, 'DM_S_SYMB = ' || s.symbol
	from master.factmarkethistory fmh
	inner join master.dimsecurity s
		on fmh.sk_securityid = s.sk_securityid
	where fmh.peratio is null
	or fmh.peratio = 0;