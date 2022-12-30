-- dimessages alert for dimtrade
insert into master.dimessages
	select
	  now()
	, 1
	, 'DimTrade'
	, 'Invalid trade commission'
	, 'Alert'
	, 'T_ID = ' || tradeid || ', T_COMM = ' || commission
	from master.dimtrade
	where commission is not null
	and commission > (tradeprice * quantity);
	
insert into master.dimessages
	select
	  now()
	, 1
	, 'DimTrade'
	, 'Invalid trade fee'
	, 'Alert'
	, 'T_ID = ' || tradeid || ', T_CHRG = ' || fee
	from master.dimtrade
	where fee is not null
	and fee > (tradeprice * quantity);