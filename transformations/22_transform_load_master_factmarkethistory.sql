-- factmarkethistory
truncate table master.factmarkethistory;
insert into master.factmarkethistory
	with market_dates_daily as (
		select 
		  dm.dm_s_symb
		, dm.dm_date
		, dm.dm_close
		, dm.dm_high
		, dm.dm_low
		, dm.dm_vol
		, dd.sk_dateid
		from staging.dailymarket dm
		inner join master.dimdate dd 
			on dm.dm_date = dd.datevalue
		order by
			  dm.dm_s_symb
			, dm.dm_date desc
	)

	, high_low as (
		select
		  dm_s_symb
		, dm_date
		, dm_close
		, dm_high
		, dm_low
		, dm_vol
		, max(dm_high) over(partition by dm_s_symb order by dm_date rows between 363 preceding and current row) as fiftytwoweekhigh
		, min(dm_low) over(partition by dm_s_symb order by dm_date rows between 363 preceding and current row) as fiftytwoweeklow
		from market_dates_daily
	)

	, high_date as (
		select
		  hl.dm_s_symb
		, hl.dm_date
		, hl.dm_close
		, hl.dm_high
		, hl.dm_low
		, hl.dm_vol
		, hl.fiftytwoweekhigh
		, hl.fiftytwoweeklow
		, max(mdd.dm_date) as sk_fiftytwoweekhighdate
		from high_low hl
		inner join market_dates_daily mdd
			on hl.dm_s_symb = mdd.dm_s_symb
			and hl.fiftytwoweekhigh = mdd.dm_high
			and mdd.dm_date <= hl.dm_date
			and mdd.dm_date >= hl.dm_date - interval '52 weeks'
		group by
			  hl.dm_s_symb
			, hl.dm_date
			, hl.dm_close
			, hl.dm_high
			, hl.dm_low
			, hl.dm_vol
			, hl.fiftytwoweekhigh
			, hl.fiftytwoweeklow
	)

	, low_date as (
		select
		  hl.dm_s_symb
		, hl.dm_date
		, hl.dm_close
		, hl.dm_high
		, hl.dm_low
		, hl.dm_vol
		, hl.fiftytwoweekhigh
		, hl.fiftytwoweeklow
		, hl.sk_fiftytwoweekhighdate
		, max(mdd.dm_date) as sk_fiftytwoweeklowdate
		from high_date hl
		inner join market_dates_daily mdd
			on hl.dm_s_symb = mdd.dm_s_symb
			and hl.fiftytwoweeklow = mdd.dm_low
			and mdd.dm_date <= hl.dm_date
			and mdd.dm_date >= hl.dm_date - interval '52 weeks'
		group by
			  hl.dm_s_symb
			, hl.dm_date
			, hl.dm_close
			, hl.dm_high
			, hl.dm_low
			, hl.dm_vol
			, hl.fiftytwoweekhigh
			, hl.fiftytwoweeklow
			, hl.sk_fiftytwoweekhighdate
	)

	, quarters as (
			select
			  f.sk_companyid
			, f.fi_qtr_start_date
			, sum(fi_basic_eps) over (partition by c.companyid order by f.fi_qtr_start_date rows between 3 preceding and current row ) as eps_qtr_sum
			, lead(fi_qtr_start_date, 1, '9999-12-31'::date) over (partition by c.companyid order by f.fi_qtr_start_date asc) as next_qtr_start
			from master.financial f 
			inner join master.dimcompany c
				on f.sk_companyid = c.sk_companyid
	)

	, final_output as (
		select
		  s.sk_securityid
		, s.sk_companyid
		, to_char(ld.dm_date, 'yyyymmdd')::numeric as sk_dateid
		, case
			when q.eps_qtr_sum != 0 and q.eps_qtr_sum is not null
			then (ld.dm_close / q.eps_qtr_sum)::numeric(10, 2)
			else null
		  end as peratio
		, case
			when ld.dm_close != 0
			then round((s.dividend / ld.dm_close) * 100, 2)
			else null
		  end as yield
		, ld.fiftytwoweekhigh
		, to_char(ld.sk_fiftytwoweekhighdate, 'yyyymmdd')::numeric as sk_fiftytwoweekhighdate
		, ld.fiftytwoweeklow
		, to_char(ld.sk_fiftytwoweeklowdate, 'yyyymmdd')::numeric as sk_fiftytwoweeklowdate
		, ld.dm_close as closeprice
		, ld.dm_high as dayhigh
		, ld.dm_low as daylow
		, ld.dm_vol as volume
		, 1 as batchid
		from low_date ld 
			inner join master.dimsecurity s 
				on ld.dm_s_symb = s.symbol 
				and ld.dm_date >= s.effectivedate 
				and ld.dm_date < s.enddate
			inner join quarters q 
				on s.sk_companyid = q.sk_companyid 
				and q.fi_qtr_start_date <= ld.dm_date 
				and q.next_qtr_start > ld.dm_date
	)

	select * from final_output;