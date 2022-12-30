-- transform and load
-- master.dimtrade
truncate table master.dimtrade;
insert into master.dimtrade
	with trades as (
		select
		  t.t_id
		, a.sk_brokerid
		, case
			when (th.th_st_id = 'SBMT' and t.t_tt_id in ('TMB', 'TMS')) or th.th_st_id = 'PNDG'
			then to_char(th.th_dts::date, 'yyyymmdd')::numeric
			else null
		  end as sk_createdateid
		, case
			when (th.th_st_id = 'SBMT' and t.t_tt_id in ('TMB', 'TMS')) or th.th_st_id = 'PNDG'
			then to_char(th.th_dts::time, 'hh24miss')::numeric
			else null
		  end as sk_createtimeid
		, case
			when th.th_st_id in ('CMPT', 'CNCL')
			then to_char(th.th_dts::date, 'yyyymmdd')::numeric
			else null
		  end as sk_closedateid
		, case
			when th.th_st_id in ('CMPT', 'CNCL')
			then to_char(th.th_dts::time, 'hh24miss')::numeric
			else null
		  end as sk_closetimeid
		, st.st_name
		, tt.tt_name
		, case
			when t.t_is_cash = 1 then true
			else false
		  end as t_is_cash
		, s.sk_securityid 
		, s.sk_companyid 
		, t.t_qty
		, t.t_bid_price
		, a.sk_customerid
		, a.sk_accountid
		, t.t_exec_name
		, t.t_trade_price
		, t.t_chrg
		, t.t_comm
		, t.t_tax
		, 1 as batchid
		, row_number() over(partition by t.t_id order by th.th_dts desc) as rn
		from staging.trade t
		inner join staging.tradehistory th
			on t.t_id = th.th_t_id
		inner join master.dimaccount a
			on t.t_ca_id = a.accountid
			and th.th_dts::date >= a.effectivedate
			and th.th_dts::date < a.enddate
		inner join master.statustype st
			on t.t_st_id = st.st_id
		inner join master.tradetype tt
			on t.t_tt_id = tt.tt_id
		inner join master.dimsecurity s
			on t.t_s_symb = s.symbol
			and th.th_dts::date >= s.effectivedate
			and th.th_dts::date < s.enddate
	)
	
	, trade_creation as (
		select
		t_id,
		min(sk_createdateid::varchar || sk_createtimeid::varchar) as trade_creation
		from trades
		group by t_id
	)
	
	, latest_trades as (
		select
		  t.t_id
		, sk_brokerid
		, coalesce(t.sk_createdateid::varchar, left(tc.trade_creation, 8))::numeric
		, coalesce(t.sk_createtimeid::varchar, right(tc.trade_creation, -8))::numeric
		, sk_closedateid
		, sk_closetimeid
		, st_name
		, tt_name
		, t_is_cash
		, sk_securityid 
		, sk_companyid 
		, t_qty
		, t_bid_price
		, sk_customerid
		, sk_accountid
		, t_exec_name
		, t_trade_price
		, t_chrg
		, t_comm
		, t_tax
		, batchid
		from trades t
		left join trade_creation tc
			on t.t_id = tc.t_id
		where rn = 1
	)

	select * from latest_trades;