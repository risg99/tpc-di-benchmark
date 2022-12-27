truncate table staging.finwire_cmp;
truncate table staging.finwire_sec;
truncate table staging.finwire_fin;

CREATE OR REPLACE FUNCTION staging_finwire_split() 
  RETURNS VOID 
AS
$$
DECLARE 
   x varchar := '';
BEGIN
    FOR x in SELECT * FROM staging.finwire LOOP
		if substring(x,16,3) = 'CMP' then
			insert into staging.finwire_cmp 
				select 
					nullif(trim(both from substring(x,1,15)), '') as pts,
					nullif(trim(both from substring(x,16,3)), '') as rectype,
					nullif(trim(both from substring(x,19,60)), '') as companyname,
					nullif(trim(both from substring(x,79,10)), '') as cik,
					nullif(trim(both from substring(x,89,4)), '') as status,
					nullif(trim(both from substring(x,93,2)), '') as industryid,
					nullif(trim(both from substring(x,95,4)), '') as sprating,
					nullif(trim(both from substring(x,99,8)), '') as foundingdate,
					nullif(trim(both from substring(x,107,80)), '') as addressline1,
					nullif(trim(both from substring(x,187,80)), '') as addressline2,
					nullif(trim(both from substring(x,267,12)), '') as postalcode,
					nullif(trim(both from substring(x,279,25)), '') as city,
					nullif(trim(both from substring(x,304,20)), '') as stateprovince,
					nullif(trim(both from substring(x,324,24)), '') as country,
					nullif(trim(both from substring(x,348,46)), '') as ceoname,
					nullif(trim(both from substring(x,394,150)), '') as description						
				from staging.finwire limit 1;
		elsif substring(x,16,3) = 'SEC' then
			insert into staging.finwire_sec 
				select 
					nullif(trim(both from substring(x,1,15)), '') as pts,
					nullif(trim(both from substring(x,16,3)), '') as rectype,
					nullif(trim(both from substring(x,19,15)), '') as symbol,
					nullif(trim(both from substring(x,34,6)), '') as issuetype,
					nullif(trim(both from substring(x,40,4)), '') as status,
					nullif(trim(both from substring(x,44,70)), '') as name,
					nullif(trim(both from substring(x,114,6)), '') as exid,
					nullif(trim(both from substring(x,120,13)), '') as shout,
					nullif(trim(both from substring(x,133,8)), '') as firsttradedate,
					nullif(trim(both from substring(x,141,8)), '') as firsttradeexchg,
					nullif(trim(both from substring(x,149,12)), '') as dividend,
					nullif(trim(both from substring(x,161,60)), '') as conameorcik						
				from staging.finwire limit 1;
		elsif substring(x,16,3) = 'FIN' then
			insert into staging.finwire_fin 
				select 
					nullif(trim(both from substring(x,1,15)), '') as pts,
					nullif(trim(both from substring(x,16,3)), '') as rectype,
					nullif(trim(both from substring(x,19,4)), '') as year,
					nullif(trim(both from substring(x,23,1)), '') as quarter,
					nullif(trim(both from substring(x,24,8)), '') as qtrstartdate,
					nullif(trim(both from substring(x,32,8)), '') as postingdate,
					nullif(trim(both from substring(x,40,17)), '') as revenue,
					nullif(trim(both from substring(x,57,17)), '') as earnings,
					nullif(trim(both from substring(x,74,12)), '') as eps,
					nullif(trim(both from substring(x,86,12)), '') as dilutedeps,
					nullif(trim(both from substring(x,98,12)), '') as margin,
					nullif(trim(both from substring(x,110,17)), '') as inventory,
					nullif(trim(both from substring(x,127,17)), '') as assets,
					nullif(trim(both from substring(x,144,17)), '') as liability,
					nullif(trim(both from substring(x,161,13)), '') as shout,
					nullif(trim(both from substring(x,174,13)), '') as dilutedshout,
					nullif(trim(both from substring(x,187,60)), '') as conameorcik
				from staging.finwire limit 1;
		end if;
    END LOOP;
END;
$$ 
LANGUAGE plpgsql;

SELECT staging_finwire_split() as output;