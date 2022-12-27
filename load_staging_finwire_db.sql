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
					substring(x,1,15) as pts,
					substring(x,16,3) as rectype,
					substring(x,19,60) as companyname,
					substring(x,79,10) as cik,
					substring(x,89,4) as status,
					substring(x,93,2) as industryid,
					substring(x,95,4) as sprating,
					substring(x,99,8) as foundingdate,
					substring(x,107,80) as addressline1,
					substring(x,187,80) as addressline2,
					substring(x,267,12) as postalcode,
					substring(x,279,25) as city,
					substring(x,304,20) as stateprovince,
					substring(x,324,24) as country,
					substring(x,348,46) as ceoname,
					substring(x,394,150) as description						
				from staging.finwire limit 1;
		elsif substring(x,16,3) = 'SEC' then
			insert into staging.finwire_sec 
				select 
					substring(x,1,15) as pts,
					substring(x,16,3) as rectype,
					substring(x,19,15) as symbol,
					substring(x,34,6) as issuetype,
					substring(x,40,4) as status,
					substring(x,44,70) as name,
					substring(x,114,6) as exid,
					substring(x,120,13) as shout,
					substring(x,133,8) as firsttradedate,
					substring(x,141,8) as firsttradeexchg,
					substring(x,149,12) as dividend,
					substring(x,161,60) as conameorcik						
				from staging.finwire limit 1;
		elsif substring(x,16,3) = 'FIN' then
			insert into staging.finwire_fin 
				select 
					substring(x,1,15) as pts,
					substring(x,16,3) as rectype,
					substring(x,19,4) as year,
					substring(x,23,1) as quarter,
					substring(x,24,8) as qtrstartdate,
					substring(x,32,8) as postingdate,
					substring(x,40,17) as revenue,
					substring(x,57,17) as earnings,
					substring(x,74,12) as eps,
					substring(x,86,12) as dilutedeps,
					substring(x,98,12) as margin,
					substring(x,110,17) as inventory,
					substring(x,127,17) as assets,
					substring(x,144,17) as liability,
					substring(x,161,13) as shout,
					substring(x,174,13) as dilutedshout,
					substring(x,187,60) as conameorcik
				from staging.finwire limit 1;
		end if;
    END LOOP;
END;
$$ 
LANGUAGE plpgsql;

SELECT staging_finwire_split() as output;