COPY staging.batchdate FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/BatchDate.txt';

COPY staging.cashtransaction FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/CashTransaction.txt' delimiter '|';

COPY staging.dailymarket FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/DailyMarket.txt' delimiter '|';

COPY staging.date FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/Date.txt' delimiter '|';

COPY staging.holdinghistory FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/HoldingHistory.txt' delimiter '|';

COPY staging.hr FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/HR.csv' delimiter ',' CSV;

COPY staging.industry FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/Industry.txt' delimiter '|';

COPY staging.prospect FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/Prospect.csv' delimiter ',' CSV;

COPY staging.statustype FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/StatusType.txt' delimiter '|';

COPY staging.taxrate FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/TaxRate.txt' delimiter '|';

COPY staging.time FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/Time.txt' delimiter '|';

COPY staging.tradehistory FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/TradeHistory.txt' delimiter '|';

COPY staging.trade FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/Trade.txt' delimiter '|' null as '';

COPY staging.tradetype FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/TradeType.txt' delimiter '|';

COPY staging.watchhistory FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1/WatchHistory.txt' delimiter '|';

COPY staging.audit FROM 'C:/Users/Rishika Gupta/Desktop/tpc-di-benchmark/sf_3/output_data/Batch1_audit.csv' DELIMITER ',' HEADER CSV NULL AS '';


