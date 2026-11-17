CREATE TABLE IF NOT EXISTS subscriptions (
  station_id VARCHAR(10) PRIMARY KEY,
  customer   VARCHAR(50),
  plan       VARCHAR(20),
  active     TINYINT(1)
);
INSERT INTO subscriptions VALUES
 ('S101','Anitha','premium',1),('S102','Karthik','basic',1),
 ('S103','Meena','basic',0),('S104','Ravi','premium',1),('S105','Divya','basic',1);
