COPY staging.trips 
FROM 's3://bixi.qc.raw/trips/'
compupdate off
statupdate off
IGNOREHEADER AS 1
DELIMITER ','
iam_role 'arn:aws:iam::982354555510:role/myRedshiftRole';

COPY staging.station_information
FROM 's3://bixi.qc.staged/station/station_information/'
compupdate off
statupdate off
IGNOREHEADER AS 1
DELIMITER ','
iam_role 'arn:aws:iam::982354555510:role/myRedshiftRole'
;

COPY staging.station_status
FROM 's3://bixi.qc.staged/station/station_status/'
compupdate off
statupdate off
IGNOREHEADER AS 1
DELIMITER ','
iam_role 'arn:aws:iam::982354555510:role/myRedshiftRole'
;