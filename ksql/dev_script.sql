
-- create a raw strem
DROP STREAM if exists raw_telecom ;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM  raw_telecom WITH \
(KAFKA_TOPIC='dbserver2.inventory.raw_telecom', VALUE_FORMAT='AVRO');

-- filter of  tables
DROP STREAM if exists replace_simple_with_standard_terminology;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM replace_simple_with_standard_terminology AS 
SELECT
 CASE
   WHEN AFTER -> F ='Originating' THEN 'Outgoing'
   WHEN AFTER -> F ='Terminating' THEN 'Incoming'
   ELSE AFTER -> F
 END AS F,
 CASE
   WHEN AFTER -> JH ='Success' THEN 'Voice Portal'
   ELSE AFTER -> JH
 END AS JH,
 CASE
   WHEN AFTER -> LA ='Shared Call Appearance' THEN 'Secondary Device'
   ELSE AFTER -> LA
 END AS LA,AFTER->ER,AFTER -> INDEX
FROM RAW_TELECOM
EMIT CHANGES;

-- combine_all_services
DROP STREAM if exists combine_all_services;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM combine_all_services AS 
SELECT
 CASE
  WHEN  ER IS NULL THEN 
    CASE
      WHEN LA IS NOT NULL and JH IS NOT NULL THEN CONCAT_WS(',',LA,JH)
      WHEN LA IS NOT NULL then LA
    ELSE
      JH
    END
  ELSE  ER
 END AS ER,LA,JH,F,INDEX
 FROM REPLACE_SIMPLE_WITH_STANDARD_TERMINOLOGY
EMIT CHANGES;


-- first step double to bigint
-- change double to varchar
DROP STREAM if exists double_to_BIGINT_timestamp;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM double_to_BIGINT_timestamp
  WITH(KAFKA_TOPIC='double_to_BIGINT_timestamp') AS
  SELECT 
    CAST(AFTER->J AS BIGINT) as start_time,
    CAST(AFTER->N AS BIGINT) as end_time,AFTER -> INDEX
  FROM raw_telecom
  EMIT CHANGES;

-- 2nd step would be bigint to timestamp

DROP STREAM if exists BIGINT_to_timestamp;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM BIGINT_to_timestamp AS 
SELECT 
       CAST(start_time AS VARCHAR) as CAST_date,
       SUBSTRING(CAST(start_time AS VARCHAR),0,8) as call_start_date,
       SUBSTRING(CAST(start_time AS VARCHAR),9,LEN(CAST(start_time AS VARCHAR))) as call_start_time,
       SUBSTRING(CAST(end_time AS VARCHAR),0,8) as call_end_date,
       SUBSTRING(CAST(end_time AS VARCHAR),9,LEN(CAST(end_time AS VARCHAR))) as call_end_time,
       INDEX
FROM double_to_BIGINT_timestamp
EMIT CHANGES;


-- -----3rd step--------start date with week day name---------------------
DROP STREAM if exists date_time_column;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM date_time_column AS 
SELECT 
  CASE 
    WHEN CAST(SUBSTRING(CALL_START_TIME,0,2) as int)>=12 THEN
      CASE  
        WHEN  CAST(SUBSTRING(CALL_START_TIME,0,2) as int)=12 THEN  CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2)) 
      ELSE
        CONCAT_WS(':', CAST(CAST(SUBSTRING(CALL_START_TIME,0,2) as int)-12 AS VARCHAR),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2))
      END
    ELSE
      CASE
        WHEN CAST(SUBSTRING(CALL_START_TIME,0,2) as int) =0 then CONCAT_WS(':','12',SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2))
      ELSE
        CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2))
      END  
  END as am_pm_time,
  CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2)) as start_call_time,
  CONCAT_WS('-', SUBSTRING(call_start_date,0,4),SUBSTRING(call_start_date,5,2),SUBSTRING(call_start_date,7,2)) as start_call_date,
  CONCAT_WS(':', SUBSTRING(call_end_time,0,2),SUBSTRING(call_end_time,3,2),SUBSTRING(call_end_time,5,2)) as end_call_time,
  CONCAT_WS('-', SUBSTRING(call_end_date,0,4),SUBSTRING(call_end_date,5,2),SUBSTRING(call_end_date,7,2)) as end_call_date,
  FORMAT_DATE(CAST(CONCAT_WS('-', SUBSTRING(call_start_date,0,4),SUBSTRING(call_start_date,5,2),SUBSTRING(call_start_date,7,2)) as date), 'E') as weekly_range,
  CONCAT_WS('-',CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),'00'),CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),'59')) as hourly_range,
  CAST((UNIX_TIMESTAMP(CAST(CONCAT_WS('T',
      CONCAT_WS('-', SUBSTRING(call_end_date,0,4),SUBSTRING(call_end_date,5,2),SUBSTRING(call_end_date,7,2)),
      CONCAT_WS(':', SUBSTRING(call_end_time,0,2),SUBSTRING(call_end_time,3,2),SUBSTRING(call_end_time,5,2))) 
      AS TIMESTAMP))-
  UNIX_TIMESTAMP(CAST(CONCAT_WS('T',
    CONCAT_WS('-', SUBSTRING(call_start_date,0,4),SUBSTRING(call_start_date,5,2),SUBSTRING(call_start_date,7,2)),
    CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2))) AS TIMESTAMP))
    ) as DOUBLE)/60000 AS duration,

  CAST(CONCAT_WS('T',
      CONCAT_WS('-', SUBSTRING(call_start_date,0,4),SUBSTRING(call_start_date,5,2),SUBSTRING(call_start_date,7,2)),
      CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2))) AS TIMESTAMP) as start_timestamp,

  CAST(CONCAT_WS('T',
        CONCAT_WS('-', SUBSTRING(call_end_date,0,4),SUBSTRING(call_end_date,5,2),SUBSTRING(call_end_date,7,2)),
        CONCAT_WS(':', SUBSTRING(call_end_time,0,2),SUBSTRING(call_end_time,3,2),SUBSTRING(call_end_time,5,2))) 
        AS TIMESTAMP) as end_timestamp,
  INDEX

FROM BIGINT_to_timestamp
EMIT CHANGES;

----------------------------------------------------------join streams-------------
-- https://docs.ksqldb.io/en/latest/concepts/time-and-windows-in-ksqldb-queries/#windowed-joins
-- https://kafka-tutorials.confluent.io/join-a-stream-to-a-stream/ksql.html
-- dataset table with joins
DROP STREAM if exists call_datasets;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM call_datasets AS
SELECT a.AFTER->INDEX as INDEXa,b.INDEX as INDEXb,a.AFTER->E as Groups,a.AFTER->O as Misses_Call,
a.AFTER->AF as Group_ID,a.AFTER->DQ as USER_ID,a.AFTER->MH as UserDeviceType,b.F as Call_Direction,
b.ER as Features,b.JH as vpDialingfacResult,b.LA as UsageDeviceType,DATE_TIME_COLUMN.* from raw_telecom as a
INNER JOIN combine_all_services as b WITHIN 3 second on a.AFTER->INDEX=b.INDEX
INNER JOIN DATE_TIME_COLUMN  WITHIN 3 second on a.AFTER->INDEX=DATE_TIME_COLUMN.INDEX
EMIT CHANGES;


DROP STREAM if exists service_datasets;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM service_datasets AS
SELECT a.AFTER->INDEX as INDEXa,b.INDEX as INDEXb,a.AFTER->DQ as USER_ID,a.AFTER->AF as Group_ID,
b.ER as FeatureName,a.AFTER->MH as UserDeviceType ,DATE_TIME_COLUMN.* from raw_telecom as a 
INNER JOIN combine_all_services as b WITHIN 3 second on a.AFTER->INDEX=b.INDEX
INNER JOIN DATE_TIME_COLUMN  WITHIN 3 second on a.AFTER->INDEX=DATE_TIME_COLUMN.INDEX
EMIT CHANGES;

DROP STREAM if exists device_dataset;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM device_dataset AS
SELECT a.AFTER->INDEX as INDEXa,b.INDEX as INDEXb,b.F as Call_Direction,a.AFTER->DQ as USER_ID,
a.AFTER->AF as Group_ID,a.AFTER->MH as UserDeviceType,b.LA as UsageDeviceType,
DATE_TIME_COLUMN.* from raw_telecom as a
INNER JOIN combine_all_services as b WITHIN 3 second on a.AFTER->INDEX=b.INDEX
INNER JOIN DATE_TIME_COLUMN  WITHIN 3 second on a.AFTER->INDEX=DATE_TIME_COLUMN.INDEX
EMIT CHANGES;

"""
SELECT a.AFTER->INDEX as INDEXa,b.INDEX as INDEXb, d.INDEX from raw_telecom as a
INNER JOIN REPLACE_SIMPLE_WITH_STANDARD_TERMINOLOGY as b WITHIN 3 second on a.AFTER->INDEX=b.INDEX
INNER JOIN DATE_TIME_COLUMN as d WITHIN 3 second on a.AFTER->INDEX=d.INDEX
EMIT CHANGES;
"""