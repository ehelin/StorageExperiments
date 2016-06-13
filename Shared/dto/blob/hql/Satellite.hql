--1) drop any previous external table to link to blob
DROP TABLE IF EXISTS satellite_data_raw;

----2) create new external table to link to blob
CREATE EXTERNAL TABLE satellite_data_raw 
(satelliteBlobRow string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ''
STORED AS TEXTFILE LOCATION 'wasb://XXXXXXXXXXXXXXXXXXXXXXXXXXXX@XXXXXXXXXXXXXXXXXXXXXXXXXXXX';

--3) create internal table
set hive.output.file.extension = .csv;

DROP TABLE IF EXISTS satellite_data;
CREATE TABLE satellite_data
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES ("serialization.null.format"="")
AS
select get_json_object(b.editedDataRow,'$.Id') as Id,
       get_json_object(b.editedDataRow,'$.Type') as Type,
       get_json_object(b.editedDataRow,'$.Data.onStation') as Data_onStation,
       get_json_object(b.editedDataRow,'$.Data.solarPanelsDeployed') as Data_solarPanelsDeployed,
       get_json_object(b.editedDataRow,'$.Data.fuel') as Data_fuel,
       get_json_object(b.editedDataRow,'$.Data.power') as Data_power,
       get_json_object(b.editedDataRow,'$.Data.PlanetShift') as Data_PlanetShift,
       get_json_object(b.editedDataRow,'$.Data.SatellitePosition.X') as Data_SatellitePosition_X,
       get_json_object(b.editedDataRow,'$.Data.SatellitePosition.Y') as Data_SatellitePosition_Y,
       get_json_object(b.editedDataRow,'$.Data.SourceX') as Data_SourceX,
       get_json_object(b.editedDataRow,'$.Data.SourceY') as Data_SourceY,
       get_json_object(b.editedDataRow,'$.Data.DestinationX') as Data_DestinationX,
       get_json_object(b.editedDataRow,'$.Data.DestinationY') as Data_DestinationY,
       get_json_object(b.editedDataRow,'$.Data.SatelliteName') as Data_SatelliteName,
       get_json_object(b.editedDataRow,'$.Data.AscentDirection') as Data_AscentDirection,
       get_json_object(b.editedDataRow,'$.Created') as Created

FROM (
     select  regexp_replace(
                            regexp_replace(
                                          regexp_replace(a.datarow, '\\\\"','"'),
                                          '\"\\{',
                                          '{'),
                            '\\}\"\,\"Created\"',
                            '},\"Created\"'
                      ) as editedDataRow
     from (
           select substring(satelliteBlobRow, 25, length(satelliteBlobRow) - 22) as datarow
           from satellite_data_raw
      ) a 
) b;

--4) create external table for csv
DROP TABLE IF EXISTS satellite_data_parsed;
CREATE EXTERNAL TABLE satellite_data_parsed
(
    Id STRING,
    Type STRING,
    Data_onStation STRING,
    Data_solarPanelsDeployed STRING,
    Data_fuel STRING,
    Data_power STRING,
    Data_PlanetShift STRING,
    Data_SatellitePosition_X STRING,
    Data_SatellitePosition_Y STRING,
    Data_SourceX STRING,
    Data_SourceY STRING,
    Data_DestinationX STRING,
    Data_DestinationY STRING,
    Data_SatelliteName STRING,
    Data_AscentDirection STRING,
    Created STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE LOCATION 'wasb://XXXXXXXXXXXXXXXXXXXXXXXXXXXX@XXXXXXXXXXXXXXXXXXXXXXXXXXXX';

INSERT INTO TABLE satellite_data_parsed
SELECT * FROM satellite_data;

--get counts
select count(*) as Total_RecordCount 
from satellite_data_parsed;

select count(*) as TotalStatusUpdate_RecordCount 
from satellite_data_parsed
where locate("StatusUpdate", satellite_data_parsed.type) > 0;

select count(*) as TotalStatusUpdate_RecordCount 
from satellite_data_parsed
where locate("ClientUpdate", satellite_data_parsed.type) > 0;

--north
select count(*) as TotalNorthSatellite_RecordCount 
from satellite_data_parsed
where locate("North", satellite_data_parsed.data_satellitename) > 0;

--north east
select count(*) as TotalNorthEastSatellite_RecordCount 
from satellite_data_parsed
where locate("NorthEast", satellite_data_parsed.data_satellitename) > 0;

--north west
select count(*) as TotalNorthWestSatellite_RecordCount 
from satellite_data_parsed
where locate("NorthWest", satellite_data_parsed.data_satellitename) > 0;

--west
select count(*) as TotalWestSatellite_RecordCount 
from satellite_data_parsed
where locate("West", satellite_data_parsed.data_satellitename) > 0;

--east
select count(*) as TotalEastSatellite_RecordCount 
from satellite_data_parsed
where locate("East", satellite_data_parsed.data_satellitename) > 0;

--south
select count(*) as TotalSouthSatellite_RecordCount 
from satellite_data_parsed
where locate("South", satellite_data_parsed.data_satellitename) > 0;

--south east
select count(*) as TotalSouthEastSatellite_RecordCount 
from satellite_data_parsed
where locate("SouthEast", satellite_data_parsed.data_satellitename) > 0;

--south west
select count(*) as TotalSouthWestSatellite_RecordCount 
from satellite_data_parsed
where locate("SouthWest", satellite_data_parsed.data_satellitename) > 0;