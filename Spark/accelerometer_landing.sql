CREATE EXTERNAL TABLE accelerometer_landing (
    timeStamp STRING,
    user STRING,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://udacityhaint/accelerometer/';