CREATE EXTERNAL TABLE customers (
    customerName STRING,
    email STRING,
    phone STRING,
    birthDay DATE,
    serialNumber STRING,
    registrationDate BIGINT,
    lastUpdateDate BIGINT,
    shareWithResearchAsOfDate BIGINT,
    shareWithPublicAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://udacityhaint/customer/';