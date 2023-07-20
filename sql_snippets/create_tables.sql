CREATE TABLE property
(
[prop_id] INT NOT NULL,
[address] VARCHAR(30) NOT NULL,
[postcode] VARCHAR(10),
[latitude] DECIMAL(10,8) NOT NULL,
[longitude] DECIMAL(11,8) NOT NULL,
CONSTRAINT property_pk PRIMARY KEY (prop_id)
);

CREATE TABLE price
(
[prop_id] INT NOT NULL,
[date] DATE NOT NULL,
[price] INT,
CONSTRAINT price_pk PRIMARY KEY ([prop_id], [date]),
CONSTRAINT price_property_fk FOREIGN KEY (prop_id) REFERENCES dbo.property (prop_id)
);

CREATE TABLE interest_rates
(
[date] DATE NOT NULL,
[IUMSOIA] DECIMAL(6,4) NOT NULL,
CONSTRAINT interest_rates_pk PRIMARY KEY ([date])
);
