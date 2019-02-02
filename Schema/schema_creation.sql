CREATE TABLE SPYDIVI(
	TRADEDATE DATE NULL,
	DIVIAMOUNT DECIMAL(12, 6) NULL
)

CREATE TABLE SPYPRICING(
	TRADEDATE DATE NULL,
	OPENED DECIMAL(12, 6) NULL,
	HIGH DECIMAL(12, 6) NULL,
	LOW DECIMAL(12, 6) NULL,
	CLOSED DECIMAL(12, 6) NULL,
	VOLUME BIGINT NULL,
	TIMESTEP INT NULL
)

CREATE TABLE SPYKEYWORDS (
	date_value TIMESTAMP NULL,
	Trump INT NULL,
	Presidend INT NULL,
	Debt INT NULL,
	Loan INT NULL,
	Mortgage INT NULL,
	Utilities INT NULL,
	Dow_Jones INT NULL,
	Stock_market INT NULL,
	Trading INT NULL,
	Dog INT NULL,
	Spy INT NULL,
	S&P INT NULL,
	Economy INT NULL,
	Election INT NULL,
	Apple INT NULL,
	Politics INT NULL,
	Unemployment INT NULL,
	Interest_rates INT NULL,
	Fed_funds_rate INT NULL
)

CREATE TABLE SPYPRICING_IEX(
	TRADEDATE DATE NULL,
	RECEIVETIME TIMESTAMP NULL,
	HIGH DECIMAL(12, 6) NULL,
	LOW DECIMAL(12, 6) NULL,
	AVERAGE DECIMAL(12, 6) NULL,
	VOLUME INT NULL,
	NOTIONAL INT NULL,
	NUMBEROFTRADES INT NULL,
	MARKETHIGH DECIMAL(12, 6) NULL,
	MARKETLOW DECIMAL(12, 6) NULL,
	MARKETAVERAGE DECIMAL(12, 6) NULL,
	MARKETVOLUME INT NULL,
	MARKETNUMBEROFTRADES INT NULL,
	OPEN DECIMAL(12, 6) NULL,
	CLOSE DECIMAL(12, 6) NULL,
	MARKETOPEN DECIMAL(12, 6) NULL,
	MARKETCLOSE DECIMAL(12, 6) NULL,
	CHANGEOVERTIME DECIMAL(12, 6) NULL,
	MARKETCHANGEOVERTIME DECIMAL(12, 6) NULL
)
