CREATE TABLE LEAP.TEST_50M_HEADER (
	CaseID VARCHAR(12) NOT NULL,
	InvoiceType VARCHAR(20),
	CompanyCode VARCHAR(20),
	DocumentType VARCHAR(40),
	Channel VARCHAR(30),
	Vendor VARCHAR(50),
	NetAmount INT,
	GrossAmount INT,
	CurrencyCode VARCHAR(15),
    PRIMARY KEY(CaseID)    
);