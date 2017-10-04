CREATE TABLE LEAP.TEST_50M (
	CaseID VARCHAR(12) NOT NULL,
	ActivityName VARCHAR(80), 
	StartDateTime DATETIME NOT NULL, 
    EndDateTime DATETIME NOT NULL,
	Discrepancy VARCHAR(80), 
    PreviousActivityName VARCHAR(80), 
	PreviousActivityEndTime DATETIME,
    PRIMARY KEY(CaseID,ActivityName,StartDateTime)
);