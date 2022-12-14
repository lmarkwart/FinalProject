DROP database `Police_Killings`;
CREATE DATABASE `Police_Killings` /*!40100 DEFAULT CHARACTER SET latin1 */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE Police_Killings;

CREATE TABLE `Victim`(
`victim_ID` int NOT NULL AUTO_INCREMENT,
`name` varchar(50) DEFAULT NULL,
`age` varchar(50) DEFAULT NULL,
`gender` varchar(50) DEFAULT NULL,
`race` varchar(50) DEFAULT NULL,
PRIMARY KEY(`victim_ID`),
KEY `name` (`name`),
KEY `age`(`age`),
KEY `race` (`race`)
)ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=utf8mb4;


CREATE TABLE `Date`(
`date_ID`int NOT NULL AUTO_INCREMENT,
`month` varchar(50) DEFAULT NULL,
`day` varchar(50) DEFAULT NULL,
`year` varchar(50) DEFAULT NULL,
PRIMARY KEY (`date_ID`)
)ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=utf8mb4;


CREATE TABLE `Location`(
`location_ID`int NOT NULL AUTO_INCREMENT,
`street_address` varchar(50) DEFAULT NULL,
`city` varchar(50) DEFAULT NULL,
`state` varchar(50) DEFAULT NULL,
`latitude` varchar(50) DEFAULT NULL,
`longitude` varchar(50) DEFAULT NULL,
PRIMARY KEY(`location_ID`),
KEY `city`(`city`),
KEY `state` (`state`)
)ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `Incident_facts`(
`incident_ID` int NOT NULL AUTO_INCREMENT,
`law_enforcement_agency` varchar(50) DEFAULT NULL,
`cause`varchar(50) DEFAULT NULL,
`armed` varchar(50) DEFAULT NULL,
PRIMARY KEY (`incident_ID`),
KEY `law_enforcement_agency` (`law_enforcement_agency`),
KEY `armed` (`armed`)
)ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `Population_facts`(
`population_ID` int NOT NULL AUTO_INCREMENT,
`population_size` varchar(50) DEFAULT NULL,
`share_white` varchar(50) DEFAULT NULL,
`share_black` varchar(50) DEFAULT NULL,
`share_hispanic` varchar(50) DEFAULT NULL,
`median_personal_income` varchar(50) DEFAULT NULL,
`median_household_income` varchar(50) DEFAULT NULL,
`poverty_rate` varchar(50) DEFAULT NULL,
`unemployment_rate` varchar(50) DEFAULT NULL,
`college_degree_rate` varchar(50) DEFAULT NULL,
PRIMARY KEY (`population_ID`)
)ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `Fact_summary1` AS
SELECT victim.victim_ID,
victim.name, 
victim.age, 
victim.race, 
location.city, 
location.state
FROM victim
JOIN location
ON victim.victim_ID = location.location_ID;

CREATE TABLE `Fact_summary2` AS
SELECT incident_facts.incident_ID,
incident_facts.law_enforcement_agency,
incident_facts.cause,
incident_facts.armed,
population_facts.share_white,
population_facts.share_black,
population_facts.share_hispanic,
population_facts.poverty_rate,
population_facts.unemployment_rate,
population_facts.college_degree_rate
FROM incident_facts
JOIN population_facts
ON incident_facts.incident_ID = population_facts.population_ID;
DROP TABLE `Fact_table_complete`;
CREATE TABLE `Fact_table_complete` AS
SELECT
Fact_summary2.incident_ID,
Fact_summary1.name,
Fact_summary1.age,
Fact_summary1.race,
Fact_summary1.city,
Fact_summary1.state,
Fact_summary2.law_enforcement_agency,
Fact_summary2.cause,
Fact_summary2.armed,
Fact_summary2.share_white,
Fact_summary2.share_black,
Fact_summary2.share_hispanic,
Fact_summary2.poverty_rate,
Fact_summary2.unemployment_rate,
Fact_summary2.college_degree_rate
FROM Fact_summary1
JOIN Fact_summary2
ON Fact_summary1.victim_ID = Fact_summary2.incident_ID;

CREATE TABLE `Fact_Table` AS
SELECT
Fact_table_complete.incident_ID,
Fact_table_complete.name,
Fact_table_complete.age,
Fact_table_complete.race,
Fact_table_complete.city,
Fact_table_complete.state,
Fact_table_complete.law_enforcement_agency,
Fact_table_complete.cause,
Fact_table_complete.armed,
Fact_table_complete.share_white,
Fact_table_complete.share_black,
Fact_table_complete.share_hispanic,
Fact_table_complete.poverty_rate,
Fact_table_complete.unemployment_rate,
Fact_table_complete.college_degree_rate,
Date.Month,
Date.Year,
Date.Day
FROM Fact_table_complete
JOIN Date
ON Fact_table_complete.incident_ID=Date.Date_ID;

