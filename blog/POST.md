## Data quality on Databricks

### Introduction

### Definition of Data Quality
The definition of Data Quality given in many resources, can be summarised measurement of how well the data represents
real-world facts. Although this definition gives a good sense of what is meant by Data Quality, this is not detailed
enough to be used for any implementation. So let's review a more detailed definition by public standards and professional  literature.

#### ISO-25000
ISO-25000 is a series of international standards describing the evaluation and management of the quality of informational systems. 
ISO-25012 and ISO-25024 are two standards from the series that have a primary focus is data quality.  

ISO-25012 defines a high-level model for data quality. It defines two main categories of data quality characteristics:
- "Inherent Data Quality" - a set of characteristics related to the data per se.
- "System-Dependent Data Quality" - a set of characteristics related to data serving implementation.

The standard specifies the list of characteristics and corresponding "data quality properties" for each characteristic can be measured by for the "Inherent Data Quality" category:

- **Accuracy** - reflects how well data represents real world facts or measurements.
For example: proportion of correct email addresses.
Data quality properties: "Syntactic Accuracy", "Semantic Accuracy", "Accuracy range";

- **Completeness** - how many of the expected entity properties are present. 
For example: proportion of empty or null values.
Data quality properties: "Record Completeness", "File completeness", "Data Value Completeness", "False Completeness Of File", etc.

- **Consistency** - how much data is coherent.
For example: proportion of denormalized data that is inconsistent across tables.
Data quality properties:  "Referential Integrity", "Risk Of Inconsistency", "Semantic Consistency", "Format Consistency".

- **Credibility** - how much we can be trusted. For example: proportion of geo-addresses confirmed via external sources like OSM.
Data quality properties: "Data values Credibility", "Source Credibility";

- **Currentness** - how fresh the data is.
For example: proportion of records upserted in the past 24 hours.   
Data quality properties: "Update Frequency", "Timelines of Update";

#### DAMA-DMBOK
["Data Management Body of Knowledge"](https://dama.org/learning-resources/dama-data-management-body-of-knowledge-dmbok/) is one of the important books in the field of Data Management 
giving valuable recommendations, in particular covering the subject of Data Quality (Chapter 16).
The book reveals a subject of Data Quality from the perspective of the following dimensions:

- **Validity** - whether values are correct from a domain knowledge perspective.
For example: proportion of numerical records within the expected range for an age in a socio-demographic data-set.

- **Completeness** - whether the required data is present. 
For example: proportion of absent or blank values for required columns in a data set.

- **Consistency** - whether data is encoded the same way all over a place.
For example: proportion of records complying same address format between tables in the CRM dataset.

- **Integrity** - whether references across the data sets are consistent.
For example, the proportion of records with broken foreign keys between the department and employee in the data set of the organization structure. 

- **Timeliness** - whether data is updated within the expected time range.
For example, the data set reflecting sales quarter results should be completely updated within two weeks.   

- **Currency** - whether the data is fresh enough and reflects the current state of the world.
For example: proportion of records that were inserted not more than 1 hour ago in the data set of IoT devices readings.  

- **Reasonableness** - whether high-level value patterns are close to expectations. 
For example, house prices are close to a normal distribution in the real estate dataset. 

- **Uniqueness / Deduplication** - whether duplicated records are present in the dataset.
For example: proportion of lead contacts records in the CRM dataset. 

- **Accuracy** - whether records in the data-set accurately reflect real-world facts or knowledge.
For example: proportion of customer address records that were verified using trusted external data sources such
as data from government bodies like the Chamber of Commerce or the Ministry of Trade.

#### Summerized metrics
Aforementioned Data Quality Dimensions (DQD*) could be summarized in the following list:

* Accuracy (IS0) & Validity (DMBOK) - Whether data comply syntactic and semantic rules.
* Completeness (ISO & DMBOK) -Whether expected data is present in full form. 
* Consistency (ISO) and Integrity (DMBOK) - Whether components of data set are coherent between each other.
* Credibility (ISO) & Accuracy (DMBOK) - Whether data can be trusted and reflects correctly state of real world.
* Currentnes (ISO) & Currency (DMBOK) - Whether data is fresh enough and reflect recent state of real world.
* Timeliness (DMBOK)
* Reasonableness (DMBOK)
* Uniqueness (DMBOK)

Although Data Quality definitions above are proposed by reputable bodies, it is worth mentioning that final implementation of Data Quality measurements and their importance are highly depend on the specific use case.

### Methodology
In order to fairly compare and evaluate each Data Quality framework in this series, they will be tested under the same conditions.
We will check the quality of [Airline](https://relational.fel.cvut.cz/dataset/Airline) data-set from "CTU Relational Learning Repository" repository.
This data set represents flight data in the US for 2016, consisting of the main `On_Time_On_Time_Performance_2016_1` table and several dimensions. 
The data set is not too complex, represents real-world data, and is slightly messy, which is a good combination for the case study.
The goal is to measure this data set quality for each dimension with corresponding metrics. 
Surely the list of measured metrics can be extended a lot more, but for the sake of brevity, let's keep it short, having 1-3 metrics per category:

#### Accuracy & Validity
- All values of `TailNum` column are valid "tail number" combinations (see [Aircraft registration](https://en.wikipedia.org/wiki/Aircraft_registration))
- All values in column `OriginState` contain valid state abbreviations (see [States Abbreviations](https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/appendix_a.html))
- All rows have `ActualElapsedTime` that is more than `AirTime`. 

#### Completeness
- All values in columns `FlightDate`, `AirlineID`, `TailNum` are not null. 

#### Consistency & Integrity
- All values in column `AirlineID` match `Code` in `L_AIRLINE_ID` table, etc.

#### Credibility / Accuracy
- At least 80% of `TailNum` column values can be found in [Federal Aviation Agency Database](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download)

#### Currentness / Currency
- All values in column `FlightDate` are not older than 2016.

#### Reasonableness
- Average speed calculated based on `AirTime` (in minutes) and `Distance` is close to the average cruise speed of modern aircraft - 885 KpH.
- 90th percentile of `DepDelay` is under 60 minutes; 

#### Uniqueness
- Proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID`, and `DestAirportID` is less than 10%.


### References
Bellow is the list of other sources used to write this post:
- https://iso25000.com/index.php/en/iso-25000-standards/iso-25012
- https://arxiv.org/pdf/2102.11527
- https://medium.com/the-thoughtful-engineer/part-5-iso-25012-in-action-what-data-quality-really-means-5e334b828595