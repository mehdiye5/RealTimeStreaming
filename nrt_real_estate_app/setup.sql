-- SETUP --

--brew install mysql
--mysql -uroot
--create database nrt_real_estate
--create user 'user'@'localhost' identified by 'ThePassword';
--grant all on nrt_real_estate.* to 'user'@'localhost';
-- USE nrt_real_estate

-- NOT BEING USED CURRENTLY
-- CREATE TABLE House (
--     tenantId VARCHAR(255),
--     zpid VARCHAR (255),
--     streetAddress VARCHAR(255),
--     zipcode VARCHAR (20),
--     latitude DOUBLE,
--     longitude DOUBLE,
--     garageSpaces INT,
--     hasGarage BOOLEAN,
--     homeType VARCHAR(255),
--     parkingSpaces INT,
--     yearBuilt INT,
--     lotSizeSqFt DOUBLE,
--     livingAreaSqFt DOUBLE,
--     numOfBathrooms INT,
--     numOfBedrooms INT,
--     numOfStories INT
-- );

CREATE TABLE Listing (
    tenantId VARCHAR(255),
    zpid VARCHAR (255),
    streetAddress VARCHAR(255),
    zipcode VARCHAR (20),
    description TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    propertyTaxRate DOUBLE,
    garageSpaces INT,
    hasAssociation BOOLEAN,
    hasCooling BOOLEAN,
    hasGarage BOOLEAN,
    hasSpa BOOLEAN,
    hasView BOOLEAN,
    homeType VARCHAR(255),
    parkingSpaces INT,
    yearBuilt INT,
    latestPrice INTEGER,
    numPriceChanges INTEGER,
    latest_saledate VARCHAR(255),
    latest_salemonth INTEGER,
    latest_saleyear INTEGER,
    latestPriceSource VARCHAR(255),
    numOfPhotos INTEGER,
    numOfAccessibilityFeatures INTEGER,
    numOfAppliances INTEGER,
    numOfParkingFeatures INTEGER,
    numOfPatioAndPorchFeatures INTEGER,
    numOfSecurityFeatures INTEGER,
    numOfWaterfrontFeatures INTEGER,
    numOfWindowFeatures INTEGER,
    numOfCommunityFeatures INTEGER,
    lotSizeSqFt DOUBLE,
    livingAreaSqFt DOUBLE,
    numOfPrimarySchools INTEGER,
    numOfElementarySchools INTEGER,
    numOfMiddleSchools INTEGER,
    numOfHighSchools INTEGER,
    avgSchoolDistance DOUBLE,
    avgSchoolRating DOUBLE,
    avgSchoolSize DOUBLE,
    medianStudentsPerTeacher DOUBLE,
    numOfBathrooms INT,
    numOfBedrooms INT,
    numOfStories INT,
    homeImage VARCHAR(255)
);