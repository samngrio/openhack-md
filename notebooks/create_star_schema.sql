-- Databricks notebook source
create database star;

-- COMMAND ----------

use star;

-- COMMAND ----------

create table DimActors(ActorSK int, ActorID string, ActorName string, ActorGender string)

-- COMMAND ----------

create table DimCategories(MovieCategorySK tinyint, MovieCategoryDescription string)

-- COMMAND ----------

create table DimCustomers (
  CustomerSK int not null,
  CustomerID string not null,
  LastName string not null,
  FirstName string not null,
  AddressLine1 string not null,
  AddressLine2 string,
  City string not null,
  State string not null,
  ZipCode string not null,
  PhoneNumber string not null,
  RecordStartDate date not null,
  RecordEndDate date,
  ActiveFlag boolean not null)

-- COMMAND ----------

create table DimDate (
  DateSK int not null,
  DateValue	date not null,
  DateYear smallint not null,
  DateMonth	int not null,
  DateDay tinyint not null,
  DateDayOfWeek	tinyint not null,
  DateDayOfYear	smallint not null,
  DateWeekOfYear tinyint not null
)

-- COMMAND ----------

create table DimLocations (
  LocationSK smallint not null,
  LocationName string not null,
  Streaming	boolean	not null,
  Rentals boolean not null,
  Salesb boolean not null
)

-- COMMAND ----------

create table DimMovieActors (
  MovieID string not null,
  ActorID string not null
)

-- COMMAND ----------

create table DimMovies(
  MovieSK	int not null,
  MovieID	string not null,
  MovieTitle string not null,
  MovieCategorySK	tinyint  not null,
  MovieRatingSK tinyint not null,
  MovieRunTimeMin	smallint  not null
)

-- COMMAND ----------

create table DimRatings(
  MovieRatingSK tinyint not null,
  MovieRatingDescription string not null
)

-- COMMAND ----------

create table DimTime (
  TimeSK	int	not null,
  TimeValue	timestamp not null,
  TimeHour	tinyint	not null,
  TimeMinute	tinyint	not null,
  TimeSecond	tinyint	not null,
  TimeMinuteOfDay	smallint	not null,
  TimeSecondOfDay	int	not null
)

-- COMMAND ----------

create table FactRentals (
  RentalSK	int	not null,
  TransactionID	string	not null,
  CustomerSK	int	not null,
  LocationSK	smallint	not null,
  MovieSK	int	not null,
  RentalDateSK	int	not null,
  ReturnDateSK	int,
  RentalDuration	tinyint,
  RentalCost	string	not null,
  LateFee	string,
  TotalCost	string,
  RewindFlag	boolean
)

-- COMMAND ----------

create table FactSales (
  SalesSK	int	not null,
  OrderID	string	not null,
  LineNumber	tinyint	not null,
  OrderDateSK	int	not null,
  ShipDateSK	int	,
  CustomerSK	int	not null,
  LocationSK	smallint	not null,
  MovieSK	int	not null,
  DaysToShip	tinyint	,
  Quantity	tinyint	not null,
  UnitCost	string	not null,
  ExtendedCost	string	not null
)

-- COMMAND ----------

create table FactStreaming (
  StreamingSK	int	not null,
  TransactionID	string	not null,
  CustomerSK	int	not null,
  MovieSK	int	not null,
  StreamStartDateSK	int	not null,
  StreamStartTimeSK	int	not null,
  StreamEndDateSK	int	,
  StreamEndTimeSK	int	,
  StreamDurationSec	int	,
  StreamDurationMin	decimal
)
