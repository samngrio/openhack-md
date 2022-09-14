-- Databricks notebook source
describe extended star.dimactors

-- COMMAND ----------

describe extended silver.catalog

-- COMMAND ----------

select distinct ActorId, Actor as 'ActorName', actorgender from silver.catalog
