# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ### Delta Live Table (DLT)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Deklerativt rammeverk for å bygge pålitlige, vedlikholdbare, og testbare data prosessering pipelines 
# MAGIC
# MAGIC - Utviklere trenger bare å tenke på transformasjonene man skal gjøre på dataene, og DLT vil ta hånd om oppgave orkestreringen, cluster management, monitorering, data kvalitet, og feilhåndteringer. 
# MAGIC
# MAGIC - I stedenfor å definere en serie av apache spark oppgaver trenger man bare å definere **streaming table** og **materialized views**
# MAGIC
# MAGIC - DLT håndterer hvordan dataen blir transformert basert på spørringer man definerer i de ulike prosesserings stegene. F.eks avhengigheter til andre spørringer
# MAGIC
# MAGIC - Man kan legge på data kvalitets sjekker (expectations) der man kan velge hvordan man håndterer dårlig input
# MAGIC
# MAGIC - Delta Live Tables støtter alle fil formater som støttes av Apache Spark på Databricks
# MAGIC
# MAGIC - DLT bygger videre på funksjonaliteten fra Delta Lake der alle tabeller som DLT håndterer er Delta tabeller med de samme garantiene. (ACID, osv.)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### DLT syntax

# COMMAND ----------


# Ordinær 
(spark
 .read("catalog.schema.tablename")
 .write
 .toTable(table_name)
)

# DLT rammeverket
@dlt.table # dekorator 
def table_name(
  name=table_name
):
  return spark.table("catalog.schema.tablename") 



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Streaming Tables
# MAGIC
# MAGIC - Er en Delta tabell som støtter for streaming eller inkrementell data prosessering
# MAGIC
# MAGIC - Streaming tabeller tillater deg å prosessere et voksende dataset, der en en rad blir posessert en gang
# MAGIC
# MAGIC - Bra valg for de fleste ingestion wordloads
# MAGIC
# MAGIC - Optimal for pipelines som krever lav latency og oppdatert data raskt
# MAGIC
# MAGIC - Kan også være nyttig for enkelte transformeringer  
# MAGIC
# MAGIC - Designet for data kilder som er append only (immutable)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# definerer en streaming tabell som leser fra cloud storage med Auto Loader
@dlt.table(table_name)
def create_streaming_table():
    return spark.readStream.format("cloudFiles")
                .options(**spark_options)
                .load(ingest_path)
    



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Materialized Views
# MAGIC
# MAGIC
# MAGIC - Et materialized view (også kalt live table) er et view der resultatet av spørringer har blir preprosessert 
# MAGIC
# MAGIC - Blir oppdatert basert på skeduleringen til pipelinen den tilhører
# MAGIC
# MAGIC - Kan håndtere alle nye endringer fra inputen. Hvis datakilden endrer seg trenger man bare å oppdatere spørringen 
# MAGIC
# MAGIC - Når man kjører en pipeline update blir alle spørringene rekalkulert for å reflektere endringer i oppstrøms datasettet
# MAGIC
# MAGIC - Materialized views blir lagret som Delta tabeller 
# MAGIC

# COMMAND ----------

# definerer en streaming tabell som leser fra cloud storage med Auto Loader
@dlt.table(table_name)
def create_materialized_view():
    return spark.read(unity_catalog_table)
    


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC
# MAGIC #### Views
# MAGIC
# MAGIC
# MAGIC - Man kan også definere views. DLT publiserer ikke resultatet av spørringene til en katalog (slik som materialized views). Kan bare referrere til views i pipelinen den kjøres i. 
# MAGIC
# MAGIC - Databricks anbefaler å bruke views for å berike dataset som brukes i flere nedstrømstabeller og for å sikre data kaviltet. 
# MAGIC
# MAGIC

# COMMAND ----------

@dlt.view
def view():
  df = dlt.read(unity_catalog_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delta Live Tables pipeline
# MAGIC
# MAGIC - DLT pipeline er hoved enheten for å konfigurere og kjøre arbeidsflyten 
# MAGIC
# MAGIC - En pipeline inneholder streaming tabeller eller materialized views som er definert med python eller sql i filene den kjører. 
# MAGIC
# MAGIC - DLT identifiserer avhengihetene mellom tabellene i kjøringen og sikrer at oppdateringer skjer i riktig rekkefølge. 
# MAGIC
# MAGIC - Oppsettet til DLT pipelines er delt inn i to kategorier
# MAGIC   1. konfigurering av notebooks og filer som skal kjøres
# MAGIC
# MAGIC   2. Konfigurering av infrastruktur, hvordan oppdateringer skal prosesseres (nye rader eller full last), og hvor tabellene skal bli lagret. 
# MAGIC
# MAGIC
# MAGIC - For å gjøre DLT tabellene synlige utenfor pipelinen må man publisere tabellene til Unity Catalog eller Hive metastore
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pipeline updates
# MAGIC
# MAGIC - En oppdatering gjør følgende
# MAGIC
# MAGIC   - Starter en cluster med spesifisert konfigurering
# MAGIC
# MAGIC   - Oppdager alle tabellene og viewene som er definert i filene med dekoderen @dlt.
# MAGIC
# MAGIC   - Analyserer om det er noen feil. (invalid column names, missing dependencies, syntax error)
# MAGIC
# MAGIC   - Lager eller oppdaterer tabellene med den nyeste dataen tilgjengelig
# MAGIC
# MAGIC   - Kan kjøre pipelinen kontinuerlig eller på en tidsplan (trigger)
# MAGIC
# MAGIC     - Avhengig av use case og latency krav. 
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Data innhenting med Delta Live Tables
# MAGIC
# MAGIC - DLT støtter alle datakilder som er tilgjengelig i Azure Databricks
# MAGIC
# MAGIC - Anbefaler å bruke streaming tabeller for de fleste data innhenting jobbene
# MAGIC
# MAGIC - For filer som lander i ADLS2 anbefaler databricks å bruke Auto Loader. 
# MAGIC
# MAGIC - Man kan også hente data direkte med DLT fra andre Structured streaming kilder. F.eks Kafka
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Monitor and enforce data quality
# MAGIC
# MAGIC - Man kan kontrollere data kvaliteten i DLT ved å bruke **expectations** (samme som CHECK)
# MAGIC
# MAGIC - Sørger for at data kvaliteten møter kravene til organisasjoner 
# MAGIC
# MAGIC - Tre typer expectations
# MAGIC
# MAGIC   1. @dlt.expect_all - Registerer alle tilfellene der kondisjonen feiler men gjør ingen ting
# MAGIC
# MAGIC   2. @dlt.expect_all_or_drop - Dropper alle radene der kondisjonen feiler
# MAGIC
# MAGIC   3. @dlt.expect_all_or_fail - Stopper pipeline kjøringen 
# MAGIC   
# MAGIC   
# MAGIC
# MAGIC   

# COMMAND ----------

valid_pages = {
    "valid_count": "count > 0", 
    "valid_current_page": "current_page_id IS NOT NULL"
}

@dlt.table
@dlt.expect_all(valid_pages)
def raw_data():
  # Create raw dataset

@dlt.table
@dlt.expect_all_or_drop(valid_pages)
def prepared_data():
  # Create cleaned and prepared dataset

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 
# MAGIC
# MAGIC
# MAGIC - 
# MAGIC
# MAGIC - 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Mer informasjon
# MAGIC
# MAGIC

# COMMAND ----------


