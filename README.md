# **ELT proces datasetu CONSUMER_INSTORE__ECOMMERCE_RECEIPT_TRANSACTION_DATA__CPG**

Tento repozitár obsahuje implementáciu ELT procesu v snowflake pre analýzu spotrebiteľského správania v sektore rýchloobrátkového tovaru. Projekt transformuje surové dáta z denormalizovaného formátu do štruktúrovaného dátového skladu typu star schema, čo umožňuje analytiku a vizualizáciu kľúčových obchodných metrík.

---

#**1. Úvod a popis zdrojových dát**

Cieľom projektu je analyzovať transakčné dáta spotrebiteľov, identifikovať trendy v predajoch a porovnať efektivitu rôznych obchodných kanálov.
Zdrojové dáta pochádzajú zo Snowflake Marketplace. Dataset obsahuje jednu rozsiahlu tabuľku s 26 stĺpcami, ktorá zahŕňa informácie o produktoch, značkách, cenách, čase transakcie a lokalite predaja.


#**2. Dátová architektúra**

V počiatočnej fáze sú dáta uložené v "flat table" bez definovaných primárnych kľúčov a relácií.
Hlavné atribúty surových dát:
-	TRANS_DATE – Dátum transakcie
-	PRODUCT_SYMBOL – Identifikátor produktu
-	SPEND_AMOUNT_USD – Suma transakcie
-	MERCHANT_CHANNEL – Kanál predaja
-	GEO – Geografická lokalita
-	... (celkovo 26 atribútov)


#**3. Dimenzionálny model**

Pre potreby analytiky bola navrhnutá star schema. Pozostáva z jednej tabuľky faktov a štyroch dimenzií:
•	fact_sales: Obsahuje kvantitatívne údaje a cudzie kľúče prepojené na dimenzie.
•	dim_date: Obsahuje časové atribúty (deň, mesiac, rok, názov dňa). (SCD typ 0)
•	dim_product: Obsahuje informácie o produktoch, značkách a kategóriách. (SCD typ 1)
•	dim_geo: Geografické údaje o mieste predaja. (SCD typ 1)
•	dim_merchant: Údaje o obchodnom kanáli a subpriemysle. (SCD typ 1)


#**4. ELT proces v snowflake**

Proces spracovania dát bol implementovaný v Snowflake pomocou SQL.


#**5. Extract & load**

Dáta boli extrahované zo snowflake marketplace a načítané do staging tabuľky sales_staging.


#**6. Transform**

V tejto fáze boli vytvorené dimenzionálne tabuľky. Na generovanie unikátnych primárnych kľúčov (Surrogate Keys) bola použitá funkcia row_number().
Oknové funkcie použité vo faktovej tabuľke:
- Denný rebríček predajov: Pomocou funkcie rank() boli transakcie zoradené podľa výšky sumy v rámci každého dňa.
- Kumulatívny obrat: Funkcia sum(...) over(...) počíta priebežný celkový súčet tržieb v čase.

  
#**SQL**
```sql
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    s.spend_amount_usd,
    d.dim_dateid,
    RANK() OVER (PARTITION BY d.dim_dateid ORDER BY s.spend_amount_usd DESC) as sales_rank_daily,
    SUM(s.spend_amount_usd) OVER (ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_cumulative_spend
FROM sales_staging s
JOIN dim_date d ON CAST(s.trans_date as date) = d.date;
```

7. Vizualizácia dát
Dashboard v snowflake obsahuje 6 vizualizácií, ktoré poskytujú komplexný pohľad na predajné metriky.
•	Top 10 product categories: Identifikuje najvýnosnejšie kategórie produktov.
•	Daily sales trend: Sleduje vývoj tržieb v čase a identifikuje sezónne špičky.
•	Basket size vs spend analysis: Vizualizuje vzťah medzi počtom položiek v košíku a celkovou sumou nákupu.
•	Busiest days of the week: Analýza aktivity podľa dní v týždni.
•	Average transaction value: Scorecard s priemernou hodnotou jednej transakcie.
•	Revenue distribution by channel: Porovnanie výkonnosti online predajov voči kamenným predajniam (Instore).

