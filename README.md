# Environmental Data Warehouse 

This project builds a normalized, query-optimized PostgreSQL warehouse for analyzing environmental indicators, air quality, population density, and regulatory compliance. It demonstrates advanced SQL practices including schema design, constraints, stored procedures, triggers, indexing, and performance benchmarking using `EXPLAIN ANALYZE`.

---

## 🧾 Schema Overview

- **Indicators (`r1`)**: Metadata for environmental measures
- **Geography (`r2`)**: Place names with unique join IDs
- **Time (`r3`)**: Start date and labeled time periods
- **Fact Table (`r4`)**: Core measurements linking time, place, and indicator
- **Supporting Tables**: `air_quality_data`, `urbanization_levels`, `compliance_data`, `city_population_data`

---

## 🔧 Features

- **Relational Normalization**
  - All tables fully normalized (3NF+)
  - Foreign key constraints with cascading updates
- **ENUM & CHECK Constraints**
  - Regulation level, climate zone, emission types as ENUMs
  - Validation for fields like population density
- **Bulk Data Import**
  - Uses `COPY` from CSV for scalable ingestion

---

## 🧠 Procedures & Triggers

- Stored Procedures:
  - `sp_add_indicator`, `sp_update_indicator_info`, `sp_delete_indicator`
- Function:
  - `fn_get_indicator(id)` – fetches a single indicator
- Trigger:
  - Logs every insert into `r1` into an `audit_log` table
  - Demonstrates full rollback in the event of a constraint failure
