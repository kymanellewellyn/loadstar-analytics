# LoadStar Analytics

This project implements a data analytics platform for an end-dump trucking company.

The business moves bulk materials using dump trailers. Profit depends on how much revenue each truck generates and how often trucks are inoperable from mechanical issues.

This repository focuses on two core business goals:

- Increase revenue per truck
- Minimize unplanned downtime

The platform ingests operational data, processes event activity related to truck downtime and repairs, and builds reporting tables that support both performance and reliability analysis.

## Architecture Overview

Data is organized using a layered approach:

- **Bronze** – raw ingestion from batch files and simulated event streams
- **Silver** – cleaned and deduplicated data
- **Gold** – reporting tables (dimensions, facts, and aggregated marts)

## Repository Structure

- `ddl/` – SQL table definitions
- `src/` – ingestion and transformation logic
- `docs/` – architecture notes and data modeling documentation
- `data/` – source data
