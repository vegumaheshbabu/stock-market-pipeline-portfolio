# Stock Market Real-Time Data Pipeline 📈

An end-to-end production-grade real-time data pipeline that ingests live stock market data, processes it through a medallion architecture, and visualises it in Power BI — with full CI/CD automation.
---

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | Finnhub API |
| Message Broker | Apache Kafka (Confluent Cloud) |
| Producer | Python on AWS EC2 |
| Processing | Databricks (PySpark + Delta Lake) |
| Storage | Medallion Architecture (Bronze/Silver/Gold) |
| Visualisation | Power BI |
| CI/CD | GitHub Actions |
| Scheduling | Cron (EC2) + Databricks Jobs |
| Version Control | GitHub (private + public) |

---

<img width="707" height="606" alt="image" src="https://github.com/user-attachments/assets/f009847d-5872-420f-8208-7533ae367ba2" />

---

## Medallion Architecture

### Bronze Layer
- Raw data ingested directly from Kafka
- No transformations applied
- Stored as Delta table

### Silver Layer
- Data cleaned and deduplicated
- Price change calculated
- MERGE operation ensures no duplicates
- Stored as Delta table

### Gold Layer
- Daily aggregations per stock symbol
- Metrics: open, close, high, low, avg price
- Ready for Power BI consumption
- MERGE operation for incremental loads

---

## CI/CD Pipeline

Every push to `main` branch automatically:

1. Runs **pytest** tests
2. If tests pass → deploys notebooks to **Databricks**
3. Deploys updated **producer.py** to **AWS EC2**

## Project Structure
stock-market-pipeline/
├── notebooks/
│   ├── producer.py          ← Kafka producer (runs on EC2)
│   ├── Bronze_Notebook.py   ← Raw ingestion from Kafka
│   ├── Silver_Notebook.py   ← Cleaning & transformation
│   └── Gold_Notebook.py     ← Daily aggregations
├── tests/
│   └── test_transformations.py  ← pytest tests
├── .github/
│   └── workflows/
│       └── ci_cd.yml        ← GitHub Actions CI/CD
├── .env.example             ← Required credentials template
└── requirements.txt         ← Python dependencies


---

## Setup Instructions

### Prerequisites
- AWS Account
- Confluent Cloud Account
- Databricks Account
- Finnhub API Key
- Power BI Desktop

### 1. Clone the repo
```bash
git clone https://github.com/vegumaheshbabu/stock-market-pipeline-portfolio.git
cd stock-market-pipeline-portfolio
```

### 2. Set up credentials
```bash
cp .env.example .env
```
Fill in your credentials in `.env`

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run producer locally
```bash
python notebooks/producer.py
```

### 5. Set up GitHub Secrets for CI/CD
| Secret | Description |
|---|---|
| `DATABRICKS_HOST` | Your Databricks workspace URL |
| `DATABRICKS_TOKEN` | Your Databricks access token |
| `EC2_HOST` | Your EC2 public IP |
| `EC2_USERNAME` | `ubuntu` |
| `EC2_KEY` | Your EC2 .pem file contents |
| `PAT_TOKEN` | Your GitHub personal access token |

---

## Stocks Tracked
- **AAPL** — Apple Inc.
- **GOOGL** — Alphabet Inc.
- **MSFT** — Microsoft Corporation

---

## Key Features
- ✅ Real-time stock data ingestion
- ✅ Medallion architecture (Bronze/Silver/Gold)
- ✅ Incremental loads using Delta Lake MERGE
- ✅ Automated CI/CD pipeline
- ✅ Scheduled daily runs
- ✅ Production grade security (private credentials)

---

## Author
**Mahesh Babu Vegu**
Data Engineer
[LinkedIn](https://linkedin.com/in/your-linkedin) | [GitHub](https://github.com/vegumaheshbabu)
www.vegumaheshbabu.com
