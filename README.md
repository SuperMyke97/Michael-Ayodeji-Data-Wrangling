# FastAPI Data Processing API

## Overview
This project is a **FastAPI-based web service** for processing large datasets using **Polars** and **Pandas**. The API provides functionalities to:
- **Load** data from external sources.
- **Clean** and handle missing values.
- **Aggregate** data efficiently.
- **Save results** in JSON or Parquet format.
- **Compare performance** between Polars and Pandas.

## Features
- **FastAPI Endpoint:** A `/process-data` endpoint to load, clean, and aggregate dataset information.
- **Efficient Data Processing:** Uses **Polars** and **Pandas** for performance comparison.
- **Data Cleaning:** Handles missing values, outliers, and formatting errors.
- **File Handling:** Saves results in JSON and Parquet formats.
- **Swagger UI:** Interactive API documentation at `/docs`.
- **Dependency Management:** Uses Poetry for managing Python packages.
- **Automated Testing:** Includes unit tests with Pytest.

## Project Structure
```
data_api/
├── .gitignore
├── .env.example
├── README.md
├── main.py
├── pyproject.toml
├── poetry.lock
├── processor/
│   ├── __init__.py
│   ├── load_data.py
│   ├── clean.py
│   ├── aggregate.py
└── tests/
    ├── __init__.py
    └── test_processor.py
```

## Installation
### Prerequisites
Ensure you have **Python 3.9+** and **Poetry** installed.

### Steps
1. **Clone the repository:**
   ```bash
   git clone https://github.com/Data-Epic/data-wrangling.git
   cd data-wrangling
   ```
2. **Install dependencies:**
   ```bash
   poetry install
   ```
3. **Activate the virtual environment:**
   ```bash
   poetry shell
   ```
4. **Run the FastAPI server:**
   ```bash
   uvicorn main:app --reload
   ```
5. **Access API documentation:**
   Open [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) in your browser.

## Usage
### 1️⃣ **Load Data**
The API can fetch data from **[Online Retail II dataset](http://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx)**.

### 2️⃣ **Data Cleaning**
- Remove null values.
- Fill missing values using **mean** or **median**.
- Handle incorrect formatting.

### 3️⃣ **Aggregation**
- Group data by **country**.
- Compute **total revenue**, **average purchase**, and **total orders**.

### 4️⃣ **API Endpoints**
| Method | Endpoint | Description |
|--------|-----------|-------------|
| **POST** | `/process-data` | Loads, cleans, and aggregates dataset. |
| **GET** | `/download-json` | Download processed data in JSON format. |
| **GET** | `/download-parquet` | Download processed data in Parquet format. |

## Performance Benchmark
The API compares data processing speeds between **Polars** and **Pandas**.
```python
import time

def benchmark():
    start = time.time()
    # Process data with Pandas
    end = time.time()
    print("Pandas time:", end - start)
```

## Testing
Run tests using **Pytest**:
```bash
pytest tests/
```

## Deployment
You can deploy the API using Docker:
```bash
docker build -t fastapi-data-api .
docker run -p 8000:8000 fastapi-data-api
```

## Contribution
Feel free to contribute by creating a **pull request** or reporting issues!

## License
This project is licensed under the **MIT License**.

---
