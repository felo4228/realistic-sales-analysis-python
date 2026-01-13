


 📊 Project #2 – Realistic Sales Analysis (Python & Pandas)

This project simulates a **real-world sales analysis scenario** in an e-commerce context.  
The goal is to integrate multiple data sources, clean and optimize the data, and apply business-oriented transformations using **Python and Pandas**.

The project was developed as part of the **Python for Data Science** learning path (Epicode).



 🎯 Project Objectives

- Generate realistic synthetic datasets (orders, products, customers)
- Integrate multiple data sources (CSV and JSON)
- Build a **unified DataFrame**
- Optimize memory usage using proper `dtypes`
- Create calculated business columns
- Apply meaningful filters based on business rules
- Prepare clean data for further analysis or reporting



 🗂️ Dataset Structure

 1️⃣ `ordini.csv`
Contains **100,000 orders** with the following fields:
- `OrdineID`
- `ClienteID`
- `ProdottoID`
- `Quantità`
- `DataOrdine`

---

2️⃣ `prodotti.json`
Contains **20 products** with:
- `ProdottoID`
- `NomeProdotto`
- `Categoria`
- `Fornitore`
- `Prezzo`

---

 3️⃣ `clienti.csv`
Contains **5,000 customers** with:
- `ClienteID`
- `Regione`
- `Segmento`



 🔗 Data Integration

The datasets are merged into a single DataFrame using:
- `merge()` on `ProdottoID`
- `merge()` on `ClienteID`

This results in a **complete dataset** combining orders, product details, and customer information.



⚙️ Memory Optimization

The project demonstrates how to significantly reduce memory usage by:

- Downcasting integer types (`int64 → int32`)
- Downcasting float types (`float64 → float32`)
- Converting repetitive string columns to `category`

📉 **Memory usage reduced from ~36 MB to ~2.4 MB**



 🧮 Calculated Columns

A new column is created:

```text
ValoreTotale = Prezzo × Quantità
````

This represents the total monetary value of each order.



 🔍 Business Filter 

Orders are filtered according to the following rule:

- `ValoreTotale > 100`-
-  Valid customer (`ClienteID` not null)

This type of filtering is commonly used to:

- Identify high-value orders
- Focus on relevant transactions
- Support sales and customer analysis



 🧠 Note on Exploratory Analysis

The codebase includes "commented and optional structures" for exploratory analysis
(e.g. `describe()`), but they are "not executed automatically", in order to strictly
respect the exercise requirements.

In a real business scenario, exploratory analysis would normally be performed.



 🛠️ Technologies Used

* Python 3
* Pandas
* NumPy
* VS Code













