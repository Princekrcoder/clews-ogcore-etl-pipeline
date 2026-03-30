<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=timeGradient&height=200&section=header&text=CLEWS%20%E2%86%94%20OG-Core&fontSize=60&fontAlignY=35&desc=Production%20ETL%20Data%20Pipeline&descAlignY=60&descAlign=50&animation=twinkling" width="100%" />

### 🌐 Integrating Macro-Energy with Economic Modeling

[![Build Status](https://img.shields.io/badge/Build-Passing-2ea44f?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/)
[![Python Version](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Code Style: Black](https://img.shields.io/badge/Code%20Style-Black-000000?style=for-the-badge&logo=prettier&logoColor=white)](https://github.com/psf/black)
[![GSoC Target](https://img.shields.io/badge/GSoC-Target-F2A600?style=for-the-badge&logo=google&logoColor=white)](https://summerofcode.withgoogle.com/)

*A highly configurable, zero-crash data transformation bridge translating raw climate/energy simulations into structured JSON payloads for robust economic analyses.*

[**Explore the Docs**](#) • [**Report Bug**](#-support--feedback) • [**Request Feature**](#-support--feedback)

---

</div>

<br>

## ⚡ Tech Stack

<div align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" />
  <img src="https://img.shields.io/badge/Pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white" />
  <img src="https://img.shields.io/badge/YAML-CB171E?style=for-the-badge&logo=yaml&logoColor=white" />
  <img src="https://img.shields.io/badge/JSON-000000?style=for-the-badge&logo=json&logoColor=white" />
</div>

<br>

<details>
<summary><b>📖 Table of Contents (Click to Expand)</b></summary>

- [🎯 The Vision](#-the-vision)
- [✨ Core Capabilities](#-core-capabilities)
- [🏗️ Pipeline Architecture](#-pipeline-architecture)
- [📂 Directory Structure](#-directory-structure)
- [🚀 Quick Start](#-quick-start)
- [💻 Usage Guide](#-usage-guide)
- [🧪 Testing](#-testing)
- [🤝 Contributing (GSoC)](#-contributing-gsoc)

</details>

---

## 🎯 The Vision

The **CLEWS-OGCore pipeline** solves a critical gap in macroeconomic climate modeling. Legacy pipelines often fail arbitrarily due to data inconsistencies, throwing complex tracebacks. 

This robust ETL system gracefully handles anomalies—dropping out-of-bounds data, executing unit conversions (like MW to GW), running dynamic simulation loops, and rigorously enforcing JSON output schemas. **It is built to fail safely and log descriptively.**

---

## ✨ Core Capabilities

<table>
  <tr>
    <td width="50%">
      <b>⚙️ Config-Driven Execution</b><br>
      Behavior, mappings, and limits are managed exclusively in <code>config.yaml</code>. No hardcoded logic.
    </td>
    <td width="50%">
      <b>🛡️ Graceful Error Handling</b><br>
      Custom strict error hierarchies (<code>DataValidationError</code>, <code>SchemaValidationError</code>) prevent silent failures.
    </td>
  </tr>
  <tr>
    <td width="50%">
      <b>🚧 Quality Fences</b><br>
      Smart validation on <strong>ingress</strong> (CSV rows) and <strong>egress</strong> (JSON schema structures).
    </td>
    <td width="50%">
      <b>🔄 Dynamic Transformation</b><br>
      Processes mathematical text normalizations, conversions, and advanced convergence loops iteratively.
    </td>
  </tr>
</table>

---

## 🏗️ Pipeline Architecture

A secure, sequentially safe data flow from unpolished CSV inputs to perfectly structured JSON egress.

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#2b2b2b', 'edgeLabelBackground':'#111'}}}%%
graph TD
    classDef errorHandle fill:#4a1515,stroke:#ff4444,stroke-width:2px,color:#fff;
    classDef successNode fill:#154a20,stroke:#44ff66,stroke-width:2px,color:#fff;
    classDef processNode fill:#223344,stroke:#4488ff,stroke-width:2px,color:#fff;

    subgraph "1️⃣ Ingress"
        A[📁 Raw CSV Data] -->|Extractor| B(DataFrame)
    end
    
    subgraph "2️⃣ Validation"
        B -->|Validator| C{Quality Limits?}
        C -->|Failed| D[Log Warning & Drop]:::errorHandle
        C -->|Passed| E[Mapper]:::processNode
    end
    
    subgraph "3️⃣ Transformation"
        E -->|Map| F[Transformer]:::processNode
        F -->|Unit Math| G[Clean & Normalise]:::processNode
        G -->|Simulation Loop| H[Pipeline Ready DF]:::processNode
    end
    
    subgraph "4️⃣ Egress"
        H -->|Loader| I{Schema Check?}
        I -->|Invalid| J[Abort Process]:::errorHandle
        I -->|Valid| K[((✅ JSON Payload))]:::successNode
    end
```

---

## 📂 Directory Structure

Clean segregation of configurations, tests, and core execution modules.

```text
clews-ogcore-etl-pipeline/
├── 📄 config.yaml             # 🧠 Master rules engine
├── 🐍 requirements.txt        # 📦 Dependencies
├── 🚀 main.py                 # 🎯 CLI Entry Point
├── 📘 README.md               # 📖 You are here
│
├── 📁 src/                    # Core Modules
│   ├── config.py              # YAML constraints parser
│   ├── exceptions.py          # Custom ETLError routing
│   ├── extractor.py           # Robust CSV chunk parsing
│   ├── validator.py           # Pre-ETL barrier constraints
│   ├── mapper.py              # Column mapping logic
│   ├── transformer.py         # Physics / Simulation engine
│   ├── schema.py              # System exit-point structural typing
│   └── logger.py              # Rotating logging thread
│
├── 📁 data/                   # Mock Datasets
│   └── 📊 sample_input.csv    
│
└── 📁 tests/                  # Integrity Assurance
    ├── test_validator.py
    └── test_transformer.py
```

---

## 🚀 Quick Start

Ensure you have **Python 3.9+** and **Git** active on your system.

```bash
# 1. Clone the repository
git clone https://github.com/Princekrcoder/clews-ogcore-etl-pipeline.git
cd clews-ogcore-etl-pipeline

# 2. Boot up a virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate
# Activate (Mac/Linux)
source venv/bin/activate

# 3. Pull required packages
pip install -r requirements.txt
```

---

## 💻 Usage Guide

The pipeline abstracts its complexity behind a sleek CLI interface. Errors are securely caught and written to `pipeline.log` instead of crashing your terminal.

```bash
python main.py \
  --input data/ \
  --output out/results.json \
  --config config.yaml \
  --log-level INFO
```

### Argument Reference

| Flag | Required | Default | Purpose |
| :--- | :---: | :--- | :--- |
| `--input` | ✅ | `None` | Path to `data.csv` OR a folder directory of multiple CSVs. |
| `--output` | ✅ | `None` | Endpoint `.json` destination. |
| `--config` | ❌ | `config.yaml` | The master YAML logic configuration path. |
| `--log-level`| ❌ | `INFO` | Switch between `DEBUG`, `INFO`, `WARNING`, `ERROR` traces. |

---

## 🧪 Testing

We rely on Pytest for mathematical stability. Run the suite to ensure data integrity limits haven't been compromised.

```bash
# Execute the entire suite
pytest tests/ -v
```

---

## 🤝 Contributing (GSoC)

> **Google Summer of Code contributors are highly welcomed!** 

To make your mark, stick to these standards:

1. **Fork** our codebase.
2. **Branch out** (`git checkout -b feature/EpicEnhancement`).
3. **Commit** changes (`git commit -m 'feat: Add EpicEnhancement'`).
4. **Push** upstream (`git push origin feature/EpicEnhancement`).
5. **Open** a Pull Request against `main`.

**Before opening a PR:**
- Run `black .` to ensure Python formatting symmetry.
- Confirm all local `pytest` modules pass `100%`.

---

<div align="center">
  <b>Architected for the Open Source Climate & Economic Modeling Community.</b><br>
  <i>Built with ✨ Python ✨</i>
</div>
