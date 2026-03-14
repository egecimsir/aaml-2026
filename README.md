# Exercise SS 2026

## Software Environment

You can either use your own laptop or the LRZ cloud for doing the exercises. You are responsible for setting up your software environment. **Python >= 3.10** is required.

All dependencies and their version constraints are defined in [`pyproject.toml`](pyproject.toml).

### Google Colab (recommended for quick start)

* <https://colab.research.google.com/>
* Open this repository in Colab: <https://colab.research.google.com/github/scalable-infrastructure/exercise-2026/>

### LRZ Cloud

* <https://www.lrz.de/services/compute/cloud_en/>

### Local Setup with venv

If you prefer to run the exercises on your local machine, create a virtual environment using Python's built-in `venv` module:

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate   # Linux / macOS
# .venv\Scripts\activate    # Windows

# Install core dependencies
pip install .

# Install all optional dependencies (Spark, Deep Learning, NLP, Quantum)
pip install ".[all]"

```

### Alternative: Local Setup with uv

[uv](https://docs.astral.sh/uv/) is a fast Python package manager written in Rust. If you have `uv` installed, setup is simpler and significantly faster:

```bash
# Create a virtual environment and install all dependencies in one step
uv sync --all-extras

# Run a notebook directly
uv run jupyter notebook
```

