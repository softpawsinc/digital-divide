"""Pin Spark's Python worker to the interpreter running pytest.

Without this, Spark falls back to the host `python3`, which may have a
different minor version than the driver (PySpark refuses to run UDFs when
driver and worker versions differ).
"""
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
