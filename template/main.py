"""Template entry point: ``python main.py`` with PYLINE_SERVER exported.

    # Windows (PowerShell)
    $env:PYLINE_SERVER = "10001"
    $env:PYLINE_TOKEN = "dev-token"
    python main.py

    # Linux
    PYLINE_SERVER=10001 PYLINE_TOKEN=dev-token python main.py
"""

from pyline.runtime import main

if __name__ == "__main__":
    main(["--config", "aioconfig"])
