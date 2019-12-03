echo pytest
pytest tests.py --cov-config .coveragerc --cov models --cov-report term-missing --cov-fail-under=85 -v
