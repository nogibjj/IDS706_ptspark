all: install format lint test

install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

format:	
	black . --line-length 100 --verbose

lint:
	ruff check ./ --fix --verbose

test:
	python -m pytest -vv

run:
	python main.py
