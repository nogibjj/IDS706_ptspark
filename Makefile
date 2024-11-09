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

deploy:
	git config --local user.email "41898282+github-actions[bot]@users.noreply.github.com"
	git config --local user.name "github-actions[bot]"
	git add -u
	git commit -m "Auto deploy"