.PHONY: up down reset topics producer consumer dlq test clean

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v && docker compose up -d

topics:
	python scripts/create_topics.py

producer:
	python -m src.producer.main

consumer:
	python -m src.consumer.main

dlq:
	python scripts/inspect_dlq.py

test:
	pytest

clean:
	rm -rf .pytest_cache __pycache__ */__pycache__ */*/__pycache__
