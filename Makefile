setup:
	pip install argparse tornado sqlalchemy mock boto moto --use-mirrors

test:
	nosetests

.PHONY: test