build:

install:
	python setup.py install

init:
	pip install -r requirements.txt

test:
	python -m tests.test_pyrps

clean:
	rm -rf build pyrps/*.pyc tests/*.pyc

.PHONY: build install init test
