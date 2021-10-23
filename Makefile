IMG_NAME = miro-home-task
IMG_NAME_RUN = ${IMG_NAME}:dev
IMG_NAME_TESTS = ${IMG_NAME}:tests

build:
	docker build -t ${IMG_NAME_RUN} .

build-tests:
	docker build -t ${IMG_NAME}:tests --build-arg python_reqs_file=dev-requirements.txt .

run-parsing:
	docker run --mount type=bind,source="$(shell pwd)",target=/app ${IMG_NAME_RUN} driver local:///app/run_pipeline.py parse

run-stat:
	docker run --mount type=bind,source="$(shell pwd)",target=/app ${IMG_NAME_RUN} driver local:///app/run_pipeline.py statistics

run-tests: build-tests
	docker run --mount type=bind,source="$(shell pwd)",target=/app ${IMG_NAME_TESTS} driver local:///app/run_tests.py
