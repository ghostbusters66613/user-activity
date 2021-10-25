from gcr.io/datamechanics/spark:platform-3.1-dm14

ARG python_reqs_file=requirements.txt
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /app/

COPY requirements.txt ${python_reqs_file} .
RUN pip3 install -r ${python_reqs_file}

COPY . .
