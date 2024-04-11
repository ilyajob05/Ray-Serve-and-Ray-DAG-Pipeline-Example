import time

import ray
from fastapi import FastAPI
from ray import remote
from ray import serve
from ray.dag.input_node import InputNode


@remote
class TextProcessor:
    def __init__(self, tail: str = "#"):
        self.tail = tail

    def processing(self, data):
        for i in range(10):
            print(f"processing: {i}")
            time.sleep(0.3)
        return data + self.tail


@remote
class TextClassifier:
    def __init__(self):
        self.count = 0

    def classify(self, text) -> int:
        self.count += 1
        for i in range(10):
            print(f"classifier: {text} {i}")
            time.sleep(0.3)
        return self.count


@ray.remote
def read_data(s: str):
    for i in range(10):
        print(f"{s}_read_data: {i}")
        time.sleep(1)
    return "test"


@ray.remote
def combine(a, b):
    for i in range(10):
        print(f"combine: {i} {a} {b}")
        time.sleep(0.3)
    return a + b


# define the nodes
text_classifier = TextClassifier.remote()
text_processor = TextProcessor.remote("_tail_")


# define the DAG
with InputNode() as dag_input:
    rd1 = read_data.bind(dag_input)
    rd2 = read_data.bind(dag_input)
    rd = combine.bind(rd1, rd2)
    dag = text_classifier.classify.bind(text_processor.processing.bind(rd))


# define fastapi server
app = FastAPI()


@serve.deployment
@serve.ingress(app)
class FastAPIDeployment:
    @app.get("/{subpath}")
    def root(self, subpath: str):
        answer = ray.get(dag.execute(subpath))
        return {"text_class": answer}

    @app.get("/")
    def root(self):
        return "Hello, world!"


fast_api_deployment = FastAPIDeployment.bind()
