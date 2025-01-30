from haystack import Pipeline, Document
from haystack.utils import Secret
from haystack.document_stores.in_memory import InMemoryDocumentStore
from haystack.components.retrievers.in_memory import (
    InMemoryBM25Retriever,
    InMemoryEmbeddingRetriever,
)
from haystack.components.builders.prompt_builder import PromptBuilder
from haystack.components.generators import HuggingFaceAPIGenerator
from haystack.components.converters import PyPDFToDocument
from haystack.components.preprocessors import DocumentCleaner
from haystack.components.preprocessors import DocumentSplitter
from haystack.components.writers import DocumentWriter
from haystack.components.embedders import (
    SentenceTransformersTextEmbedder,
    SentenceTransformersDocumentEmbedder, 
    HuggingFaceAPITextEmbedder,
    HuggingFaceAPIDocumentEmbedder,
)
from haystack.dataclasses.byte_stream import ByteStream

from pathlib import Path
from dotenv import load_dotenv
import io

class SummaryExtractor:
    """
    A class to extract summaries from PDF documents using a pipeline of components.
    Attributes:
        indexing_pipeline (Pipeline): The pipeline used for indexing documents.
        query_pipeline (Pipeline): The pipeline used for querying and generating summaries.
    Methods:
        __init__(hf_api_key: str):
            Initializes the SummaryExtractor with the provided Hugging Face API key.
        _setup_pipelines(api_key: str) -> Tuple[Pipeline, Pipeline]:
            Sets up the indexing and query pipelines using the provided API key.
        create_summary(pdf_data: bytes) -> str:
            Creates a summary from the provided PDF data.
    """
    def __init__(self, hf_api_key: str):
        self.indexing_pipeline, self.query_pipeline = self._setup_pipelines(hf_api_key)

    def _setup_pipelines(self, api_key):
        prompt_template = """
        Given these documents, answer the question.
        Documents:
        {% for doc in documents %}
            {{ doc.content }}
        {% endfor %}
        Question: {{question}}
        Answer:
        """
        document_store = InMemoryDocumentStore()
        hf_llm = HuggingFaceAPIGenerator(
            api_type="serverless_inference_api",
            api_params={"model": "mistralai/Mistral-7B-Instruct-v0.3"},
            token=Secret.from_token(api_key),
        )

        indexing_pipeline = Pipeline()
        indexing_pipeline.add_component("converter", PyPDFToDocument())
        indexing_pipeline.add_component("cleaner", DocumentCleaner())
        indexing_pipeline.add_component(
            "splitter", DocumentSplitter(split_by="sentence", split_length=5)
        )
        indexing_pipeline.add_component(
            "document_embedder", HuggingFaceAPIDocumentEmbedder(api_type="serverless_inference_api",
                                              api_params={"model": "BAAI/bge-small-en-v1.5"},
                                              token=Secret.from_token(api_key))
        )
        indexing_pipeline.add_component(
            "writer", DocumentWriter(document_store=document_store)
        )

        indexing_pipeline.connect("converter", "cleaner")
        indexing_pipeline.connect("cleaner", "splitter")
        indexing_pipeline.connect("splitter", "document_embedder")
        indexing_pipeline.connect("document_embedder", "writer")

        query_pipeline = Pipeline()
        query_pipeline.add_component(
            "text_embedder", HuggingFaceAPITextEmbedder(api_type="serverless_inference_api",
                                              api_params={"model": "BAAI/bge-small-en-v1.5"},
                                              token=Secret.from_token(api_key))
        )
        query_pipeline.add_component(
            "retriever", InMemoryEmbeddingRetriever(document_store=document_store)
        )
        query_pipeline.add_component(
            "prompt_builder", PromptBuilder(template=prompt_template)
        )
        query_pipeline.add_component("llm", hf_llm)

        query_pipeline.connect("text_embedder", "retriever")
        query_pipeline.connect("retriever", "prompt_builder")
        query_pipeline.connect("prompt_builder", "llm")

        return indexing_pipeline, query_pipeline

    def create_summary(self, pdf_data: bytes) -> str:
        """
        Creates a summary of the given PDF data.
        This method processes the provided PDF data using an indexing pipeline and
        then generates a summary based on a predefined set of questions.
        Args:
            pdf_data (bytes): The PDF data to be summarized.
        Returns:
            str: The generated summary of the PDF data.
        """
        results = self.indexing_pipeline.run(
            {"converter": {"sources": [ByteStream(data=pdf_data)]}},
        )

        question = "Bitte fasse zusammen: 1. Wie Angebote eingereicht werden dürfen 2. Verfahrensart 3. Die Art und den Umfang der Leistung 4. Die Ausführungsfrist/die Länge des Auftrags 5. Sonstiges"
        results = self.query_pipeline.run(
            {
                "text_embedder": {"text": question},
                "prompt_builder": {"question": question},
            },
            include_outputs_from=["llm", "prompt_builder"], 
        )
        print(results["prompt_builder"]["prompt"])
        return results["llm"]["replies"][0]
