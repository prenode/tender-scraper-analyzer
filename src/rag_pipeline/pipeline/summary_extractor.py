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
from haystack.components.rankers import LostInTheMiddleRanker
from haystack_integrations.document_stores.chroma import ChromaDocumentStore
from haystack_integrations.components.retrievers.chroma import ChromaEmbeddingRetriever
from haystack.dataclasses.byte_stream import ByteStream

from pathlib import Path
from dotenv import load_dotenv
import io


class RAGPipeline:
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

    def __init__(self, hf_api_key: str, llm_id: str, embedding_model_id: str):
        self.document_store = ChromaDocumentStore(host="localhost", port="8000")
        self.llm_id = llm_id
        self.embedding_model_id = embedding_model_id
        self.indexing_pipeline, self.query_pipeline = self._setup_pipelines(hf_api_key)

    def _setup_pipelines(self, api_key):
        prompt_template = """
        Write a high-quality answer for the given question using only the provided search results (some of which might be irrelevant). Your answer must be written in german!
        Documents:
        {% for doc in documents %}
            {{ doc.content }}
        {% endfor %}
        Question: {{question}}
        Answer:
        """

        hf_llm = HuggingFaceAPIGenerator(
            api_type="serverless_inference_api",
            api_params={"model": self.llm_id},
            token=Secret.from_token(api_key),
        )

        indexing_pipeline = Pipeline()
        indexing_pipeline.add_component("converter", PyPDFToDocument())
        indexing_pipeline.add_component("cleaner", DocumentCleaner())
        indexing_pipeline.add_component(
            "splitter", DocumentSplitter(split_by="period", split_length=6)
        )
        indexing_pipeline.add_component(
            "document_embedder",
            HuggingFaceAPIDocumentEmbedder(
                api_type="serverless_inference_api",
                api_params={"model": self.embedding_model_id},
                token=Secret.from_token(api_key),
            ),
        )
        indexing_pipeline.add_component(
            "writer", DocumentWriter(document_store=self.document_store)
        )

        indexing_pipeline.connect("converter", "cleaner")
        indexing_pipeline.connect("cleaner", "splitter")
        indexing_pipeline.connect("splitter", "document_embedder")
        indexing_pipeline.connect("document_embedder", "writer")

        query_pipeline = Pipeline()
        query_pipeline.add_component(
            "text_embedder",
            HuggingFaceAPITextEmbedder(
                api_type="serverless_inference_api",
                api_params={"model": self.embedding_model_id},
                token=Secret.from_token(api_key),
            ),
        )
        query_pipeline.add_component(
            "retriever", ChromaEmbeddingRetriever(document_store=self.document_store)
        )
        query_pipeline.add_component(
            "ranker", LostInTheMiddleRanker(word_count_threshold=1024)
        )
        query_pipeline.add_component(
            "prompt_builder", PromptBuilder(template=prompt_template)
        )
        query_pipeline.add_component("llm", hf_llm)

        query_pipeline.connect("text_embedder", "retriever")
        query_pipeline.connect("retriever", "ranker")
        query_pipeline.connect("ranker", "prompt_builder")
        query_pipeline.connect("prompt_builder", "llm")

        return indexing_pipeline, query_pipeline

    def create_summary(self, pdf_data: bytes, question: str) -> str:
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

        results = self.query_pipeline.run(
            {
                "text_embedder": {"text": question},
                "prompt_builder": {"question": question},
            },
            include_outputs_from=["llm", "prompt_builder"],
        )
        return results["llm"]["replies"][0]

    def init_pipeline(self, file_paths, tender_id) -> str:
        """
        Creates a detailed description based on the content of the given file.
        This method processes the file specified by `file_path` through an indexing pipeline
        and then queries the processed data to generate a detailed description.
        Args:
            file_path (str): The path to the file to be processed.
        Returns:
            str: A detailed description generated from the file content.
        """
        # convert all files to documents
        while True:
            try:
                self.indexing_pipeline.run(
                    {
                        "converter": {"sources": list(file_paths)},
                        "writer": {"meta": {"id": tender_id}},
                    },
                )

                break
            except Exception as e:
                print(f"Error running pipeline: {e}. Retrying...")

    def answer_question(self, question) -> str:
        results = self.query_pipeline.run(
            {
                "text_embedder": {"text": question},
                "prompt_builder": {"question": question},
            },
            include_outputs_from=["llm", "prompt_builder"],
        )
        return results["llm"]["replies"][0]
