import dagster as dg
import pyalex
import pyalex.api
import requests


class TimeoutHTTPAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, *args, timeout=None, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


def _get_requests_session_with_timeout(timeout: float):
    session = requests.Session()
    retries = requests.packages.urllib3.util.Retry(
        total=pyalex.config.max_retries,
        backoff_factor=pyalex.config.retry_backoff_factor,
        status_forcelist=pyalex.config.retry_http_codes,
        allowed_methods={"GET", "POST"},
    )
    adapter = TimeoutHTTPAdapter(timeout=timeout, max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class OpenAlexResource(dg.ConfigurableResource):
    openalex_key: str
    openalex_email: str
    per_page: int
    request_timeout: float = 30.0
    max_retries: int = 3
    retry_backoff_factor: float = 1.0

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        pyalex.config.api_key = self.openalex_key
        pyalex.config.email = self.openalex_email
        pyalex.config.max_retries = self.max_retries
        pyalex.config.retry_backoff_factor = self.retry_backoff_factor
        pyalex.config.retry_http_codes = [429, 500, 502, 503, 504]
        pyalex.api._get_requests_session = lambda: _get_requests_session_with_timeout(
            self.request_timeout
        )

    def get_author_id(self, author_name: str) -> tuple:
        author = pyalex.Authors().search(author_name).get()
        if author:
            author_id = author[0]["id"]
            return author_id, author
        return None, []

    def get_pages(self, author_id: int, per_page: int):
        if author_id:
            author_params = {"id": author_id}
            pages = pyalex.Works().filter(author=author_params).paginate(per_page=per_page)
            return pages
        return []

    def articles_pages_to_list(self, pages):
        article_list = []
        for page in pages:
            article_list.extend(page)
        return article_list

    def extract_articles(self, author_name: str):
        author_id, _ = self.get_author_id(author_name=author_name)
        pages = self.get_pages(author_id=author_id, per_page=self.per_page)
        articles_list = self.articles_pages_to_list(pages=pages)
        return articles_list
