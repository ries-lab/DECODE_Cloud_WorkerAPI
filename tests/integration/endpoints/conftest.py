import abc
from dataclasses import dataclass, field
from typing import Any

import pytest
from fastapi.testclient import TestClient

from workerfacing_api.dependencies import current_user_dep
from workerfacing_api.main import workerfacing_app


@dataclass
class EndpointParams:
    method: str
    path_params: str = ""
    params: dict[str, str | int] = field(default_factory=dict)


class _TestEndpoint(abc.ABC):
    endpoint = ""

    @abc.abstractmethod
    @pytest.fixture(scope="session")
    def passing_params(self, *args: Any, **kwargs: Any) -> list[EndpointParams]:
        raise NotImplementedError

    @pytest.fixture(scope="session")
    def client(self) -> TestClient:
        return TestClient(workerfacing_app)

    def test_required_auth(
        self,
        monkeypatch: pytest.MonkeyPatch,
        client: TestClient,
        passing_params: list[EndpointParams],
    ) -> None:
        for param in passing_params:
            response = client.request(
                method=param.method,
                url=f"{self.endpoint}/{param.path_params}",
                params=param.params,
            )
            response.raise_for_status()
        monkeypatch.delitem(
            workerfacing_app.dependency_overrides,  # type: ignore
            current_user_dep,
        )
        for param in passing_params:
            response = client.request(
                method=param.method, url=f"{self.endpoint}/{param.path_params}"
            )
            assert response.status_code == 401
