"""F-54: metrics endpoint -- default loopback bind and optional bearer token."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

import pytest

from pyline.runtime_wiring import start_metrics_endpoint


def scrape(url: str, token: str | None = None) -> tuple[int, str]:
    request = urllib.request.Request(url)
    if token is not None:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, ""


@pytest.mark.integration
class TestMetricsEndpointF54:
    def test_token_required_when_configured(self) -> None:
        server = start_metrics_endpoint("127.0.0.1", 0, "s3cret-token")
        try:
            port = server.server_address[1]
            url = f"http://127.0.0.1:{port}/metrics"

            status, _ = scrape(url)  # no credentials: rejected
            assert status == 401
            status, _ = scrape(url, "wrong-token")  # wrong token: rejected
            assert status == 401
            status, body = scrape(url, "s3cret-token")  # right token: served
            assert status == 200
            assert "python_info" in body or "pyline" in body
        finally:
            server.shutdown()
            server.server_close()

    def test_non_ascii_token_answered_401_not_500(self) -> None:
        """hmac.compare_digest raises TypeError when either str operand is
        non-ASCII: a non-ASCII token used to crash the handler (500/close)
        on every scrape instead of answering 401."""
        server = start_metrics_endpoint("127.0.0.1", 0, "métrics-tökén")
        try:
            port = server.server_address[1]
            status, _ = scrape(f"http://127.0.0.1:{port}/metrics")  # ASCII-only header
            assert status == 401
        finally:
            server.shutdown()
            server.server_close()

    def test_no_token_serves_openly(self) -> None:
        server = start_metrics_endpoint("127.0.0.1", 0, None)
        try:
            port = server.server_address[1]
            status, _ = scrape(f"http://127.0.0.1:{port}/metrics")
            assert status == 200
        finally:
            server.shutdown()
            server.server_close()


class TestMetricsConfig:
    def test_defaults_are_safe(self) -> None:
        import json5

        from pyline.config.models import ProjectSettings

        raw = {
            "project": "p",
            "srv_type": "develop",
            "socket": {"token": "$plain:x", "client_port": 1, "server_port": 2},
            "mysql": {"user": "u", "password": "$plain:y", "db_name": "d"},
            "redis": {},
        }
        settings = ProjectSettings.model_validate(json5.loads(json.dumps(raw)))
        assert settings.metrics_bind == "127.0.0.1"
        assert settings.metrics_token is None

    def test_metrics_token_is_a_secret_reference(self) -> None:
        """metrics_token rides the SecretStr machinery: the resolved value is
        masked in repr and the loader accepts only references."""
        from pyline.config.loader import _secret_paths
        from pyline.config.models import ProjectSettings

        paths = list(_secret_paths(ProjectSettings))
        assert ("metrics_token",) in paths


class TestMetricsLifecycleF99:
    async def test_devtools_layer_stops_metrics_server(self, config_dir) -> None:
        """F-99: the ThreadingHTTPServer used to be dropped on the floor --
        never closed, holding its port until process death. The devtools
        layer keeps the reference and closes it on stop_metrics."""
        import json
        import socket

        from pyline.core.events import EventBus
        from pyline.obs.metrics import AlarmHub
        from pyline.runtime import build_context
        from pyline.runtime_wiring import DevtoolsLayer

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        project = {
            "project": "metrics-test",
            "srv_type": "production",  # no console/watcher in this test
            "metrics_port": port,
            "socket": {"token": "$plain:x", "client_port": 1, "server_port": 2},
            "mysql": {"user": "u", "password": "$plain:y", "db_name": "d"},
            "redis": {},
        }
        (config_dir / "project.json5").write_text(json.dumps(project), encoding="utf-8")
        ctx = build_context(config_dir, 10001, "main", 0, 0)

        async def reload_hook(module: str) -> None:  # pragma: no cover - unused here
            return None

        layer = DevtoolsLayer(ctx, AlarmHub(), EventBus())
        layer.start(reload_hook=reload_hook, shutdown_hook=lambda _reason: None)
        try:
            assert layer.metrics_server is not None
            status, _ = scrape(f"http://127.0.0.1:{port}/metrics")
            assert status == 200
        finally:
            await layer.stop_metrics()
            assert layer.metrics_server is None
            assert layer.monitor is not None
            await layer.monitor.stop()
        # port released: the endpoint no longer answers
        with pytest.raises(urllib.error.URLError):
            scrape(f"http://127.0.0.1:{port}/metrics")

    async def test_stop_metrics_is_idempotent(self, config_dir) -> None:
        import json

        from pyline.core.events import EventBus
        from pyline.obs.metrics import AlarmHub
        from pyline.runtime import build_context
        from pyline.runtime_wiring import DevtoolsLayer

        project = {
            "project": "metrics-test",
            "srv_type": "production",
            "metrics_port": None,  # endpoint disabled: close must be a no-op
            "socket": {"token": "$plain:x", "client_port": 1, "server_port": 2},
            "mysql": {"user": "u", "password": "$plain:y", "db_name": "d"},
            "redis": {},
        }
        (config_dir / "project.json5").write_text(json.dumps(project), encoding="utf-8")
        ctx = build_context(config_dir, 10001, "main", 0, 0)
        layer = DevtoolsLayer(ctx, AlarmHub(), EventBus())

        async def reload_hook(module: str) -> None:  # pragma: no cover - unused here
            return None

        layer.start(reload_hook=reload_hook, shutdown_hook=lambda _reason: None)
        await layer.stop_metrics()
        await layer.stop_metrics()  # second close: no error
        assert layer.metrics_server is None
        assert layer.monitor is not None
        await layer.monitor.stop()
