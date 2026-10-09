"""Server-registry facade (old GetServerList family; the exclude branch now
actually works -- prototype issue #3)."""

from __future__ import annotations

from pyline import api


def server_list(exclude: list[int] | None = None) -> list[int]:
    return api.ctx().registry.all_servers(exclude=exclude)


def server_ip(server_no: int) -> str:
    return api.ctx().registry.entry(server_no).advertise_ip


def server_name(server_no: int) -> str:
    return api.ctx().registry.entry(server_no).name


def server_by_ip(ip: str) -> int | None:
    return api.ctx().registry.by_advertise_ip(ip)


def server_port(server_no: int) -> int:
    """Inter-connect port of a server's main process.

    Raises ConfigError for entries without ``server_port`` (F-84): they
    cannot be addressed, and pretending port 0 works just moved the failure
    to a confusing connect() attempt."""
    return api.ctx().registry.entry(server_no).process_port(0)


def proxy_list() -> list[int]:
    return api.ctx().registry.proxy_list()


def is_proxy(server_no: int) -> bool:
    return api.ctx().registry.entry(server_no).is_proxy
