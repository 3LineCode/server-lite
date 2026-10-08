"""Configuration error types."""


class ConfigError(Exception):
    """Raised for any configuration problem (missing key, bad type, unresolved
    secret, unknown server, broken inheritance chain...).

    The message always names the file/field involved so startup failures are
    actionable; this is the fail-fast replacement for the prototype's config
    object that silently returned ``None`` on unknown keys.
    """
