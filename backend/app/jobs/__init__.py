"""Jobs package.

Keep package initialization side-effect free so submodule imports like
`from app.jobs import service` do not trigger router/service cycles.
"""

__all__ = []
