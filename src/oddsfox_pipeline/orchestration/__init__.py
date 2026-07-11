__all__ = ["defs"]


def __getattr__(name: str):
    if name == "defs":
        from oddsfox_pipeline.orchestration.definitions import defs

        return defs
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
