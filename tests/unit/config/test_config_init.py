def test_config_module_reexports_settings():
    import oddsfox_pipeline.config as cfg

    assert hasattr(cfg, "DUCKDB_PATH")
    assert hasattr(cfg, "BASE_DIR")
