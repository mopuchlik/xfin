from xfin.forecaster.registry import list_models, create_model


def test_forecaster_registry():
    """
    The model registry is the single source of truth for "what models exist".

    Contract:
    - list_models() returns known model names (strings).
    - create_model(name) returns a new model instance with matching .name.
    """

    assert "naive_last_close" in list_models()
    m = create_model("naive_last_close")
    assert getattr(m, "name", None) == "naive_last_close"
