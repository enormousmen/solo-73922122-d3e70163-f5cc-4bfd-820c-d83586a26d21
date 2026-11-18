import pytest

from relay.config import load_config


def test_defaults():
    cfg = load_config({})
    assert cfg.initial == "blue"
    assert set(cfg.targets) == {"blue", "green"}
    assert cfg.drain_seconds == 5.0


def test_json_targets_and_validation():
    cfg = load_config({"RELAY_TARGETS": '{"a": "http://x:1", "b": "http://y:2"}', "RELAY_INITIAL_TARGET": "b"})
    assert cfg.initial == "b"
    assert cfg.targets["a"].base_url == "http://x:1"

    with pytest.raises(ValueError):
        load_config({"RELAY_TARGETS": "a=http://x:1", "RELAY_INITIAL_TARGET": "nope"})
    with pytest.raises(ValueError):
        load_config({"RELAY_TARGETS": "a"})
    with pytest.raises(ValueError):
        load_config({"RELAY_TARGETS": "a=http://x:1,a=http://y:2"})
    with pytest.raises(ValueError):
        load_config({"RELAY_DRAIN_SECONDS": "-1"})


def test_bad_generation_history_limit():
    with pytest.raises(ValueError):
        load_config({"RELAY_MAX_CLOSED_GENERATIONS": "0"})


def test_probe_path_is_normalized():
    assert load_config({"RELAY_PROBE_PATH": "ready"}).probe_path == "/ready"
    assert load_config({"RELAY_PROBE_PATH": "/healthz"}).probe_path == "/healthz"
