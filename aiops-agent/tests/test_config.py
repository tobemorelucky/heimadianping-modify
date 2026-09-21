"""Configuration loading tests."""

from config import load_settings


def test_process_environment_overrides_dotenv_values(tmp_path):
    env_file = tmp_path / ".env"
    database_path = tmp_path / "configured.db"
    env_file.write_text(
        "AIOPS_PORT=8110\n"
        "AIOPS_LOG_LEVEL=warning\n"
        f"AIOPS_DATABASE_PATH={database_path}\n",
        encoding="utf-8",
    )

    settings = load_settings(
        env_file=env_file,
        environ={"AIOPS_PORT": "8120"},
    )

    assert settings.port == 8120
    assert settings.log_level == "WARNING"
    assert settings.database_path == database_path.resolve()


def test_configuration_rejects_non_loopback_host():
    try:
        load_settings(environ={"AIOPS_HOST": "0.0.0.0"})
    except ValueError as error:
        assert "loopback" in str(error)
    else:
        raise AssertionError("non-loopback host should be rejected")


def test_kafka_readonly_settings_are_loaded_from_environment():
    settings = load_settings(
        environ={
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
            "KAFKA_VOUCHER_ORDER_TOPIC": "custom.order.topic",
            "KAFKA_CONSUMER_GROUP": "custom-order-group",
            "AIOPS_KAFKA_REQUEST_TIMEOUT_MS": "2500",
            "AIOPS_KAFKA_LAG_THRESHOLD": "42",
        }
    )

    assert settings.kafka_bootstrap_servers == "localhost:19092"
    assert settings.kafka_default_topic == "custom.order.topic"
    assert settings.kafka_default_consumer_group == "custom-order-group"
    assert settings.kafka_request_timeout_ms == 2500
    assert settings.kafka_lag_threshold == 42
