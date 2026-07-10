import importlib


def test_main_imports_as_package_module():
    module = importlib.import_module("src.main")

    assert module.PaymentRequestHandler is not None
    assert module.RabbitMQHelper is not None
