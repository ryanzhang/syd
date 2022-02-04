from syd.config import configs

def test_properties_been_load():
    assert configs is not None
    assert configs.get("postgres_host").data== "pg-quant-invest"
    assert configs["postgres_port"].data == "5432"
    assert configs.get("postgres_user").data== "user"
    assert configs.get("postgres_password").data== "password"
    assert configs.get("postgres_database").data== "market"
    assert configs.get("tus_token").data != ""
    assert configs.get("log_level").data != ""
    assert configs.get("something_not_exists") is None
