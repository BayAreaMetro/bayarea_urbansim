from .. import datasources


def test_limits_settings():
    limits_settings = {
        "default": {
            "Residential": {"city1": 100, "city2": 200},
        },
        "foo": {"Residential": {"city1": 50}, "Office": {"city3": 12}},
    }
    out = datasources.limits_settings({"development_limits": limits_settings}, "foo")
    assert out["Residential"]["city1"] == 50
    assert out["Residential"]["city2"] == 200
    assert out["Office"]["city3"] == 12


def test_inclusionary_housing_settings():
    inclusionary_housing_settings = {
        "foo": [
            {"amount": 0.2, "values": ["Berkeley", "Oakland"]},
            {"amount": 0.1, "values": ["San Francisco"]},
        ]
    }
    out = datasources.inclusionary_housing_settings(
        {
            "inclusionary_housing_settings": inclusionary_housing_settings,
            "inclusionary_fr2_enable": "foo",
            "inclusionary_d_b_enable": "foo",
        },
        "foo",
    )

    assert out["Berkeley"] == 0.2
    assert out["Oakland"] == 0.2
    assert out["San Francisco"] == 0.1
