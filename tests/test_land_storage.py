import runpy
from src import land_storage

def test_script():
    runpy.run_path("src/land_storage.py")

def test_port_class_creation():
    port = land_storage.Port("test-port")
    assert(port.name) == "test-port"
    assert(port.diesel_abs) == 0
    assert(port.diesel_max) == 0

def test_port_class_creation_values():
    port = land_storage.Port("test-port",10,0.5)
    assert(port.name) == "test-port"
    assert(port.diesel_abs) == 10
    assert(port.diesel_max) == 20

def test_port_class_creation_all_values():
    port = land_storage.Port("test-port",10,0.5,10,0.1)
    assert(port.name) == "test-port"
    assert(port.crude_abs) == 10
    assert(port.crude_max) == 100

def test_port_load_cargo():
    port = land_storage.Port("test-port", 20)
    port.load_cargo("diesel", 10)
    assert(port.diesel_abs) == 10

def test_port_discharge_cargo():
    port = land_storage.Port("test-port", 20)
    port.discharge_cargo("diesel", 10)
    assert(port.diesel_abs) == 30

def test_port_load_cargo_direct():
    port = land_storage.Port("test-port", 20)
    port.load_cargo_diesel(10)
    assert(port.diesel_abs) == 10

def test_port_discharge_cargo_direct():
    port = land_storage.Port("test-port", 20)
    port.discharge_cargo_diesel(10)
    assert(port.diesel_abs) == 30