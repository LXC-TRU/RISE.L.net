"""MicroPython client example for K230-class boards.

Copy this file and the rise_l_net package onto the device, then::

    >>> import micropython_client

The package is designed to import on MicroPython without standard-Python
dependencies. The async submodules are not imported unless you use them.
"""

from rise_l_net import RISELDevice
from rise_l_net.client import RetryMiddleware


def main() -> None:
    device = RISELDevice(
        "http://192.168.1.100:8080",
        wifi_ssid="MyWiFi",
        wifi_password="mypass",
        heartbeat_interval=30,
        metadata={"device_name": "K230-Sensor-01", "location": "garage"},
    ).use(RetryMiddleware(max_retries=5, base_delay=2.0))

    device.start(block=False)
    # In real firmware you would loop on sensor reads and call device.report(...)
    device.report("boot", {"reason": "power-on"})


if __name__ == "__main__":
    main()
