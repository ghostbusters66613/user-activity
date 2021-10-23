import pytest

result_code = pytest.main(["-s", "-x", "tests"])

if result_code != pytest.ExitCode.OK:
    raise RuntimeError('Unit tests are failed')
