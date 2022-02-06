# -*- coding: UTF-8 -*-
import sys

import pytest
from kupy.logger import logger


# each test runs on cwd to its temp dir
@pytest.fixture(autouse=True)
def go_to_tmpdir(request):
    logger.info(
        "\n=======================request start================================="
    )
    logger.debug(
        "\n=======================request start================================="
    )
    # Get the fixture dynamically by its name.
    tmpdir = request.getfixturevalue("tmpdir")
    # ensure local test created packages can be imported
    sys.path.insert(0, str(tmpdir))
    # Chdir only for the duration of the test.
    with tmpdir.as_cwd():
        yield
