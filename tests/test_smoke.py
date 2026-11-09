"""Phase 0 sanity checks: the project imports and the test harness works."""

import delta_crops


def test_package_is_importable() -> None:
    assert delta_crops.__version__ == "0.1.0"
