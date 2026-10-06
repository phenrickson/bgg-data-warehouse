"""dataform-dev.yml must report a failed dev run as a failed workflow run."""

from pathlib import Path

TEXT = (Path(__file__).resolve().parents[1] / ".github/workflows/dataform-dev.yml").read_text(encoding="utf-8")


def test_compile_errors_stop_the_run():
    assert ".compilationErrors" in TEXT


def test_invocation_is_checked_and_polled_to_a_terminal_state():
    assert "invocation_name" in TEXT or "INVOCATION_NAME" in TEXT
    for state in ("SUCCEEDED", "FAILED", "CANCELLED"):
        assert state in TEXT, state


def test_failed_invocation_fails_the_job():
    assert 'exit 1' in TEXT.split("Invoke full graph", 1)[1]


def test_ref_input_is_not_interpolated_into_the_script():
    assert '"${{ github.event.inputs.ref }}"' not in TEXT
    assert "REF: ${{ github.event.inputs.ref }}" in TEXT
