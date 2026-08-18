"""
Phase 6 of DLT_PLAN.md -- trace-vs-log corroboration.

`test_business_exception_hiding_a_timeout_is_contradicted` is the mis-cast case
and the reason the log lane exists at all. Without it, the headers alone would
be enough and none of Phase 5 would be worth building.

The other property that matters is the one the log pipeline already makes with
`FetchResult.ok`: "could not look" and "looked and found nothing" must stay
distinguishable. Collapsing them lets a finding read as "no errors occurred"
when the truth is "we could not read the logs".
"""
import pytest

from src.dlt.corroborate import Verdict, corroborate

BUSINESS_FQCN = "in.gov.uidai.common.exception.BusinessException"
CODE = "UID_ORIGIN_TRACKER_DATA_NOT_FOUND"
FRAMES = ("com.uidai.enu.biometric.service.impl.BioDataBaseHelperServiceImpl."
          "getUidOriginTrackerData",)


def logs(*lines):
    return "--- Log Trace ---\n" + "\n".join(lines) + "\n--- End ---"


# ======================================================================
# CORROBORATED
# ======================================================================

def test_declared_root_present_by_fqcn():
    result = corroborate(
        logs(f"[2026-08-18T02:20:08Z] [enu-biometric] [ERROR] {BUSINESS_FQCN}: [{CODE}] absent"),
        BUSINESS_FQCN, CODE, FRAMES)
    assert result.verdict is Verdict.CORROBORATED
    assert result.matched_declared is True
    assert result.citations


def test_declared_root_present_by_simple_name():
    result = corroborate(
        logs("[2026-08-18T02:20:08Z] [ERROR] BusinessException thrown during dedup"),
        BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.CORROBORATED
    assert result.details["matched_on"] == "simple name"


def test_declared_root_present_by_business_code_only():
    """Services log the code without the exception type often enough that
    requiring the FQCN would produce false contradictions."""
    result = corroborate(
        logs(f"[2026-08-18T02:20:08Z] [ERROR] lookup failed: {CODE}"),
        BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.CORROBORATED
    assert result.details["matched_on"] == "business code"


def test_frame_hits_are_recorded_as_supporting_evidence():
    result = corroborate(
        logs(f"[ERROR] {BUSINESS_FQCN}: [{CODE}] in getUidOriginTrackerData"),
        BUSINESS_FQCN, CODE, FRAMES)
    assert result.details["frame_hits"]


# ======================================================================
# CONTRADICTED -- the mis-cast case
# ======================================================================

def test_business_exception_hiding_a_timeout_is_contradicted():
    """THE case this whole lane exists for: the catch block rethrows an infra
    fault as a business error, and the trace confidently reports the wrong
    root cause."""
    result = corroborate(
        logs("[2026-08-18T02:20:07Z] [ERROR] java.net.SocketTimeoutException: "
             "Read timed out talking to the uid-origin datasource",
             "[2026-08-18T02:20:08Z] [ERROR] transaction rolled back"),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.CONTRADICTED
    assert result.is_discrepancy
    assert result.matched_declared is False
    assert "java.net.SocketTimeoutException" in result.unexplained
    assert "SocketTimeoutException" in result.reason
    assert result.citations, "a contradiction must cite what it saw"


def test_contradiction_names_the_declared_root_it_could_not_find():
    result = corroborate(
        logs("[ERROR] java.sql.SQLRecoverableException: connection lost"),
        BUSINESS_FQCN, CODE)
    assert "BusinessException" in result.reason


# ======================================================================
# PARTIAL
# ======================================================================

def test_declared_root_plus_unexplained_errors_is_partial():
    result = corroborate(
        logs(f"[ERROR] {BUSINESS_FQCN}: [{CODE}] absent",
             "[ERROR] java.net.SocketTimeoutException: Read timed out"),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.PARTIAL
    assert result.is_discrepancy
    assert result.matched_declared is True
    assert "java.net.SocketTimeoutException" in result.unexplained


# ======================================================================
# UNVERIFIABLE -- and the could-not-look distinction
# ======================================================================

@pytest.mark.parametrize("value", [None, "", "   "])
def test_no_logs_is_unverifiable_and_could_not_look(value):
    result = corroborate(value, BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.could_not_look is True


@pytest.mark.parametrize("marker", [
    "No refId available; logs were not fetched.",
    "No usable timestamp; logs were not fetched.",
    "Log window too old to fetch: ...",
    "Log fetch failed: RuntimeError: cluster unreachable",
])
def test_skipped_fetches_are_could_not_look(marker):
    result = corroborate(marker, BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.could_not_look is True


def test_source_returned_nothing_is_not_could_not_look():
    """'Looked and found nothing' is a different state from 'could not look',
    exactly as FetchResult.ok distinguishes them."""
    result = corroborate("No logs found for ID: REF-1", BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.could_not_look is False


def test_logs_without_error_lines_are_unverifiable():
    result = corroborate(
        logs("[INFO] started", "[INFO] finished"), BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.could_not_look is False
    assert result.error_lines_seen == 0


def test_errors_naming_no_exception_type_do_not_contradict():
    """Not enough signal to accuse the trace of lying."""
    result = corroborate(
        logs("[ERROR] something went wrong", "[ERROR] retrying"),
        BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.error_lines_seen == 2


# ======================================================================
# Matching hygiene
# ======================================================================

def test_error_matching_is_word_bounded():
    """'TERROR' and 'error_code' must not register as error-level lines."""
    result = corroborate(
        logs("[INFO] TERROR movie night", "[INFO] error_code=0 all good"),
        BUSINESS_FQCN, CODE)
    assert result.error_lines_seen == 0


@pytest.mark.parametrize("level", ["ERROR", "FATAL", "SEVERE"])
def test_all_error_levels_are_recognised(level):
    result = corroborate(logs(f"[{level}] {BUSINESS_FQCN}: [{CODE}] x"),
                         BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.CORROBORATED


def test_citations_are_bounded():
    """A 4,000-line retry storm must not land whole in the casebook."""
    storm = logs(*[f"[ERROR] java.net.SocketTimeoutException: attempt {i}"
                   for i in range(4000)])
    result = corroborate(storm, BUSINESS_FQCN, CODE)
    assert len(result.citations) <= 20
    assert result.error_lines_seen == 4000


def test_unknown_declared_root_still_reports_what_it_saw():
    result = corroborate(
        logs("[ERROR] java.net.SocketTimeoutException: Read timed out"), None, None)
    assert result.verdict is Verdict.CONTRADICTED
    assert "java.net.SocketTimeoutException" in result.unexplained


def test_same_exception_in_a_different_package_is_not_unexplained():
    """Matching on the simple name keeps a repackaged type from reading as a
    contradiction."""
    result = corroborate(
        logs("[ERROR] com.other.pkg.BusinessException: [CODE] boom"),
        BUSINESS_FQCN, "CODE")
    assert result.verdict is Verdict.CORROBORATED


def test_verdict_is_total():
    for value in (None, "", "garbage", "[ERROR]", "\n\n"):
        assert corroborate(value, BUSINESS_FQCN, CODE).verdict in set(Verdict)
