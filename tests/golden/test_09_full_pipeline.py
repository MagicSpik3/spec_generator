import pytest

# Mark as xfail (expected failure) until we implement the full semantic parser
@pytest.mark.xfail(reason="Need to implement Date Math and Match Files semantics")
def test_09_full_pipeline(run_golden_test):
    spss_code = """
    GET DATA /TYPE=TXT /FILE='regions.csv'.
    SORT CASES BY region.
    SAVE OUTFILE='tmp_regions.sav'.

    GET DATA /TYPE=TXT /FILE='people.csv'.
    MATCH FILES /FILE=* /TABLE='tmp_regions.sav' /BY region.
    SAVE OUTFILE='final.csv'.
    """

    expected_ops = [
        {"type": "LOAD_CSV", "params": {"filename": "regions.csv"}},
        {"type": "SORT_ROWS", "params": {"keys": "region"}},
        {"type": "SAVE_BINARY", "params": {"filename": "tmp_regions.sav"}},
        
        {"type": "LOAD_CSV", "params": {"filename": "people.csv"}},
        # Note: The builder should implicitly sort the active dataset if needed, 
        # or we might need an explicit sort op here depending on implementation.
        
        {
            "type": "JOIN",
            "params": {
                "type": "LEFT", # MATCH FILES /TABLE is a Left Join
                "by": "region"
            }
        },
        {"type": "SAVE_BINARY", "params": {"filename": "final.csv"}}
    ]

    run_golden_test(spss_code, expected_ops)