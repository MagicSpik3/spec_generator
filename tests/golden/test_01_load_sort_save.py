def test_01_load_sort_save(run_golden_test):
    """
    Case 1: Load + Sort + Save
    Validates: LOAD, SORT_ROWS, SAVE_BINARY, File Separation
    """
    spss_code = """
    GET DATA
      /TYPE=TXT
      /FILE='a.csv'
      /DELIMITERS=','.

    SORT CASES BY id.

    SAVE OUTFILE='b.sav'.
    """

    expected_ops = [
        {
            "type": "LOAD_CSV",
            "params": {"filename": "a.csv"}
        },
        {
            # 🚨 THIS IS THE CRITICAL CHECK
            # If this comes back as GENERIC_TRANSFORM, the test fails.
            "type": "SORT_ROWS", 
            "params": {"keys": "id"}
        },
        {
            "type": "SAVE_BINARY",
            "params": {"filename": "b.sav"}
        }
    ]

    run_golden_test(spss_code, expected_ops)