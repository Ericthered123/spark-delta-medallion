def test_metadata_filters_processed_files():
    files = ["events_2024-01-01.csv", "events_2024-01-02.csv"]
    last_processed = "events_2024-01-01.csv"

    new_files = [f for f in files if f > last_processed]

    assert new_files == ["events_2024-01-02.csv"]