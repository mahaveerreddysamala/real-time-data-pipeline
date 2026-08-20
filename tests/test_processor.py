from processor.stream_processor import SCHEMA


def test_event_schema_contains_required_fields():
    assert {f.name for f in SCHEMA.fields} == {"event_id", "customer_id", "event_type", "amount", "event_time"}
