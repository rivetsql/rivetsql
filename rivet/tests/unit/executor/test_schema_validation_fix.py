"""Test that schema validation handles the specific type mismatches from the user's issue."""

from rivet_core.executor import _normalize_arrow_type, _schemas_are_compatible


def test_enriched_transactions_schema_compatibility():
    """Verify all type differences from enriched_transactions_output are normalized correctly."""
    # transaction_date: utf8 → timestamp[s]
    assert _normalize_arrow_type("utf8") != _normalize_arrow_type("timestamp[s]")

    # All text fields: utf8 → string (should normalize to same)
    assert _normalize_arrow_type("utf8") == _normalize_arrow_type("string")

    # All numeric fields: float64 → double (should normalize to same)
    assert _normalize_arrow_type("float64") == _normalize_arrow_type("double")


def test_customer_analytics_schema_compatibility():
    """Verify all type differences from customer_analytics_output are normalized correctly."""
    # All text fields: utf8 → string (should normalize to same)
    assert _normalize_arrow_type("utf8") == _normalize_arrow_type("string")

    # Date fields: utf8 → timestamp[s] (different types, should not normalize)
    assert _normalize_arrow_type("utf8") != _normalize_arrow_type("timestamp[s]")

    # signup_date: date32 → date32[day] (should normalize to same)
    assert _normalize_arrow_type("date32") == _normalize_arrow_type("date32[day]")

    # Numeric fields: float64 → double (should normalize to same)
    assert _normalize_arrow_type("float64") == _normalize_arrow_type("double")

    # Count fields: int64 → decimal128(38, 0) (should normalize to same)
    assert _normalize_arrow_type("int64") == _normalize_arrow_type("decimal128(38, 0)")


def test_schema_dict_comparison_with_normalization():
    """Test that normalized schema comparison works correctly."""
    # Simulate the expected vs actual schemas from the user's issue
    expected_schema = {
        "customer_id": "int64",
        "customer_name": "utf8",
        "total_revenue": "float64",
        "signup_date": "date32",
        "completed_transactions": "int64",
    }

    actual_schema = {
        "customer_id": "int64",
        "customer_name": "string",
        "total_revenue": "double",
        "signup_date": "date32[day]",
        "completed_transactions": "decimal128(38, 0)",
    }

    # Without normalization, these would differ
    assert expected_schema != actual_schema

    # With normalization, they should match
    normalized_expected = {k: _normalize_arrow_type(v) for k, v in expected_schema.items()}
    normalized_actual = {k: _normalize_arrow_type(v) for k, v in actual_schema.items()}

    assert normalized_expected == normalized_actual


def test_schemas_are_compatible_with_user_issue():
    """Test that the actual schemas from the user's issue are considered compatible."""
    # enriched_transactions_output schema
    expected_enriched = {
        "transaction_id": "int64",
        "transaction_date": "utf8",
        "quantity": "int64",
        "payment_method": "utf8",
        "status": "utf8",
        "customer_id": "int64",
        "customer_name": "utf8",
        "email": "utf8",
        "country": "utf8",
        "customer_tier": "utf8",
        "product_id": "int64",
        "product_name": "utf8",
        "category": "utf8",
        "price": "float64",
        "total_amount": "float64",
        "discounted_amount": "float64",
    }

    actual_enriched = {
        "transaction_id": "int64",
        "transaction_date": "timestamp[s]",
        "quantity": "int64",
        "payment_method": "string",
        "status": "string",
        "customer_id": "int64",
        "customer_name": "string",
        "email": "string",
        "country": "string",
        "customer_tier": "string",
        "product_id": "int64",
        "product_name": "string",
        "category": "string",
        "price": "double",
        "total_amount": "double",
        "discounted_amount": "double",
    }

    # These should be considered compatible
    assert _schemas_are_compatible(expected_enriched, actual_enriched)

    # customer_analytics_output schema
    expected_analytics = {
        "customer_id": "int64",
        "customer_name": "utf8",
        "email": "utf8",
        "country": "utf8",
        "customer_tier": "utf8",
        "signup_date": "date32",
        "total_transactions": "int64",
        "total_revenue": "float64",
        "avg_transaction_value": "float64",
        "last_transaction_date": "utf8",
        "first_transaction_date": "utf8",
        "unique_categories_purchased": "int64",
        "completed_transactions": "int64",
        "failed_transactions": "int64",
    }

    actual_analytics = {
        "customer_id": "int64",
        "customer_name": "string",
        "email": "string",
        "country": "string",
        "customer_tier": "string",
        "signup_date": "date32[day]",
        "total_transactions": "int64",
        "total_revenue": "double",
        "avg_transaction_value": "double",
        "last_transaction_date": "timestamp[s]",
        "first_transaction_date": "timestamp[s]",
        "unique_categories_purchased": "int64",
        "completed_transactions": "decimal128(38, 0)",
        "failed_transactions": "decimal128(38, 0)",
    }

    # These should be considered compatible
    assert _schemas_are_compatible(expected_analytics, actual_analytics)


def test_schemas_are_incompatible_for_real_mismatches():
    """Test that truly incompatible schemas are still flagged."""
    expected = {
        "id": "int64",
        "name": "utf8",
        "value": "float64",
    }

    # Different column names
    actual_wrong_cols = {
        "id": "int64",
        "name": "utf8",
        "amount": "float64",  # Different column name
    }
    assert not _schemas_are_compatible(expected, actual_wrong_cols)

    # Incompatible types (int vs string)
    actual_wrong_type = {
        "id": "utf8",  # Should be int64
        "name": "utf8",
        "value": "float64",
    }
    assert not _schemas_are_compatible(expected, actual_wrong_type)
