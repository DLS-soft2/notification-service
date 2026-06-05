from app.service.idempotency import is_duplicate


async def test_is_duplicate_returns_false_for_new_event(fake_redis):
    """First call for a new event_id should return False (not a duplicate)."""
    result = await is_duplicate(fake_redis, "new-event-id")
    assert result is False


async def test_is_duplicate_returns_true_on_second_call(fake_redis):
    """Second call with same event_id should return True (duplicate)."""
    first = await is_duplicate(fake_redis, "evt-dup")
    second = await is_duplicate(fake_redis, "evt-dup")
    assert first is False
    assert second is True


async def test_is_duplicate_sets_idempotency_key(fake_redis):
    """is_duplicate sets a key with the format 'idempotency:{event_id}'."""
    await is_duplicate(fake_redis, "evt-fmt")
    exists = await fake_redis.exists("idempotency:evt-fmt")
    assert exists
