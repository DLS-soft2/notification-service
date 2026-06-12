import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka import TopicPartition

from app.kafka.consumer import _process_dispatch_batch, run_dispatch_job
from app.service.receipt_service import generate_delivery_receipt, get_delivery_receipt

TP0 = TopicPartition("deliveries", 0)


def _make_settings(**overrides):
    """Create a minimal Settings-like object for dispatch tests."""
    defaults = {
        "kafka_bootstrap_servers": "localhost:9092",
        "kafka_topic_deliveries": "deliveries",
        "kafka_group_id": "test-group",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_message(topic, partition, offset, payload):
    """Build a fake Kafka message with .value, .offset, .topic, .partition."""
    msg = MagicMock()
    msg.topic = topic
    msg.partition = partition
    msg.offset = offset
    msg.value = json.dumps(payload).encode("utf-8")
    return msg


def _delivery_event(event_id="evt-001", order_id="order-001", customer_id="cust-001"):
    return {
        "event_type": "DeliveryCompleted",
        "event_id": event_id,
        "order_id": order_id,
        "customer_id": customer_id,
        "timestamp": "2026-06-12T10:00:00Z",
    }


@pytest.fixture(name="mock_consumer")
def fixture_mock_consumer():
    """Pre-configured mock AIOKafkaConsumer for dispatch tests."""
    consumer = AsyncMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.end_offsets = AsyncMock()
    consumer.position = AsyncMock()
    consumer.getmany = AsyncMock(return_value={})
    consumer.assignment = MagicMock(return_value=set())
    return consumer


# --- run_dispatch_job tests ---


async def test_run_dispatch_job_no_messages(mock_consumer):
    """Partitions assigned, all end_offsets=0 — exits without processing."""
    mock_consumer.assignment.return_value = {TP0}
    mock_consumer.end_offsets.return_value = {TP0: 0}

    with patch("app.kafka.consumer.AIOKafkaConsumer", return_value=mock_consumer):
        await run_dispatch_job(_make_settings())

    mock_consumer.start.assert_awaited_once()
    mock_consumer.stop.assert_awaited_once()
    mock_consumer.commit.assert_not_awaited()


async def test_run_dispatch_job_generates_receipts(mock_consumer, fake_redis):
    """Dispatch job calls generate_delivery_receipt, not handle_event."""
    mock_consumer.assignment.return_value = {TP0}
    mock_consumer.end_offsets.return_value = {TP0: 1}

    event = _delivery_event()
    msg = _make_message("deliveries", 0, 0, event)
    mock_consumer.getmany.return_value = {TP0: [msg]}

    with (
        patch("app.kafka.consumer.AIOKafkaConsumer", return_value=mock_consumer),
        patch("app.kafka.consumer.get_redis", return_value=fake_redis),
        patch(
            "app.kafka.consumer.generate_delivery_receipt",
            new_callable=AsyncMock,
            return_value={"order_id": "order-001"},
        ) as mock_receipt,
    ):
        await run_dispatch_job(_make_settings())

    mock_receipt.assert_awaited_once()
    mock_consumer.commit.assert_awaited_once()
    mock_consumer.stop.assert_awaited_once()


async def test_run_dispatch_job_timeout_exits_gracefully(mock_consumer, fake_redis):
    """Deadline expires mid-drain — job exits without raising."""
    mock_consumer.assignment.return_value = {TP0}
    mock_consumer.end_offsets.return_value = {TP0: 999}
    mock_consumer.getmany.return_value = {}
    mock_consumer.position.return_value = 0

    time_values = iter([100.0, 161.0])
    mock_loop = MagicMock()
    mock_loop.time.side_effect = lambda: next(time_values)

    with (
        patch("app.kafka.consumer.AIOKafkaConsumer", return_value=mock_consumer),
        patch("app.kafka.consumer.get_redis", return_value=fake_redis),
        patch("app.kafka.consumer.asyncio.get_running_loop", return_value=mock_loop),
        patch("app.kafka.consumer.asyncio.sleep", new_callable=AsyncMock),
    ):
        await run_dispatch_job(_make_settings())

    mock_consumer.commit.assert_awaited_once()
    mock_consumer.stop.assert_awaited_once()


async def test_process_dispatch_batch_marks_reached(fake_redis):
    """Last message.offset+1 >= end_offset sets reached[tp] to True."""
    end_offsets = {TP0: 3}
    reached = {TP0: False}

    msg = _make_message("deliveries", 0, 2, _delivery_event(event_id="evt-reach"))

    with patch("app.kafka.consumer.generate_delivery_receipt", new_callable=AsyncMock):
        await _process_dispatch_batch({TP0: [msg]}, end_offsets, reached, fake_redis)

    assert reached[TP0] is True


async def test_run_dispatch_job_empty_assignment_raises(mock_consumer):
    """Empty partition assignment after wait raises RuntimeError."""
    mock_consumer.assignment.return_value = set()

    with (
        patch("app.kafka.consumer.AIOKafkaConsumer", return_value=mock_consumer),
        pytest.raises(RuntimeError, match="Partition assignment still empty"),
    ):
        await run_dispatch_job(_make_settings())

    mock_consumer.stop.assert_awaited_once()


# --- receipt_service tests ---


async def test_generate_delivery_receipt_stores_in_redis(fake_redis):
    """Receipt is stored at delivery_receipt:{order_id} with correct data."""
    event = _delivery_event()
    receipt = await generate_delivery_receipt(event, fake_redis)

    assert receipt is not None
    assert receipt["order_id"] == "order-001"
    assert receipt["customer_id"] == "cust-001"
    assert receipt["delivered_at"] == "2026-06-12T10:00:00Z"
    assert receipt["status"] == "delivered"
    assert "generated_at" in receipt

    stored = await fake_redis.get("delivery_receipt:order-001")
    assert stored is not None
    assert json.loads(stored)["order_id"] == "order-001"


async def test_generate_delivery_receipt_missing_fields_returns_none(fake_redis):
    """Missing order_id returns None without storing anything."""
    event = {"event_type": "DeliveryCompleted", "customer_id": "cust-001", "timestamp": "2026-06-12T10:00:00Z"}
    result = await generate_delivery_receipt(event, fake_redis)

    assert result is None
    keys = await fake_redis.keys("delivery_receipt:*")
    assert keys == []


async def test_get_delivery_receipt_found(fake_redis):
    """Returns stored receipt when it exists."""
    receipt_data = {"order_id": "order-42", "status": "delivered"}
    await fake_redis.set("delivery_receipt:order-42", json.dumps(receipt_data))

    result = await get_delivery_receipt("order-42", fake_redis)
    assert result == receipt_data


async def test_get_delivery_receipt_not_found(fake_redis):
    """Returns None when no receipt exists."""
    result = await get_delivery_receipt("nonexistent", fake_redis)
    assert result is None
