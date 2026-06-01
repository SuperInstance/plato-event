# plato-event

> Event bus for PLATO nervous system — publish/subscribe routing with priority and filtering

## What This Does

plato-event implements a typed event bus for PLATO's internal communication. Events have types (sensor reading, alert, state change, etc.), priorities, timestamps, and sources. Subscribers register with filters and receive only matching events. The bus supports topic-based routing and priority-aware dispatch.

## The Key Idea

PLATO components need to talk to each other without knowing who's listening. A sensor publishes a reading. The anomaly detector subscribes to sensor readings. The alert system subscribes to anomalies. Nobody knows about each other — they just publish and subscribe through the event bus. Filters ensure subscribers only get what they care about.

## Install

```bash
cargo add plato-event
```

## Quick Start

```rust
use plato_event::*;

let event = Event::new(
    EventType::SensorReading,
    "temp-sensor-1",
    "22.5",
    1700000000,
    Priority::Normal,
);

// Filter for high-priority events from a specific source
let filter = EventFilter::new()
    .source("temp-sensor-1")
    .min_priority(Priority::High);
filter.matches(&event); // false — Normal < High
```

## API Reference

| Type | Description |
|---|---|
| `EventType` | `SensorReading` / `Alert` / `StateChange` / `Coordination` / `Heartbeat` / `Custom(String)` |
| `Priority` | `Low` < `Normal` < `High` < `Critical` |
| `Event { id, event_type, source, payload, timestamp, priority }` | A single event with auto-generated UUID |
| `EventFilter` | Builder: `source()`, `min_priority()`, `time_range()`. `matches(event)` |

## Testing

17 tests: event creation, filter matching (source, priority, time range), event bus publish/subscribe, topic routing, priority ordering.

## License

Apache-2.0
