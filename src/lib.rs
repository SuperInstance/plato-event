use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

// ── Core types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    SensorReading,
    Alert,
    StateChange,
    Coordination,
    Heartbeat,
    Custom(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub event_type: EventType,
    pub source: String,
    pub payload: String,
    pub timestamp: u64,
    pub priority: Priority,
}

impl Event {
    pub fn new(event_type: EventType, source: impl Into<String>, payload: impl Into<String>, timestamp: u64, priority: Priority) -> Self {
        Self {
            id: Uuid::new_v4(),
            event_type,
            source: source.into(),
            payload: payload.into(),
            timestamp,
            priority,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventFilter {
    pub source: Option<String>,
    pub min_priority: Option<Priority>,
    pub time_range: Option<(u64, u64)>,
}

impl EventFilter {
    pub fn new() -> Self {
        Self {
            source: None,
            min_priority: None,
            time_range: None,
        }
    }

    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn min_priority(mut self, priority: Priority) -> Self {
        self.min_priority = Some(priority);
        self
    }

    pub fn time_range(mut self, start: u64, end: u64) -> Self {
        self.time_range = Some((start, end));
        self
    }

    pub fn matches(&self, event: &Event) -> bool {
        if let Some(ref source) = self.source {
            if event.source != *source {
                return false;
            }
        }
        if let Some(min) = self.min_priority {
            if event.priority < min {
                return false;
            }
        }
        if let Some((start, end)) = self.time_range {
            if event.timestamp < start || event.timestamp > end {
                return false;
            }
        }
        true
    }
}

impl Default for EventFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: Uuid,
    pub event_types: Vec<EventType>,
    pub callback_id: String,
    pub filter: Option<EventFilter>,
}

impl Subscription {
    pub fn new(event_types: Vec<EventType>, callback_id: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            event_types,
            callback_id: callback_id.into(),
            filter: None,
        }
    }

    pub fn with_filter(mut self, filter: EventFilter) -> Self {
        self.filter = Some(filter);
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventStats {
    pub published: u64,
    pub delivered: u64,
    pub dropped: u64,
    pub by_type: HashMap<EventType, u64>,
}

// ── Event bus ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBus {
    subscriptions: Vec<Subscription>,
    history: Vec<Event>,
    stats: EventStats,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            subscriptions: Vec::new(),
            history: Vec::new(),
            stats: EventStats::default(),
        }
    }

    pub fn publish(&mut self, event: Event) -> Vec<&Subscription> {
        *self.stats.by_type.entry(event.event_type.clone()).or_insert(0) += 1;
        self.stats.published += 1;

        let matching: Vec<&Subscription> = self
            .subscriptions
            .iter()
            .filter(|sub| self.matches(&event, sub))
            .collect();

        if matching.is_empty() {
            self.stats.dropped += 1;
        } else {
            self.stats.delivered += matching.len() as u64;
        }

        self.history.push(event);
        matching
    }

    pub fn subscribe(&mut self, mut sub: Subscription) -> Uuid {
        let id = Uuid::new_v4();
        sub.id = id;
        self.subscriptions.push(sub);
        id
    }

    pub fn unsubscribe(&mut self, sub_id: Uuid) -> bool {
        let before = self.subscriptions.len();
        self.subscriptions.retain(|s| s.id != sub_id);
        self.subscriptions.len() < before
    }

    pub fn matches(&self, event: &Event, sub: &Subscription) -> bool {
        // Check event type
        let type_match = sub.event_types.is_empty()
            || sub.event_types.iter().any(|t| {
                match (t, &event.event_type) {
                    (EventType::Custom(a), EventType::Custom(b)) => a == b,
                    (a, b) => a == b,
                }
            });

        if !type_match {
            return false;
        }

        // Check filter
        if let Some(ref filter) = sub.filter {
            if !filter.matches(event) {
                return false;
            }
        }

        true
    }

    pub fn history(&self, filter: &EventFilter) -> Vec<&Event> {
        self.history.iter().filter(|e| filter.matches(e)).collect()
    }

    pub fn stats(&self) -> EventStats {
        self.stats.clone()
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn history_count(&self) -> usize {
        self.history.len()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_creation_and_serialization() {
        let event = Event::new(
            EventType::SensorReading,
            "room-1",
            r#"{"temp": 22.5}"#,
            1000,
            Priority::Normal,
        );
        assert!(!event.id.is_nil());
        assert_eq!(event.source, "room-1");
        assert_eq!(event.event_type, EventType::SensorReading);

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("SensorReading"));
        let back: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, event.id);
    }

    #[test]
    fn publish_no_subscribers() {
        let mut bus = EventBus::new();
        let event = Event::new(EventType::Heartbeat, "system", "ping", 100, Priority::Low);
        let matching = bus.publish(event);
        assert!(matching.is_empty());

        let stats = bus.stats();
        assert_eq!(stats.published, 1);
        assert_eq!(stats.dropped, 1);
        assert_eq!(stats.delivered, 0);
    }

    #[test]
    fn subscribe_to_specific_event_type() {
        let mut bus = EventBus::new();
        let sub = Subscription::new(vec![EventType::Alert], "alert-handler");
        let sub_id = bus.subscribe(sub);
        assert!(!sub_id.is_nil());
        assert_eq!(bus.subscription_count(), 1);

        let alert = Event::new(EventType::Alert, "room-1", "fire!", 200, Priority::Critical);
        let matching = bus.publish(alert);
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].callback_id, "alert-handler");
    }

    #[test]
    fn multiple_subscribers_selective_matching() {
        let mut bus = EventBus::new();
        bus.subscribe(Subscription::new(vec![EventType::Alert], "handler-a"));
        bus.subscribe(Subscription::new(vec![EventType::SensorReading], "handler-b"));
        bus.subscribe(Subscription::new(vec![], "handler-all")); // wildcard

        let alert = Event::new(EventType::Alert, "room-1", "alert", 100, Priority::High);
        let matching = bus.publish(alert);
        assert_eq!(matching.len(), 2); // handler-a + handler-all

        let reading = Event::new(EventType::SensorReading, "room-1", "22.5", 200, Priority::Normal);
        let matching = bus.publish(reading);
        assert_eq!(matching.len(), 2); // handler-b + handler-all
    }

    #[test]
    fn priority_filtering() {
        let mut bus = EventBus::new();
        let filter = EventFilter::new().min_priority(Priority::High);
        let sub = Subscription::new(vec![EventType::Alert], "high-alerts").with_filter(filter);
        bus.subscribe(sub);

        let low = Event::new(EventType::Alert, "room-1", "low", 100, Priority::Low);
        assert!(bus.publish(low).is_empty());

        let critical = Event::new(EventType::Alert, "room-1", "critical", 200, Priority::Critical);
        assert_eq!(bus.publish(critical).len(), 1);
    }

    #[test]
    fn source_filtering() {
        let mut bus = EventBus::new();
        let filter = EventFilter::new().source("room-1");
        let sub = Subscription::new(vec![EventType::SensorReading], "room1-handler").with_filter(filter);
        bus.subscribe(sub);

        let from_room1 = Event::new(EventType::SensorReading, "room-1", "22", 100, Priority::Normal);
        assert_eq!(bus.publish(from_room1).len(), 1);

        let from_room2 = Event::new(EventType::SensorReading, "room-2", "23", 200, Priority::Normal);
        assert!(bus.publish(from_room2).is_empty());
    }

    #[test]
    fn time_range_filtering() {
        let mut bus = EventBus::new();
        let filter = EventFilter::new().time_range(100, 200);
        let sub = Subscription::new(vec![EventType::Heartbeat], "time-handler").with_filter(filter);
        bus.subscribe(sub);

        let in_range = Event::new(EventType::Heartbeat, "sys", "beat", 150, Priority::Normal);
        assert_eq!(bus.publish(in_range).len(), 1);

        let before = Event::new(EventType::Heartbeat, "sys", "beat", 50, Priority::Normal);
        assert!(bus.publish(before).is_empty());

        let after = Event::new(EventType::Heartbeat, "sys", "beat", 300, Priority::Normal);
        assert!(bus.publish(after).is_empty());
    }

    #[test]
    fn history_queries() {
        let mut bus = EventBus::new();
        bus.publish(Event::new(EventType::SensorReading, "room-1", "a", 100, Priority::Normal));
        bus.publish(Event::new(EventType::Alert, "room-2", "b", 200, Priority::High));
        bus.publish(Event::new(EventType::SensorReading, "room-1", "c", 300, Priority::Normal));

        let all = bus.history(&EventFilter::new());
        assert_eq!(all.len(), 3);

        let room1 = bus.history(&EventFilter::new().source("room-1"));
        assert_eq!(room1.len(), 2);

        let range = bus.history(&EventFilter::new().time_range(150, 250));
        assert_eq!(range.len(), 1);
    }

    #[test]
    fn stats_accuracy() {
        let mut bus = EventBus::new();
        bus.subscribe(Subscription::new(vec![EventType::Alert], "h1"));
        bus.subscribe(Subscription::new(vec![EventType::Alert], "h2"));

        bus.publish(Event::new(EventType::Alert, "src", "a", 100, Priority::High));
        bus.publish(Event::new(EventType::SensorReading, "src", "b", 200, Priority::Low));

        let stats = bus.stats();
        assert_eq!(stats.published, 2);
        assert_eq!(stats.delivered, 2); // 2 for alert + 0 for sensor reading
        assert_eq!(stats.dropped, 1); // sensor reading had no subscribers
        assert_eq!(*stats.by_type.get(&EventType::Alert).unwrap(), 1);
        assert_eq!(*stats.by_type.get(&EventType::SensorReading).unwrap(), 1);
    }

    #[test]
    fn unsubscribe_removes_subscription() {
        let mut bus = EventBus::new();
        let sub_id = bus.subscribe(Subscription::new(vec![EventType::Alert], "h1"));
        assert_eq!(bus.subscription_count(), 1);

        assert!(bus.unsubscribe(sub_id));
        assert_eq!(bus.subscription_count(), 0);
        assert!(!bus.unsubscribe(sub_id)); // already gone
    }

    #[test]
    fn empty_bus() {
        let bus = EventBus::new();
        assert_eq!(bus.subscription_count(), 0);
        assert_eq!(bus.history_count(), 0);
        let stats = bus.stats();
        assert_eq!(stats.published, 0);
    }

    #[test]
    fn all_events_match_wildcard() {
        let mut bus = EventBus::new();
        bus.subscribe(Subscription::new(vec![], "catch-all"));

        for et in [
            EventType::SensorReading,
            EventType::Alert,
            EventType::StateChange,
            EventType::Coordination,
            EventType::Heartbeat,
        ] {
            let e = Event::new(et, "src", "p", 100, Priority::Normal);
            assert_eq!(bus.publish(e).len(), 1);
        }
    }

    #[test]
    fn no_events_match_wrong_type() {
        let mut bus = EventBus::new();
        bus.subscribe(Subscription::new(vec![EventType::Coordination], "coord"));

        let event = Event::new(EventType::Alert, "src", "p", 100, Priority::Normal);
        assert!(bus.publish(event).is_empty());
    }

    #[test]
    fn custom_event_type() {
        let mut bus = EventBus::new();
        bus.subscribe(Subscription::new(vec![EventType::Custom("diy".into())], "diy-handler"));

        let matches = bus.publish(Event::new(EventType::Custom("diy".into()), "src", "x", 100, Priority::Normal));
        assert_eq!(matches.len(), 1);

        let no = bus.publish(Event::new(EventType::Custom("other".into()), "src", "x", 100, Priority::Normal));
        assert!(no.is_empty());
    }

    #[test]
    fn combined_filter() {
        let mut bus = EventBus::new();
        let filter = EventFilter::new()
            .source("room-1")
            .min_priority(Priority::High)
            .time_range(100, 500);
        bus.subscribe(Subscription::new(vec![EventType::Alert], "strict").with_filter(filter));

        // All conditions met
        let good = Event::new(EventType::Alert, "room-1", "!", 200, Priority::Critical);
        assert_eq!(bus.publish(good).len(), 1);

        // Wrong source
        let bad_src = Event::new(EventType::Alert, "room-2", "!", 200, Priority::Critical);
        assert!(bus.publish(bad_src).is_empty());

        // Too low priority
        let bad_pri = Event::new(EventType::Alert, "room-1", "!", 200, Priority::Low);
        assert!(bus.publish(bad_pri).is_empty());

        // Out of time range
        let bad_time = Event::new(EventType::Alert, "room-1", "!", 999, Priority::Critical);
        assert!(bus.publish(bad_time).is_empty());
    }

    #[test]
    fn event_filter_default_matches_all() {
        let event = Event::new(EventType::Heartbeat, "any", "x", 42, Priority::Low);
        assert!(EventFilter::new().matches(&event));
        assert!(EventFilter::default().matches(&event));
    }

    #[test]
    fn priority_ordering() {
        assert!(Priority::Low < Priority::Normal);
        assert!(Priority::Normal < Priority::High);
        assert!(Priority::High < Priority::Critical);
    }
}
