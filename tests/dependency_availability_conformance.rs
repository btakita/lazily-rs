mod common;

use std::cell::Cell;
use std::rc::Rc;
#[cfg(any(feature = "thread-safe", feature = "async"))]
use std::sync::Arc;
#[cfg(feature = "thread-safe")]
use std::sync::Barrier;
#[cfg(any(feature = "thread-safe", feature = "async"))]
use std::sync::atomic::{AtomicUsize, Ordering};

use common::Expect;
#[cfg(feature = "async")]
use lazily::{AsyncContext, AsyncDependencyMap};
use lazily::{Context, DependencyAvailability, DependencyMap};
#[cfg(feature = "thread-safe")]
use lazily::{ThreadSafeContext, ThreadSafeDependencyMap};
use serde_json::{Value, json};

const FIXTURE: common::SpecDir =
    common::SpecDir("collections/dependency_reactive_availability.json");

fn availability_json(state: DependencyAvailability<i64>) -> Value {
    match state {
        DependencyAvailability::Unavailable => json!("Unavailable"),
        DependencyAvailability::Available(value) => json!({ "Available": value }),
    }
}

#[test]
fn dependency_availability_fixture_replays() {
    let raw =
        common::spec_read_to_string(FIXTURE.path()).expect("read dependency availability fixture");
    let fixture: Value = serde_json::from_str(&raw).expect("parse dependency availability fixture");
    let steps = fixture["steps"].as_array().expect("steps array");

    let ctx = Context::new();
    let map: DependencyMap<String, i64> = DependencyMap::new(&ctx);
    let recomputes = Rc::new(Cell::new(0usize));
    let reader = ctx.computed({
        let map = map.clone();
        let recomputes = Rc::clone(&recomputes);
        move |cx| {
            recomputes.set(recomputes.get() + 1);
            map.observe_dependency(cx, "wanted".to_string())
        }
    });
    let mut first_handle = None;

    for (index, step) in steps.iter().enumerate() {
        let op = &step["op"];
        let key = op["key"].as_str().expect("op key").to_string();
        match op["type"].as_str().expect("op type") {
            "observe_dependency" => {
                assert_eq!(key, "wanted");
            }
            "publish" => {
                map.publish(&ctx, key, op["value"].as_i64().expect("publish value"));
            }
            "unpublish" => map.unpublish(&ctx, key),
            other => panic!("unknown dependency operation {other}"),
        }

        let state = ctx.get(&reader);
        let handle = map.handle(&"wanted".to_string()).expect("wanted handle");
        let stable = *first_handle.get_or_insert(handle) == handle;
        let expected = Expect::new(
            FIXTURE.to_string(),
            format!("steps[{index}].expected"),
            &step["expected"],
        );
        expected.assert_key("state", availability_json(state));
        expected.assert_key("recomputes", json!(recomputes.get()));
        expected.assert_key("present_count", json!(map.present_count()));
        expected.assert_key(
            "identity",
            json!(if stable { "wanted-1" } else { "changed" }),
        );
    }
}

#[cfg(feature = "thread-safe")]
#[test]
fn thread_safe_dependency_availability_is_exact_key_reactive() {
    let ctx = ThreadSafeContext::new();
    let map: ThreadSafeDependencyMap<String, i64> = ThreadSafeDependencyMap::new(&ctx);
    let recomputes = Arc::new(AtomicUsize::new(0));
    let reader = ctx.computed({
        let map = map.clone();
        let recomputes = Arc::clone(&recomputes);
        move |cx| {
            recomputes.fetch_add(1, Ordering::SeqCst);
            map.observe_dependency(cx, "wanted".to_string())
        }
    });

    assert_eq!(ctx.get(&reader), DependencyAvailability::Unavailable);
    let first = map.handle(&"wanted".to_string()).expect("wanted handle");
    map.publish(&ctx, "other".to_string(), 9);
    assert_eq!(ctx.get(&reader), DependencyAvailability::Unavailable);
    assert_eq!(recomputes.load(Ordering::SeqCst), 1);
    map.publish(&ctx, "wanted".to_string(), 1);
    assert_eq!(ctx.get(&reader), DependencyAvailability::Available(1));
    assert_eq!(recomputes.load(Ordering::SeqCst), 2);
    map.unpublish(&ctx, "wanted".to_string());
    assert_eq!(ctx.get(&reader), DependencyAvailability::Unavailable);
    assert_eq!(map.handle(&"wanted".to_string()), Some(first));
}

#[cfg(feature = "thread-safe")]
#[test]
fn concurrent_first_observers_converge_on_one_dependency_source() {
    let ctx = ThreadSafeContext::new();
    let map: ThreadSafeDependencyMap<String, i64> = ThreadSafeDependencyMap::new(&ctx);
    let start = Arc::new(Barrier::new(16));
    let mut threads = Vec::new();

    for _ in 0..16 {
        let ctx = ctx.clone();
        let map = map.clone();
        let start = Arc::clone(&start);
        threads.push(std::thread::spawn(move || {
            start.wait();
            map.observe_dependency(&ctx, "wanted".to_string())
        }));
    }
    for thread in threads {
        assert_eq!(
            thread.join().expect("observer thread"),
            DependencyAvailability::Unavailable
        );
    }

    assert_eq!(map.present_count(), 1);
}

#[cfg(feature = "async")]
#[tokio::test]
async fn async_dependency_availability_tracks_publication() {
    let ctx = AsyncContext::new();
    let map: AsyncDependencyMap<String, i64> = AsyncDependencyMap::new(&ctx);
    let recomputes = Arc::new(AtomicUsize::new(0));
    let reader = ctx.computed_async({
        let map = map.clone();
        let recomputes = Arc::clone(&recomputes);
        move |cx| {
            recomputes.fetch_add(1, Ordering::SeqCst);
            let state = map.observe_dependency(&cx, "wanted".to_string());
            async move { state }
        }
    });

    assert_eq!(
        ctx.get_async(&reader).await,
        DependencyAvailability::Unavailable
    );
    let first = map.handle(&"wanted".to_string()).expect("wanted handle");
    map.publish(&ctx, "other".to_string(), 9);
    assert_eq!(
        ctx.get_async(&reader).await,
        DependencyAvailability::Unavailable
    );
    assert_eq!(recomputes.load(Ordering::SeqCst), 1);
    map.publish(&ctx, "wanted".to_string(), 1);
    assert_eq!(
        ctx.get_async(&reader).await,
        DependencyAvailability::Available(1)
    );
    assert_eq!(recomputes.load(Ordering::SeqCst), 2);
    map.unpublish(&ctx, "wanted".to_string());
    assert_eq!(
        ctx.get_async(&reader).await,
        DependencyAvailability::Unavailable
    );
    assert_eq!(map.handle(&"wanted".to_string()), Some(first));
}
