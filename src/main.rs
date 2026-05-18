use resilient::pipeline::Pipeline;
use resilient::retry_policy::RetryPolicy;
use resilient::timeout::TimeoutPolicy;
use resilient::circuit_breaker::BreakerPolicy;
use std::time::Duration;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("=== Testing Resilient Library Features ===\n");

    test_success().await;
    test_timeout().await;
    test_retry().await;
    test_circuit_breaker().await;
    test_combined_pipeline().await;

    println!("\n=== All tests completed ===");
}

// Test 1: Happy path — operation succeeds immediately
async fn test_success() {
    println!("Test 1: Success case");
    let pipeline = Pipeline::default()
        .with_timeout(TimeoutPolicy::default().with_timeout(Duration::from_secs(5)));

    let result = pipeline.run(&mut || async {
        Ok::<String, String>("success".into())
    }).await;

    match result {
        Ok(val) => println!("  ✓ Got: {}\n", val),
        Err(e) => eprintln!("  ✗ Failed: {:?}\n", e),
    }
}

// Test 2: Timeout — operation takes too long
async fn test_timeout() {
    println!("Test 2: Timeout policy");
    let policy = TimeoutPolicy::default().with_timeout(Duration::from_millis(500));

    let result = policy.run(&mut || async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok::<String, String>("success".into())
    }).await;

    match result {
        Ok(_) => println!("  ✗ Should have timed out\n"),
        Err(e) => println!("  ✓ Correctly timed out: {:?}\n", e),
    }
}

// Test 3: Retry — operation fails then succeeds
async fn test_retry() {
    println!("Test 3: Retry policy");
    let attempt = Arc::new(AtomicU32::new(0));
    let attempt_clone = attempt.clone();

    let policy = RetryPolicy::default().with_max_retries(3);

    let result = policy.run(&mut || {
        let attempt_clone = attempt_clone.clone();
        async move {
            let count = attempt_clone.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                Err::<String, String>(format!("attempt {}: failed", count))
            } else {
                Ok("success after retries".into())
            }
        }
    }).await;

    match result {
        Ok(val) => println!("  ✓ {}, took {} attempts\n", val, attempt.load(Ordering::SeqCst)),
        Err(e) => eprintln!("  ✗ Failed: {}\n", e),
    }
}

// Test 4: Circuit breaker — trips after consecutive failures
async fn test_circuit_breaker() {
    println!("Test 4: Circuit breaker policy");
    let breaker = BreakerPolicy::default().with_failure_threshold(3);

    // First 3 operations fail
    for i in 0..3 {
        let result: Result<String, _> = breaker.run(&mut || async {
            Err::<String, String>("simulated error".into())
        }).await;

        if result.is_err() {
            println!("  Attempt {}: Failed as expected", i + 1);
        }
    }

    // Next operation should be rejected by the circuit breaker
    let result: Result<String, _> = breaker.run(&mut || async {
        Ok::<String, String>("success".into())
    }).await;

    if result.is_err() {
        println!("  ✓ Circuit breaker opened after failures\n");
    } else {
        println!("  ✗ Circuit breaker should be open\n");
    }
}

// Test 5: Combined pipeline with multiple policies
async fn test_combined_pipeline() {
    println!("Test 5: Combined pipeline (retry + timeout + circuit breaker)");
    let attempt = Arc::new(AtomicU32::new(0));
    let attempt_clone = attempt.clone();

    let pipeline = Pipeline::default()
        .with_retry(RetryPolicy::default().with_max_retries(2))
        .with_timeout(TimeoutPolicy::default().with_timeout(Duration::from_secs(5)))
        .with_circuit_breaker(BreakerPolicy::default());

    let result = pipeline.run(&mut || {
        let attempt_clone = attempt_clone.clone();
        async move {
            let count = attempt_clone.fetch_add(1, Ordering::SeqCst);
            if count < 1 {
                Err::<String, String>("temporary failure".into())
            } else {
                Ok("recovered successfully".into())
            }
        }
    }).await;

    match result {
        Ok(val) => println!("  ✓ Pipeline result: {}, retried {} times\n", val, attempt.load(Ordering::SeqCst)),
        Err(e) => eprintln!("  ✗ Pipeline failed: {:?}\n", e),
    }
}

