# WebSocket Reconnection Checklist

Use this checklist when implementing or auditing WebSocket reliability in your trading bot.

---

## Layer 1: Reconnection

### Basic Reconnection
- [ ] Automatic reconnection on `onclose` event
- [ ] `isIntentionallyClosed` flag to prevent reconnect after manual close
- [ ] Reconnect attempt counter

### Exponential Backoff
- [ ] Base delay configured (recommended: 1000ms)
- [ ] Exponential increase: delay = base × 2^attempt
- [ ] Maximum delay cap (recommended: 30000ms)
- [ ] Jitter added (±20%) to prevent thundering herd

### Retry Limits
- [ ] Maximum retry attempts configured (recommended: 10)
- [ ] Alert/notification when max retries reached
- [ ] Graceful degradation when giving up

### Close Code Handling
- [ ] Code 1000 (normal): reconnect fast
- [ ] Code 1001 (going away): reconnect fast
- [ ] Code 1006 (abnormal): reconnect with backoff
- [ ] Code 1008 (policy/auth): refresh auth, then reconnect
- [ ] Code 1011 (server error): reconnect with backoff
- [ ] Exchange-specific codes documented and handled

---

## Layer 2: Gap Detection

### Sequence Number Tracking
- [ ] Identify sequence field for your exchange (`u`, `seq`, etc.)
- [ ] Store last seen sequence number
- [ ] Compare each message to expected sequence
- [ ] Log gaps when detected
- [ ] Trigger state recovery on gap

### Heartbeat Implementation
- [ ] Heartbeat interval configured per exchange requirements
- [ ] Ping sent on interval
- [ ] Pong response tracked
- [ ] Timeout threshold configured (2× heartbeat interval)
- [ ] Force reconnect on heartbeat timeout

### Exchange Ping Handling
- [ ] Respond to exchange "ping" messages
- [ ] Handle string pings (`"ping"` → `"pong"`)
- [ ] Handle JSON pings (`{ping: X}` → `{pong: X}`)

---

## Layer 3: State Recovery

### REST Verification After Reconnect
- [ ] Fetch open orders via REST
- [ ] Compare to local order state
- [ ] Adopt orphan orders (on exchange, not local)
- [ ] Remove ghost orders (local, not on exchange)
- [ ] Backfill fills (remote.filled > local.filled)

### Position Verification
- [ ] Fetch position via REST
- [ ] Compare to local position
- [ ] Update if drift exceeds threshold
- [ ] Log position corrections

### Sequence Reset
- [ ] Reset sequence counter after reconnect
- [ ] Don't compare first message to old sequence

---

## Exchange-Specific Requirements

### Binance
- [ ] Listen key obtained for user data stream
- [ ] Listen key refresh every 30-60 minutes
- [ ] Respond to Binance ping frames
- [ ] Handle 24-hour mandatory disconnects

### Bybit
- [ ] Ping every 20 seconds
- [ ] Use `seq` field for gap detection
- [ ] Resubscribe after reconnect

### OKX
- [ ] Respond to `"ping"` with `"pong"`
- [ ] Login for private channels after reconnect
- [ ] Channel-based resubscription

### Kraken
- [ ] WebSocket token authentication
- [ ] Token refresh before expiry

---

## Integration Checklist

### Callbacks/Events
- [ ] `onConnect` handler for post-connect setup
- [ ] `onDisconnect` handler for cleanup
- [ ] `onMessage` handler for business logic
- [ ] `onGap` handler for state recovery trigger
- [ ] `onMaxRetries` handler for alerting

### Subscription Management
- [ ] Track active subscriptions
- [ ] Resubscribe after reconnect
- [ ] Handle subscription errors

### Thread Safety (if applicable)
- [ ] Message handler doesn't block
- [ ] State updates are atomic
- [ ] Reconnection doesn't race with message handling

---

## Testing Checklist

### Simulate Disconnects
- [ ] Test manual close: should NOT reconnect
- [ ] Test network disconnect: should reconnect with backoff
- [ ] Test server close (code 1000): should reconnect fast
- [ ] Test auth failure (code 1008): should re-auth then reconnect

### Simulate Gaps
- [ ] Test missed messages: gap handler called
- [ ] Test state recovery: REST snapshot fetched
- [ ] Verify position after recovery

### Stress Testing
- [ ] Multiple rapid disconnects
- [ ] Reconnect during high message volume
- [ ] Recovery with many open orders

---

## Monitoring Checklist

### Metrics to Track
- [ ] Disconnect count (per hour/day)
- [ ] Average reconnect time
- [ ] Gap detection count
- [ ] State corrections count
- [ ] Max retry events

### Alerts
- [ ] Alert on max retries exceeded
- [ ] Alert on frequent disconnects (> N per hour)
- [ ] Alert on large gaps (> N missed messages)
- [ ] Alert on position corrections

### Logging
- [ ] Log all disconnects with close code
- [ ] Log reconnect attempts with timing
- [ ] Log gap detections with sequence numbers
- [ ] Log state recovery results

---

## Quick Reference: Timing Recommendations

| Parameter | Recommended Value | Notes |
|-----------|------------------|-------|
| Base backoff | 1000ms | Starting delay |
| Max backoff | 30000ms | Cap for backoff growth |
| Jitter | ±20% | Prevents thundering herd |
| Max retries | 10 | Before giving up |
| Heartbeat interval | 15000-30000ms | Exchange dependent |
| Heartbeat timeout | 2× interval | Time without pong |
| Recovery timeout | 10000ms | Max time for REST recovery |

---

## Code Review Questions

Before merging WebSocket code, verify:

1. **What happens if disconnect occurs during order placement?**
   - Answer: Should use idempotency key and verify via REST

2. **What happens if state recovery fails?**
   - Answer: Should not enable trading, alert operator

3. **What happens if exchange is completely down?**
   - Answer: Should respect max retries, alert, pause trading

4. **What happens if messages arrive during recovery?**
   - Answer: Should buffer or ignore until recovery complete

5. **What happens on first connect (no previous sequence)?**
   - Answer: Should not trigger gap handler for first message
