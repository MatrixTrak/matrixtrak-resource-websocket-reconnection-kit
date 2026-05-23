# WebSocket Reconnection Kit

Automatic reconnection, message gap detection, and state recovery for trading bot WebSocket connections.

## Contents

| File | Purpose |
|------|---------|
| `websocket-manager.ts` | TypeScript WebSocket manager with reconnection, heartbeat, and gap detection |
| `reconnection-checklist.md` | Step-by-step checklist for implementing reliable WebSocket handling |

## Quick Start

1. Copy `websocket-manager.ts` into your project
2. Implement the exchange-specific handlers (auth, subscribe, message parsing)
3. Add state recovery callback for post-reconnect reconciliation
4. Configure heartbeat interval per exchange requirements

## The Three-Layer Defense

### Layer 1: Reconnection
Automatic reconnection with exponential backoff and jitter. Handles clean disconnects, network errors, and exchange-initiated closes.

### Layer 2: Gap Detection
Sequence number tracking to detect missed messages. When a gap is detected, triggers state recovery.

### Layer 3: State Recovery
REST snapshot after reconnect to verify local state matches exchange. Catches fills, cancellations, and position changes that occurred during disconnect.

## Exchange Configuration

| Exchange | Heartbeat | Mandatory Disconnect | Special Requirements |
|----------|-----------|---------------------|----------------------|
| Binance | 30s | Every 24h | Listen key refresh every 60min |
| Bybit | 20s | Every 24h | seq field for gaps |
| OKX | Respond to ping | Varies | Login for private channels |

## Key Principle

**Never trust WebSocket alone.** After any reconnect, verify state via REST API. WebSocket is for speed, REST is for truth.

## Related Blog Post

[WebSocket Disconnects in Trading Bots: Reconnection That Actually Works](https://matrixtrak.com/blog/websocket-disconnects-trading-bots-reconnection)
