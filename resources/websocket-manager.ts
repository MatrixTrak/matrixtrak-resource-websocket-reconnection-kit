// @ts-nocheck
/**
 * WebSocket Manager
 * 
 * Reliable WebSocket connection for trading bots with:
 * - Automatic reconnection with exponential backoff
 * - Message gap detection via sequence numbers
 * - Heartbeat/ping-pong handling
 * - State recovery triggers
 * 
 * @see /blog/websocket-disconnects-trading-bots-reconnection
 */

import WebSocket from 'ws'; // or use native WebSocket in browser

// ============================================================================
// Types
// ============================================================================

interface WebSocketConfig {
  url: string;
  heartbeatInterval: number;    // ms between heartbeats (typically 15000-30000)
  heartbeatTimeout: number;     // ms without pong before considering stale
  maxReconnectAttempts: number; // Max attempts before giving up
  baseBackoff: number;          // Base delay for exponential backoff (ms)
  maxBackoff: number;           // Maximum backoff delay (ms)
}

interface ExchangeMessage {
  // Common fields - extend based on your exchange
  e?: string;        // Event type
  E?: number;        // Event timestamp
  u?: number;        // Update ID / sequence number
  seq?: number;      // Alternative sequence field (Bybit)
  [key: string]: any;
}

type MessageHandler = (message: ExchangeMessage) => void;
type ConnectionHandler = () => void;
type ErrorHandler = (error: Error) => void;
type GapHandler = (lastSeen: number, current: number) => void;
type MaxRetriesHandler = () => void;

// ============================================================================
// WebSocket Manager
// ============================================================================

class WebSocketManager {
  private ws: WebSocket | null = null;
  private config: WebSocketConfig;
  private reconnectAttempts = 0;
  private isIntentionallyClosed = false;
  private isConnected = false;
  
  // Heartbeat
  private heartbeatTimer: NodeJS.Timer | null = null;
  private lastPong: number = Date.now();
  
  // Gap detection
  private lastSequence: number | null = null;
  
  // Event handlers
  private onMessage: MessageHandler = () => {};
  private onConnect: ConnectionHandler = () => {};
  private onDisconnect: ConnectionHandler = () => {};
  private onError: ErrorHandler = () => {};
  private onGap: GapHandler = () => {};
  private onMaxRetries: MaxRetriesHandler = () => {};
  
  constructor(config: Partial<WebSocketConfig> = {}) {
    this.config = {
      url: '',
      heartbeatInterval: 15000,
      heartbeatTimeout: 30000,
      maxReconnectAttempts: 10,
      baseBackoff: 1000,
      maxBackoff: 30000,
      ...config,
    };
  }
  
  // ---------------------------------------------------------------------------
  // Event Registration
  // ---------------------------------------------------------------------------
  
  setMessageHandler(handler: MessageHandler): this {
    this.onMessage = handler;
    return this;
  }
  
  setConnectHandler(handler: ConnectionHandler): this {
    this.onConnect = handler;
    return this;
  }
  
  setDisconnectHandler(handler: ConnectionHandler): this {
    this.onDisconnect = handler;
    return this;
  }
  
  setErrorHandler(handler: ErrorHandler): this {
    this.onError = handler;
    return this;
  }
  
  setGapHandler(handler: GapHandler): this {
    this.onGap = handler;
    return this;
  }
  
  setMaxRetriesHandler(handler: MaxRetriesHandler): this {
    this.onMaxRetries = handler;
    return this;
  }
  
  // ---------------------------------------------------------------------------
  // Connection Management
  // ---------------------------------------------------------------------------
  
  async connect(url?: string): Promise<void> {
    if (url) {
      this.config.url = url;
    }
    
    if (!this.config.url) {
      throw new Error('WebSocket URL not configured');
    }
    
    this.isIntentionallyClosed = false;
    
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.config.url);
        
        this.ws.onopen = () => {
          console.log('[WS] Connected');
          this.isConnected = true;
          this.reconnectAttempts = 0;
          this.startHeartbeat();
          this.onConnect();
          resolve();
        };
        
        this.ws.onclose = (event) => {
          console.log(`[WS] Closed: code=${event.code}, reason=${event.reason || 'none'}`);
          this.isConnected = false;
          this.stopHeartbeat();
          this.onDisconnect();
          
          if (!this.isIntentionallyClosed) {
            this.handleReconnect(event.code);
          }
        };
        
        this.ws.onerror = (event) => {
          const error = new Error('WebSocket error');
          console.error('[WS] Error:', error.message);
          this.onError(error);
          // onclose will fire after onerror
        };
        
        this.ws.onmessage = (event) => {
          this.handleRawMessage(event.data as string);
        };
        
      } catch (error) {
        reject(error);
      }
    });
  }
  
  close(): void {
    console.log('[WS] Intentional close');
    this.isIntentionallyClosed = true;
    this.stopHeartbeat();
    this.ws?.close(1000, 'Client closing');
    this.ws = null;
    this.isConnected = false;
  }
  
  send(data: object | string): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn('[WS] Cannot send: not connected');
      return;
    }
    
    const payload = typeof data === 'string' ? data : JSON.stringify(data);
    this.ws.send(payload);
  }
  
  getStatus(): { connected: boolean; reconnectAttempts: number } {
    return {
      connected: this.isConnected,
      reconnectAttempts: this.reconnectAttempts,
    };
  }
  
  // ---------------------------------------------------------------------------
  // Message Handling
  // ---------------------------------------------------------------------------
  
  private handleRawMessage(raw: string): void {
    // Handle ping/pong (exchange sends "ping" string)
    if (raw === 'ping') {
      this.send('pong');
      return;
    }
    
    try {
      const data = JSON.parse(raw);
      
      // Handle Binance-style ping
      if (data.ping) {
        this.send({ pong: data.ping });
        return;
      }
      
      // Handle pong response
      if (data.pong || raw === 'pong') {
        this.lastPong = Date.now();
        return;
      }
      
      // Check for message gaps
      this.checkSequence(data);
      
      // Forward to handler
      this.onMessage(data);
      
    } catch (error) {
      console.error('[WS] Failed to parse message:', error);
    }
  }
  
  // ---------------------------------------------------------------------------
  // Gap Detection
  // ---------------------------------------------------------------------------
  
  private checkSequence(message: ExchangeMessage): void {
    // Get sequence number (different exchanges use different field names)
    const seq = message.u ?? message.seq;
    
    if (seq === undefined) return;
    
    if (this.lastSequence !== null) {
      const expected = this.lastSequence + 1;
      
      if (seq > expected) {
        const missed = seq - expected;
        console.warn(`[WS] Gap detected: missed ${missed} messages (${this.lastSequence} → ${seq})`);
        this.onGap(this.lastSequence, seq);
      }
    }
    
    this.lastSequence = seq;
  }
  
  resetSequence(): void {
    this.lastSequence = null;
    console.log('[WS] Sequence tracking reset');
  }
  
  // ---------------------------------------------------------------------------
  // Heartbeat
  // ---------------------------------------------------------------------------
  
  private startHeartbeat(): void {
    this.lastPong = Date.now();
    
    this.heartbeatTimer = setInterval(() => {
      const timeSincePong = Date.now() - this.lastPong;
      
      if (timeSincePong > this.config.heartbeatTimeout) {
        console.warn('[WS] Heartbeat timeout, forcing reconnect');
        this.ws?.close(4001, 'Heartbeat timeout');
        return;
      }
      
      // Send ping
      this.send({ op: 'ping' });
      
    }, this.config.heartbeatInterval);
    
    console.log(`[WS] Heartbeat started (${this.config.heartbeatInterval}ms interval)`);
  }
  
  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
      console.log('[WS] Heartbeat stopped');
    }
  }
  
  // ---------------------------------------------------------------------------
  // Reconnection
  // ---------------------------------------------------------------------------
  
  private handleReconnect(closeCode: number): void {
    // Handle specific close codes
    switch (closeCode) {
      case 1000: // Normal close
      case 1001: // Going away
        // Server closed cleanly, reconnect quickly
        setTimeout(() => this.reconnect(), 100);
        return;
        
      case 1008: // Policy violation (often auth issue)
        console.log('[WS] Auth issue detected, will need re-authentication');
        // Fall through to normal reconnect - handler can refresh auth
        break;
        
      case 4001: // Our custom code for heartbeat timeout
        // Already logged, use normal backoff
        break;
    }
    
    this.reconnect();
  }
  
  private reconnect(): void {
    if (this.reconnectAttempts >= this.config.maxReconnectAttempts) {
      console.error('[WS] Max reconnect attempts reached');
      this.onMaxRetries();
      return;
    }
    
    const delay = this.calculateBackoff(this.reconnectAttempts);
    this.reconnectAttempts++;
    
    console.log(`[WS] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.config.maxReconnectAttempts})`);
    
    setTimeout(async () => {
      try {
        await this.connect();
      } catch (error) {
        console.error('[WS] Reconnect failed:', error);
        // Will trigger another reconnect via onclose
      }
    }, delay);
  }
  
  private calculateBackoff(attempt: number): number {
    // Exponential backoff with jitter
    const exponential = Math.min(
      this.config.baseBackoff * Math.pow(2, attempt),
      this.config.maxBackoff
    );
    
    // Add ±20% jitter to prevent thundering herd
    const jitter = exponential * 0.2 * (Math.random() * 2 - 1);
    
    return Math.floor(exponential + jitter);
  }
}

// ============================================================================
// State Recovery Helper
// ============================================================================

interface Exchange {
  fetchOpenOrders(symbol?: string): Promise<any[]>;
  fetchOrder(clientOrderId: string): Promise<any>;
  fetchPosition(symbol: string): Promise<any>;
}

interface TradingState {
  symbol: string;
  openOrders: any[];
  position: { size: number; entryPrice: number };
  findOrder(clientOrderId: string): any | undefined;
  adoptOrder(order: any): Promise<void>;
  processFill(clientOrderId: string, filled: number, price: number): Promise<void>;
  removeOrder(clientOrderId: string): Promise<void>;
}

/**
 * Recover state after WebSocket reconnection.
 * Always call this after reconnect before resuming trading.
 */
async function recoverStateAfterReconnect(
  exchange: Exchange,
  state: TradingState
): Promise<{ ordersReconciled: number; positionCorrected: boolean }> {
  console.log('[Recovery] Starting post-reconnect state recovery...');
  
  let ordersReconciled = 0;
  let positionCorrected = false;
  
  // 1. Fetch current open orders via REST
  const exchangeOrders = await exchange.fetchOpenOrders(state.symbol);
  
  // 2. Check for new orders (created while disconnected)
  for (const remote of exchangeOrders) {
    const local = state.findOrder(remote.clientOrderId);
    
    if (!local) {
      console.log(`[Recovery] Adopting order: ${remote.clientOrderId}`);
      await state.adoptOrder(remote);
      ordersReconciled++;
    } else if (remote.filled > local.filled) {
      console.log(`[Recovery] Backfilling: ${local.clientOrderId}`);
      await state.processFill(remote.clientOrderId, remote.filled, remote.price);
      ordersReconciled++;
    }
  }
  
  // 3. Check for orders that closed while disconnected
  const remoteIds = new Set(exchangeOrders.map(o => o.clientOrderId));
  
  for (const local of state.openOrders) {
    if (!remoteIds.has(local.clientOrderId)) {
      try {
        const historical = await exchange.fetchOrder(local.clientOrderId);
        
        if (historical?.status === 'filled') {
          await state.processFill(local.clientOrderId, historical.filled, historical.price || 0);
          ordersReconciled++;
        } else if (historical?.status === 'canceled' || historical?.status === 'expired') {
          await state.removeOrder(local.clientOrderId);
          ordersReconciled++;
        }
      } catch (error) {
        console.warn(`[Recovery] Could not fetch order ${local.clientOrderId}:`, error);
      }
    }
  }
  
  // 4. Verify position
  const remotePosition = await exchange.fetchPosition(state.symbol);
  if (Math.abs(remotePosition.size - state.position.size) > 0.0001) {
    console.warn(`[Recovery] Position drift: ${state.position.size} → ${remotePosition.size}`);
    state.position.size = remotePosition.size;
    state.position.entryPrice = remotePosition.entryPrice;
    positionCorrected = true;
  }
  
  console.log(`[Recovery] Complete: ${ordersReconciled} orders reconciled, position ${positionCorrected ? 'corrected' : 'unchanged'}`);
  
  return { ordersReconciled, positionCorrected };
}

// ============================================================================
// Full Reconnection Sequence
// ============================================================================

interface ReconnectionContext {
  wsManager: WebSocketManager;
  exchange: Exchange;
  state: TradingState;
  subscriptions: string[];
  authenticate: () => Promise<void>;
}

/**
 * Complete reconnection sequence including auth, recovery, and resubscription.
 */
async function fullReconnectionSequence(ctx: ReconnectionContext): Promise<void> {
  const { wsManager, exchange, state, subscriptions, authenticate } = ctx;
  
  console.log('[Reconnect] Starting full reconnection sequence...');
  
  // 1. Connect WebSocket
  await wsManager.connect();
  
  // 2. Authenticate if required
  await authenticate();
  
  // 3. Recover state via REST
  await recoverStateAfterReconnect(exchange, state);
  
  // 4. Reset sequence tracking (new connection = new sequence)
  wsManager.resetSequence();
  
  // 5. Resubscribe to channels
  for (const sub of subscriptions) {
    wsManager.send({ op: 'subscribe', args: [sub] });
  }
  
  console.log('[Reconnect] Full sequence complete');
}

// ============================================================================
// Exports
// ============================================================================

export {
  WebSocketManager,
  WebSocketConfig,
  ExchangeMessage,
  recoverStateAfterReconnect,
  fullReconnectionSequence,
  ReconnectionContext,
};

export default WebSocketManager;
