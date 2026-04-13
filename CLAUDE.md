# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Study With Someone (SWS)** - A WebSocket-based WebRTC peer matching service built with Spring Boot. The application pairs peers in real-time for video/audio streaming using a publisher-subscriber model with Redis-backed queues and distributed locking for concurrent safety.

## Build & Development Commands

### Building
```bash
# Build with tests
./gradlew build

# Build without tests (used in CI/CD)
./gradlew build -x test

# Clean build artifacts
./gradlew clean
```

### Running Locally
```bash
# Run the application (requires .env file with Redis credentials)
./gradlew bootRun

# Or build and run the JAR
./gradlew bootJar
java -jar build/libs/sws-0.0.1-SNAPSHOT.jar
```

### Testing
```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests "com.koa.sws.service.RedisQueueServiceTest"
```

### Docker
```bash
# Build the application first
./gradlew build -x test

# Build Docker image
docker build -t sws:latest .

# Run container with environment file
docker run -d --env-file .env -p 8080:8080 --name sws sws:latest
```

## Core Architecture

### Peer Matching Flow

The system uses a **dual-queue publisher-subscriber matching model**:

1. **Connection**: Client connects via WebSocket to `/signal` endpoint
2. **Registration**: Peer registers as BOTH publisher AND subscriber simultaneously
3. **Matching**: System attempts to match from opposing queue (publisher ↔ subscriber)
4. **Queue Wait**: If no match found, peer is added to appropriate queue(s)
5. **Match Confirmation**: Both peers receive PUBLISH/SUBSCRIBE messages with partner IDs
6. **WebRTC Signaling**: Peers exchange OFFER/ANSWER/ICE messages via the server relay
7. **Disconnection & Rematch**: When peer disconnects, partner is automatically rematched

### Service Layer Architecture

**MatchService** (`service/MatchService.java`)
- Core orchestration service for peer matching
- Manages dual registration (publisher + subscriber)
- Validates sessions and prevents invalid matches (self-matching, circular dependencies)
- Handles rematching when peers disconnect
- Relays WebRTC signaling messages (OFFER/ANSWER/ICE) between matched peers

**RedisQueueService** (`service/RedisQueueService.java`)
- Manages two Redis-backed FIFO queues: `sws:publishQueue` and `sws:subscribeQueue`
- Uses `@DistributedLock` annotation on pop operations for concurrent safety
- Ensures atomic queue operations across multiple application instances

**SessionService** (`service/SessionService.java`)
- Maintains two concurrent maps:
  - `websocketSessions`: Maps peer ID → WebSocketSession
  - `peerSessions`: Maps peer ID → PeerSession (relationship metadata)
- Tracks publisher/subscriber relationships
- Provides session lifecycle management

### Distributed Locking System

**@DistributedLock Annotation** (`aop/DistributedLock.java`)
- Redisson-based distributed lock for concurrent operations
- Default parameters: `waitTime=5s`, `leaseTime=3s`
- Supports Spring EL expressions for dynamic lock keys
- Example: `@DistributedLock(key = "'publishQueue'")`

**DistributedLockAop** (`aop/DistributedLockAop.java`)
- AOP aspect that intercepts `@DistributedLock` methods
- Acquires Redisson RLock before execution
- Automatically releases lock in finally block (prevents deadlocks)
- Used on `RedisQueueService.popFrom*Queue()` methods to prevent duplicate peer allocation

**AopForTransaction** (`aop/AopForTransaction.java`)
- Ensures distributed lock operations execute in new transactions (`REQUIRES_NEW`)
- Prevents lock/transaction deadlocks

### Message Types

| Type | Direction | Purpose |
|------|-----------|---------|
| `PUBLISH` | Server → Peer | "You are now publishing to peer X" |
| `SUBSCRIBE` | Server → Peer | "You are now subscribing from peer X" |
| `OFFER` | Peer → Peer | WebRTC SDP offer (relayed) |
| `ANSWER` | Peer → Peer | WebRTC SDP answer (relayed) |
| `ICE` | Peer → Peer | ICE candidate (relayed) |
| `LEAVE` | Server → Peer | Partner disconnected |
| `ERROR` | Server → Peer | Error notification |

### Critical Matching Logic

**Queue Validation** (`MatchService.getWaitingSubscriber()/getWaitingPublisher()`):
- Loops through queue candidates until valid match found
- Skips invalid sessions (closed connections)
- Prevents self-matching (peer cannot match with itself)
- Prevents circular dependencies (subscriber cannot become publisher of their own publisher)
- Restores invalid candidates to queue for re-matching

**Match Execution** (`MatchService.match()`):
1. Validates both sessions are still open
2. Sends PUBLISH message to publisher with subscriber's ID
3. Sends SUBSCRIBE message to subscriber with publisher's ID
4. Updates PeerSession bidirectional relationships

## Configuration

### Environment Variables (.env)
```properties
REDIS_HOST=<redis-host>
REDIS_PORT=<redis-port>
REDIS_PASSWORD=<redis-password>
REDIS_TIMEOUT=60000
FRONT_URL=<frontend-origin-url>
```

### CORS Configuration
WebSocket endpoint `/signal` allows origins from:
- `http://localhost:3000` (development)
- `${FRONT_URL}` (production, configured via .env)

## CI/CD Pipeline

**Workflow**: `.github/workflows/deploy_backend.yml`

Triggers on push/PR to `develop` branch:

1. **Build Stage**:
   - Checkout code
   - Inject `.env` from GitHub Secrets (`ENV_FILE`)
   - Setup JDK 17 (Temurin)
   - Build with Gradle (skip tests)
   - Build Docker image
   - Push to Docker Hub

2. **Deploy Stage**:
   - SSH to production server
   - Pull latest image
   - Stop/remove existing container
   - Start new container with environment variables
   - Expose port 8080

**Required GitHub Secrets**:
- `ENV_FILE` - Complete .env file content
- `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`, `DOCKER_IMAGE_NAME`
- `SERVER_HOST`, `SERVER_USER`, `SERVER_SSH_KEY`

## Important Implementation Notes

### When Working with Matching Logic

1. **Always validate sessions** before sending messages - use `session.isOpen()` check
2. **Queue operations must be locked** - use `@DistributedLock` for any new queue operations
3. **Prevent matching loops** - validate that `peer.publisher != newSubscriber` and vice versa
4. **Handle disconnections gracefully** - always notify partner peer with LEAVE message before cleanup
5. **Restore to queue on validation failure** - invalid candidates should be re-queued, not dropped

### When Adding New Services

1. Use `@RequiredArgsConstructor` (Lombok) for dependency injection
2. Add `@Slf4j` for logging
3. For Redis operations requiring atomicity, use `@DistributedLock`
4. Follow existing transaction propagation patterns with `AopForTransaction`

### WebSocket Message Handling

All WebSocket messages are JSON-serialized `SignalMessage` objects. When adding new message types:
1. Add enum value to `MessageType.java`
2. Add handler case in `SignalingWebSocketHandler.handleTextMessage()`
3. Ensure target session exists and is open before sending
4. Log appropriately with emoji prefixes (⭐ for connection, 🔴 for disconnect)

### Testing Distributed Systems

See `RedisQueueServiceTest.java` for examples of:
- Testing distributed locks under concurrency (50 threads)
- Validating atomic queue operations
- Ensuring no duplicate pops across threads

Run concurrent tests multiple times to catch race conditions:
```bash
./gradlew test --tests "RedisQueueServiceTest" --rerun-tasks
```

## Dependencies

- **Spring Boot 3.5.7** (Web, WebSocket, Data Redis, AOP)
- **Redisson 3.52.0** - Distributed locks and Redis client
- **Lombok** - Boilerplate reduction
- **JUnit 5** - Testing framework

## Project Structure

```
src/main/java/com/koa/sws/
├── SwsApplication.java          # Entry point
├── handler/
│   └── SignalingWebSocketHandler.java  # WebSocket message handler
├── config/
│   ├── WebSocketConfig.java     # WebSocket endpoint configuration
│   └── RedissonConfig.java      # Redisson client setup
├── service/
│   ├── MatchService.java        # Core matching orchestration
│   ├── RedisQueueService.java   # Queue operations with locks
│   └── SessionService.java      # Session lifecycle management
├── aop/
│   ├── DistributedLock.java     # Lock annotation
│   ├── DistributedLockAop.java  # Lock aspect
│   ├── AopForTransaction.java   # Transaction helper
│   └── CustomSpringELParser.java # Dynamic lock key parser
└── model/
    ├── SignalMessage.java       # WebSocket message structure
    ├── PeerSession.java         # Peer relationship metadata
    ├── MessageType.java         # Message type enum
    └── Role.java                # Peer role enum
```
