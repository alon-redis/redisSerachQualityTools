# Redis Bookstore Testing Tool

## Overview
This application is a comprehensive Redis performance testing utility designed to evaluate Redis JSON and RediSearch module capabilities through simulation of a bookstore data model. The tool conducts parallel operations for indexing, retrieval, verification, and stress-testing using a variety of Redis commands.

## Key Features
- Dynamic JSON schema generation for bookstore inventory with 35+ fields
- Full-text search indexing with sortable field declarations
- Multi-threaded workload simulation (read/write/verification)
- Robust connection pool management
- Real-time performance metrics reporting
- Customizable chaos testing via random command execution
- Connection resilience and error handling

## Requirements
- Python 3.6+
- Redis server with RedisJSON and RediSearch modules
- Required Python packages:
  - `redis`
  - `faker`

## Installation
```bash
apt-get update
apt-get upgrade -Vy
apt install -y python3-pip
pip3 install redis faker
```

## Usage
```bash
python3 redis_bookstore_tester.py [options]
```

### Command Line Options
- `--redis`: Redis URL (default: redis://localhost:6379)
- `--max-connections`: Maximum Redis connections in pool (default: 10)
- `--max-books`: Number of books to generate (default: 3000)
- `--max-random`: Upper bound for random book IDs (default: 3000)
- `--flush`: Flush Redis database on startup
- `--run-random-cmds`: Enable chaos testing with random commands

## Execution Flow
1. Establishes connection pool to Redis server
2. Creates RediSearch index for bookstore data
3. Writes verification data for health checks
4. Spawns parallel threads for:
   - Data generation and insertion
   - Continuous verification queries
   - Random command execution (optional)
   - Real-time status reporting

## Performance Metrics
- Successful/unsuccessful data verifications
- Successful/unsuccessful writes
- Total random commands executed

## Example
```bash
python3 redis_bookstore_tester.py --redis redis://redis-server:6379 --max-connections 20 --max-books 5000 --run-random-cmds
```

OverviewbookStore is a Redis Search workload generator for a bookstore style dataset.It writes synthetic book records, creates a search index, and runs a verification query while writes are in progress.VariantsbookStore.pyOriginal version. JSON documents. Includes optional random search commands.bookStore_v2.pyHash based version. Random commands removed.bookStore_v2_1.pyHash based version with thread safe counters and a paced verification loop.bookStore_v3.pyJSON based version built from v2.1.bookStore_v3_1.pyJSON based version with cleaner normalization of verification results.RequirementsServer sideRedis or Redis Enterprise with:1. RediSearch2. RedisJSON, needed for JSON versions onlyClient side1. Python 3.9+2. pip3. redis-py4. Faker5. redis-cli, optional for manual query testingPython packagespip install redis FakerRecommended checkpython3 -c "import redis, faker; print(redis.__version__)"Run examplesOriginal JSON versionpython3 bookStore.py --redis redis://localhost:6379 --max-connections 10 --max-books 3000 --max-random 3000 --flushHash versionpython3 bookStore_v2_1.py --redis redis://localhost:6379 --max-connections 10 --max-books 3000 --max-random 3000 --flush --verify-sleep 0.1JSON versionpython3 bookStore_v3_1.py --redis redis://localhost:6379 --max-connections 10 --max-books 3000 --max-random 3000 --flush --verify-sleep 0.1Main arguments--redisRedis URI--max-connectionsMaximum client connections in the pool--max-booksNumber of write operations--max-randomUpper bound for random book ids--flushFlush the database before the run--verify-sleepDelay between verification searches, supported in v2.1 and laterNotes1. JSON versions require RedisJSON and a JSON index.2. Hash versions require a HASH index.3. The verification thread checks that the seeded record remains searchable.4. Use smaller values first to validate connectivity and index creation. 
