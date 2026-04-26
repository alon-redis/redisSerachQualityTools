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

## Notes
- The implementation uses RediSearch 2.x APIs with JSON path syntax
- The tool demonstrates concurrent Redis access patterns and connection pool management
- Intended for performance evaluation and Redis module capability testing
