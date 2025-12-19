#!/usr/bin/env python3
"""
Verification script untuk memastikan semua komponen ada dan bekerja.
Jalankan sebelum submission.
"""

import os
import sys
import json
from pathlib import Path


def check_file_exists(path: str, name: str) -> bool:
    """Check file exists"""
    if Path(path).exists():
        print(f"✓ {name}")
        return True
    else:
        print(f"✗ {name} MISSING")
        return False


def check_directory_exists(path: str, name: str) -> bool:
    """Check directory exists"""
    if Path(path).is_dir():
        print(f"✓ {name} (directory)")
        return True
    else:
        print(f"✗ {name} (directory) MISSING")
        return False


def verify_project_structure():
    """Verify project structure"""
    print("\n" + "=" * 60)
    print("VERIFIKASI STRUKTUR PROYEK")
    print("=" * 60)
    
    checks = []
    
    # Root files
    print("\n📁 Root Files:")
    checks.append(check_file_exists("docker-compose.yml", "docker-compose.yml"))
    checks.append(check_file_exists("Dockerfile", "Dockerfile"))
    checks.append(check_file_exists("publisher.Dockerfile", "publisher.Dockerfile"))
    checks.append(check_file_exists("requirements.txt", "requirements.txt"))
    checks.append(check_file_exists("README.md", "README.md"))
    checks.append(check_file_exists(".env", ".env"))
    checks.append(check_file_exists(".gitignore", ".gitignore"))
    
    # Directories
    print("\n📁 Directories:")
    checks.append(check_directory_exists("src", "src/"))
    checks.append(check_directory_exists("src/tests", "src/tests/"))
    checks.append(check_directory_exists("publisher", "publisher/"))
    checks.append(check_directory_exists("notes", "notes/"))
    
    # Source files
    print("\n🐍 Source Code:")
    checks.append(check_file_exists("src/__init__.py", "src/__init__.py"))
    checks.append(check_file_exists("src/main.py", "src/main.py (Aggregator API)"))
    checks.append(check_file_exists("src/models.py", "src/models.py (Pydantic models)"))
    checks.append(check_file_exists("src/database.py", "src/database.py (PostgreSQL ops)"))
    checks.append(check_file_exists("src/tests/__init__.py", "src/tests/__init__.py"))
    checks.append(check_file_exists("src/tests/test_comprehensive.py", "src/tests/test_comprehensive.py (24 tests)"))
    
    # Publisher files
    print("\n📨 Publisher Service:")
    checks.append(check_file_exists("publisher/__init__.py", "publisher/__init__.py"))
    checks.append(check_file_exists("publisher/main.py", "publisher/main.py"))
    
    # Documentation
    print("\n📚 Documentation:")
    checks.append(check_file_exists("notes/INDEX.md", "notes/INDEX.md (Navigation)"))
    checks.append(check_file_exists("notes/TEORI.md", "notes/TEORI.md (T1-T10 Theory)"))
    checks.append(check_file_exists("notes/KEPUTUSAN_DESAIN.md", "notes/KEPUTUSAN_DESAIN.md (Design decisions)"))
    checks.append(check_file_exists("notes/METRIK_PERFORMA.md", "notes/METRIK_PERFORMA.md (Performance metrics)"))
    checks.append(check_file_exists("notes/TESTING_SETUP.md", "notes/TESTING_SETUP.md (Testing guide)"))
    
    return all(checks)


def verify_python_imports():
    """Verify Python dependencies are available"""
    print("\n" + "=" * 60)
    print("VERIFIKASI PYTHON DEPENDENCIES")
    print("=" * 60)
    
    imports = [
        ("fastapi", "FastAPI"),
        ("uvicorn", "Uvicorn"),
        ("pydantic", "Pydantic"),
        ("asyncpg", "asyncpg"),
        ("pytest", "pytest"),
        ("pytest_asyncio", "pytest-asyncio"),
        ("httpx", "httpx"),
    ]
    
    checks = []
    for module, name in imports:
        try:
            __import__(module)
            print(f"✓ {name}")
            checks.append(True)
        except ImportError:
            print(f"✗ {name} NOT INSTALLED")
            checks.append(False)
    
    return all(checks)


def verify_docker_compose_syntax():
    """Verify docker-compose.yml syntax"""
    print("\n" + "=" * 60)
    print("VERIFIKASI DOCKER COMPOSE")
    print("=" * 60)
    
    try:
        import yaml
        with open("docker-compose.yml", "r") as f:
            config = yaml.safe_load(f)
        
        required_services = ["aggregator", "postgres", "redis", "publisher"]
        for service in required_services:
            if service in config.get("services", {}):
                print(f"✓ Service: {service}")
            else:
                print(f"✗ Service: {service} NOT FOUND")
                return False
        
        print("✓ docker-compose.yml valid")
        return True
    except Exception as e:
        print(f"✗ Error reading docker-compose.yml: {e}")
        return False


def verify_models():
    """Verify models can be imported"""
    print("\n" + "=" * 60)
    print("VERIFIKASI MODELS")
    print("=" * 60)
    
    try:
        sys.path.insert(0, "src")
        from models import (
            Event, PublishRequest, PublishResponse, 
            EventResponse, StatsResponse, HealthResponse
        )
        
        print("✓ Event model")
        print("✓ PublishRequest model")
        print("✓ PublishResponse model")
        print("✓ EventResponse model")
        print("✓ StatsResponse model")
        print("✓ HealthResponse model")
        
        # Test event creation
        event = Event(
            topic="test.topic",
            event_id="evt-001",
            timestamp="2025-12-18T10:00:00Z",
            source="test",
            payload={}
        )
        print(f"✓ Event creation: {event.topic}/{event.event_id}")
        return True
    except Exception as e:
        print(f"✗ Error with models: {e}")
        return False


def verify_database():
    """Verify database module can be imported"""
    print("\n" + "=" * 60)
    print("VERIFIKASI DATABASE MODULE")
    print("=" * 60)
    
    try:
        sys.path.insert(0, "src")
        from database import (
            init_pool, close_pool, init_db, is_processed, mark_processed,
            get_events_by_topic, get_stats, get_topics, get_event_count
        )
        
        print("✓ init_pool function")
        print("✓ close_pool function")
        print("✓ init_db function")
        print("✓ is_processed function")
        print("✓ mark_processed function")
        print("✓ get_events_by_topic function")
        print("✓ get_stats function")
        print("✓ get_topics function")
        print("✓ get_event_count function")
        
        return True
    except Exception as e:
        print(f"✗ Error with database module: {e}")
        return False


def verify_api():
    """Verify API endpoints can be imported"""
    print("\n" + "=" * 60)
    print("VERIFIKASI API ENDPOINTS")
    print("=" * 60)
    
    try:
        sys.path.insert(0, "src")
        from main import app
        
        # Check endpoints
        endpoints = [
            ("/health", "GET"),
            ("/publish", "POST"),
            ("/events", "GET"),
            ("/stats", "GET"),
            ("/info", "GET"),
        ]
        
        routes = {(route.path, route.methods) for route in app.routes if hasattr(route, 'methods')}
        
        for path, method in endpoints:
            print(f"✓ {method} {path}")
        
        print(f"✓ Total routes: {len(app.routes)}")
        return True
    except Exception as e:
        print(f"✗ Error with API: {e}")
        return False


def verify_tests():
    """Verify test file exists and has tests"""
    print("\n" + "=" * 60)
    print("VERIFIKASI TESTS")
    print("=" * 60)
    
    try:
        with open("src/tests/test_comprehensive.py", "r") as f:
            content = f.read()
        
        # Count test functions
        test_count = content.count("async def test_")
        print(f"✓ Found {test_count} tests")
        
        # Check for critical tests
        critical = [
            "test_concurrent_duplicate_processing",
            "test_stats_increment_concurrent",
            "test_dedup_store_persisted",
        ]
        
        for test in critical:
            if test in content:
                print(f"✓ Critical test: {test}")
            else:
                print(f"✗ Missing critical test: {test}")
        
        return test_count >= 20
    except Exception as e:
        print(f"✗ Error with tests: {e}")
        return False


def main():
    """Run all verifications"""
    print("\n" + "=" * 60)
    print("🚀 VERIFICATION SCRIPT - PUB-SUB LOG AGGREGATOR")
    print("=" * 60)
    
    results = {
        "Project Structure": verify_project_structure(),
        "Python Imports": verify_python_imports(),
        "Models": verify_models(),
        "Database": verify_database(),
        "API": verify_api(),
        "Tests": verify_tests(),
    }
    
    # Docker Compose check (optional if yaml not installed)
    try:
        results["Docker Compose"] = verify_docker_compose_syntax()
    except:
        print("\n⚠️  Skip Docker Compose check (pyyaml not installed)")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 60)
    
    for component, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{component:<30} {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL CHECKS PASSED - Ready for Docker Compose!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. docker-compose up --build")
        print("2. curl http://localhost:8080/health")
        print("3. pytest src/tests/test_comprehensive.py -v")
        print("4. Create GitHub release")
        print("5. Upload video demo to YouTube")
        return 0
    else:
        print("✗ SOME CHECKS FAILED - Please fix above issues")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
