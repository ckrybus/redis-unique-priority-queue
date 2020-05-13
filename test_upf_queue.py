import pytest
from redis import StrictRedis

from upf_queue import UniquePriorityFifoQueue


@pytest.fixture
def redis_client():
    redis_client = StrictRedis(host='localhost', port=6379, db=10)
    redis_client.flushdb()
    redis_client.script_flush()
    return redis_client


@pytest.fixture
def queue(redis_client):
    return UniquePriorityFifoQueue('queue_name', redis=redis_client)


def test_init_queue(queue):
    assert queue.name == 'queue_name'


@pytest.mark.parametrize('data', [
    ['A', 'B', 'C'],
    (x for x in ['A', 'B', 'C']),
])
def test_insert(queue, data):
    for item in data:
        queue.insert([item])
    assert list(queue.pop(10)) == ['A', 'B', 'C']


def test_insert_chunk_size(queue):
    queue.insert(['A', 'B', 'C', 'D', 'E', 'F'], chunk_size=2)
    assert list(queue.pop(10)) == ['A', 'B', 'C', 'D', 'E', 'F']


def test_insert_duplicates_remain_in_place(queue):
    queue.insert(['A', 'B', 'C', 'D'])
    queue.insert(['B', 'C'])
    assert list(queue.pop(4)) == ['A', 'B', 'C', 'D']


@pytest.mark.xfail
def test_autoincrement_overflow(queue):
    """
    The auto increment needs to be less than 10**15 but the current
    implementation doesn't enforce it.

    """
    key = f'_{queue.name}_seq'
    queue._redis.set(key, '999999999999999')
    queue.insert(['A'])
    auto_increment = queue._redis.get(key).decode('utf-8')
    assert int(auto_increment) < 10**15


def test_queue_length(queue):
    assert len(queue) == 0
    queue.insert(['A'])
    assert len(queue) == 1
    queue.insert(['B', 'C'])
    assert len(queue) == 3


@pytest.mark.parametrize('data', [
    ['K', 'D', 'Y'],
    (x for x in ['K', 'D', 'Y']),
])
def test_delete(queue, data):
    queue.insert(data)
    queue.delete(['K'])
    assert list(queue.pop(10)) == ['D', 'Y']


@pytest.mark.parametrize('data', [
    ['R', 'P', 'N'],
    (x for x in ['R', 'P', 'N']),
])
def test_empty_delete(queue, data):
    queue.insert(data)
    queue.delete([])
    assert list(queue.pop(10)) == ['R', 'P', 'N']


def test_pop_empty_queue(queue):
    assert list(queue.pop(10)) == []


@pytest.mark.parametrize('data', [
    ['A', 'B', 'C', 'D', 'E'],
    (x for x in ['A', 'B', 'C', 'D', 'E']),
])
def test_pop_limit(queue, data):
    queue.insert(data)
    assert list(queue.pop(3)) == ['A', 'B', 'C']
    assert list(queue.pop(10)) == ['D', 'E']


HIGH = 2
LOW = 6


def test_priority(queue):
    queue.insert(['C'], priority=LOW)
    queue.insert(['B'], priority=HIGH)
    queue.insert(['D', 'A'], priority=LOW)
    queue.insert(['E'], priority=HIGH)
    assert list(queue.pop(10)) == ['B', 'E', 'C', 'D', 'A']


def test_count(queue):
    queue.insert(['A', 'B', 'C'], priority=HIGH)
    queue.insert(['D', 'E'], priority=LOW)

    assert queue.count() == len(queue)
    assert queue.count(priority=HIGH) == 3
    assert queue.count(priority=LOW) == 2


def test_insert_duplicate_with_higher_priority(queue):
    queue.insert(['A'], priority=LOW)
    assert queue.count(priority=HIGH) == 0
    assert queue.count(priority=LOW) == 1

    queue.insert(['A'], priority=HIGH)
    assert queue.count(priority=HIGH) == 1
    assert queue.count(priority=LOW) == 0


def test_insert_duplicate_with_lower_priority(queue):
    queue.insert(['A'], priority=HIGH)
    assert queue.count(priority=HIGH) == 1
    assert queue.count(priority=LOW) == 0

    queue.insert(['A'], priority=LOW)
    assert queue.count(priority=HIGH) == 1
    assert queue.count(priority=LOW) == 0
