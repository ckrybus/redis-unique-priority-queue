# Redis unique priority queue

This is an example of a redis unique priority queue which preserves ordering. It's written in python 3 + a bit of lua.


## UniquePriorityFifoQueue

* preserves ordering: first in, first out (FIFO)

* doesn't allow duplicates

    When a duplicate item with a higher priority is inserted the priority will be overwritten.
    When a duplicate item with a lower priority is inserted nothing happens.

* has 10 priority classes

    Items are returned from the highest to the lowest priority. Only after all items with a high priority have been popped, items with a lower
    priority will be returned.

    The priority parameter is an integer in the range 0-9.
    0 is the highest priority, 9 ist the lowest priority.
    Default priority ist 9.


## Requirements
In order to run the code you need to install the python redis client:

```python
pip install redis
```


## Usage

```python
from redis import StrictRedis
from upf_queue import UniquePriorityFifoQueue


r = StrictRedis(host='localhost', port=6379, db=10)
q = UniquePriorityFifoQueue('queue_name', redis=r)

q.insert(['item1', 'item2', ])

len(q)  # -> 0

list(q.pop(10))  # -> ['item1', 'item2']
```

See test_upf_queue.py for more examples.


## Design

Behind the scenes each queue consist of two parts: a redis sorted set and a simple key value which is used for generating an auto increment id.

The priority is encoded in the sorted set score as the first character.
The rest is the auto increment integer.

Queue.insert() can be called with millions of items. Behind the scenes the items are split into 5k sized chunks.


## Limitations

Right now only following data types are supported: bytes, string, int or float.

Highest priority is 0, lowest 9. Probably the reverse would be better. An easy workaround is to use constants e.g. HIGH_PRIORITY = 7

The auto increment could theoretically overflow.


## Feature ideas

* bump_priority() method

* pickle/unpickle (serialization) functionality

* reset the auto increment value after the last item has been popped


## License

MIT LICENSE.
