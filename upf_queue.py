import itertools

lua_insert_script = """
    local key = KEYS[1]

    for i=1, #ARGV, 2 do
        local priority = ARGV[i]
        local member = ARGV[i + 1]
        local score = redis.call('zscore', key, member)
        if not score then
            -- add, because the element is not yet in the queue
            local auto_increment = redis.call('incr', '_'..key..'_seq')
            score = priority..string.format('%015d', auto_increment)
            redis.call('ZADD', key, score, member)
        else
            -- update only if the priority is higher
            local current_priority = string.sub(score, 1, 1)
            if tonumber(priority) < tonumber(current_priority) then
                score = priority..string.sub(score, 2)
                redis.call('ZADD', key, score, member)
            end
        end
    end

"""

HIGHEST_PRIORITY = 0
LOWEST_PRIORITY = 9


class UniquePriorityFifoQueue:
    """
    A redis queue which:

    * preserves ordering: first in, first out (FIFO)

    * doesn't allow duplicates

        When a duplicate item with a higher priority is inserted the priority
        will be overwritten.
        When a duplicate item with a lower priority is inserted nothing happens.

    * has 10 priority classes

        Items are returned from the highest to the lowest priority. Only after
        all items with a high priority have been popped, items with a lower
        priority will be returned.

        The priority parameter is an integer in the range 0-9.
        0 is the highest priority, 9 ist the lowest priority.
        Default priority ist 9.

    """

    def __init__(self, name, redis):
        self.name = name
        self._redis = redis
        self.lua_special_zadd = self._redis.register_script(lua_insert_script)

    def __len__(self):
        return self._redis.zcard(self.name)

    def count(self, priority=None):
        if priority is None:
            return len(self)

        idx = '{}000000000000000'
        if priority == 0:
            score_range = ('-inf', idx.format('(1'))
        elif priority < 9:
            score_range = (idx.format(priority),
                           idx.format(f'({priority + 1}'))
        elif priority == 9:
            score_range = (idx.format('9'), '+inf')
        else:
            raise ValueError('TODO')
        return self._redis.zcount(self.name, *score_range)

    def insert(self, keys, priority=LOWEST_PRIORITY, chunk_size=5000):
        for keys_chunk in chunks(keys, chunk_size):
            # redis expects: priority1, member1, priority2, member2, ...
            args = itertools.chain(*((priority, k) for k in keys_chunk))
            self.lua_special_zadd(keys=[self.name], args=args)

    def delete(self, keys):
        if keys:
            return self._redis.zrem(self.name, *keys)

    def pop(self, count):
        assert 0 < count <= 25000

        with self._redis.pipeline() as pipe:
            pipe.multi()
            pipe.zrange(self.name, 0, count - 1)
            pipe.zremrangebyrank(self.name, 0, count - 1)
            items, _ = pipe.execute()

        for value in items:
            yield value.decode('utf-8')


def chunks(iterable, size):
    """
    A generator which returns chunks of `size` elements until there are no
    more items left in the iterable.

    """
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))
