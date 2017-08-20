def foo(bar, baz, i):
    y = i
    while y < i+5:
        print '\n hello {0}'.format(bar), y
        y += 1
    return 'foo' + baz + bar

from multiprocessing.pool import ThreadPool
pool = ThreadPool(processes=1)
pool2 = ThreadPool(processes=2)

async_result = pool.apply_async(foo, ('world', 'foo', 0)) # tuple of args for foo
async_result2 = pool2.apply_async(foo, ('russia', 'foo', 6)) # tuple of args for foo

# do some other stuff in the main process

return_val = async_result.get()  # get the return value from your function.
return_val = async_result2.get()


print return_val
print return_val