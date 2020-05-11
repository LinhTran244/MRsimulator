

def timeit(method):
    import time
    def timed(*args, **kw):
        effective_kw = kw.copy()
        effective_kw.pop("log_time",None)
        effective_kw.pop("log_name",None)
        ts = time.time()
        result = method(*args, **effective_kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts)*1000)
        return result
    return timed