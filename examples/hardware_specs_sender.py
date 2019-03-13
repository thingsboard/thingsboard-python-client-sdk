import psutil
uploadFrequency = 10
psutil.virtual_memory()

dict(psutil.virtual_memory()._asdict())
dict(psutil.cpu_percent()._asdict())

