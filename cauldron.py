from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import datetime

class Kettle(Thread):

    def __init__(self,name):
        super().__init__()
        self.listeners = []
        self.transformations = []
        self.futures = []
        self.name = name
        self.executor = ThreadPoolExecutor(max_workers=10)
        #self.executor = ProcessPoolExecutor(max_workers=10)
        #ThreadPoolExecutor(max_workers=10)
        print('Initializing Kettle ' + name)

    def __del__(self):
        self.executor.shutdown(wait=False)

    def add(self,transformation):
        self.transformations.append(transformation)

    def run(self):
        print('Executing Kettle ' + self.name)
        self.start = datetime.datetime.now()
        self.done = 0
        for transformation in self.transformations:
            #transformation.execute()
            future = self.executor.submit(transformation.execute)
            #pass transformation arguments
            self.futures.append(future)
            ##future.add_done_callback(self.__reconcile)

    def __reconcile(self):
        print('Finished exeucting transformation')
        self.done += 1
        if self.done == len(self.transformations):
            print('Kettle {0} took {1} to finish.'.format(self.name,(datetime.datetime.now() - self.start)))
            self.__notify()

    def register(self,listener):
        self.listeners.append(listener)

    def __notify(self):
        for listener in self.listeners:
            listener()



def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate

@static_vars(counter=0)
def reconcile():
    reconcile.counter += 1
    print("Counter is %d" % reconcile.counter)