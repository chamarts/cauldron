import datetime


#class responsible for creating a transformation and adding steps of execution to transformation
class Transformation:

    def __init__(self,name):
        self.steps = []
        self.name = name

    def addStep(self,step):
        self.steps.append(step)
        return self

    def execute(self):
        print('Executing transformation::' + self.name)
        self.start = datetime.datetime.now()
        result = None
        for step in self.steps:
            step_start_time = datetime.datetime.now()
            step.setData(result)
            result = step.perform()
            print('step {0} took {1} to finish.'.format(step.name, (datetime.datetime.now() - step_start_time)))
        print('Finished Executing tranformation::' + self.name)
        print('Transformtion {0} took {1} to finish.'.format(self.name, (datetime.datetime.now() - self.start)))