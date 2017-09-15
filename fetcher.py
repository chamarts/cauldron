
#abstract base class for source fetch
class SourceFetcher:

    def fetch(self):
        pass

class DBSource(SourceFetcher):
    def __init__(selfs):
        print('Initialzed db source')