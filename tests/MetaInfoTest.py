import unittest
import dmp.assets
from dmp.assets import AssetsProcessor
from dmp.assets.metainfo import MetaInfo

class MetaInfoTest(unittest.TestCase):

    def test_meta_data(self):
        AssetsProcessor()
        self.assertEquals(len(MetaInfo.getMetaInfo('s_asset_xa')),35)
        self.assertEquals(len(MetaInfo.getMetaInfo('s_asset')),421)
