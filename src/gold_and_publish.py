import inspect
import os
import sys

_this_file = os.path.abspath(inspect.currentframe().f_code.co_filename)
sys.path.insert(0, os.path.dirname(os.path.dirname(_this_file)))

import src.gold_streaming as gold_streaming
import src.gold_batch as gold_batch
import src.publish_snapshot as publish_snapshot

gold_streaming.run()
gold_batch.run()
publish_snapshot.run()
