import src.gold_streaming as gold_streaming
import src.gold_batch as gold_batch
import src.publish_snapshot as publish_snapshot

gold_streaming.run()
gold_batch.run()
publish_snapshot.run()
