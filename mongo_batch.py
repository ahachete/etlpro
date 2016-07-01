class MongoBatch:
	def __init__(self, collection, size=1):
		self.size = size
		self.collection = collection
		self.__init_batch()

	def __init_batch(self):
		self.count = 0
		self.batch = self.collection.initialize_ordered_bulk_op()
		
	def __batch_op(self):
		self.count += 1
		if self.count == self.size:
			self.batch.execute()
			self.__init_batch()

	def add(self, doc):
		self.batch.insert(doc)
		self.__batch_op()

	def finalize(self):
		if self.count > 0:
			self.batch.execute()
		
