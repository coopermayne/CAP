class Array
	def extract(&block)
		temp = self.select(&block)
		self.reject!(&block)
		temp
	end
end
