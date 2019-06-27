class Array
	def extract(&block)
		temp = self.select(&block)
		self.reject!(&block)
		temp
	end
end

class String
  def last_match(rgx)
    self.to_enum(:scan, rgx).map{Regexp.last_match}.last
  end
end

def continue
  print "Press any key to continue\r"
  gets
end
