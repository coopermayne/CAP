def create_backup_collection(col_name)
	backup_name = (col_name.to_s + "_backup_" + Time.now.to_i.to_s).to_sym
	DB[backup_name].insert_many(DB[col_name].find)
end

def create_play_copy(play_copy_name='playtime', date="2011")
	DB[play_copy_name].insert_many(DB[:ALL].find({"decision_date"=>{"$gte"=> date}}))
end

def restore_from_backup(col_name)
	#delete current col - get most recent back up and copy it over to cur col name
	#backup_name = (col_name.to_s + "_backup_" + Time.now.to_i.to_s).to_sym
	#DB[backup_name].insert_many(DB[col_name].find)
end

def find_db_errors_in_opinion(col_name = :ALL_SCOTUS)
	col = DB[col_name]

	kases = []
	op_with_errors = []

	col.find.each do |kase|
		opinions = kase['casebody']['data']['opinions']
		xml = kase['casebody_xml']['data']
		p_xml = Nokogiri::XML(xml)

		#next if p_xml.css('opinion').count 
		p_xml.css('opinion').each_with_index do |op, i|
			type = op.attr('type')
			authors = op.css('author')
			
			if authors.select{|au| au.text.length>5}.length > 1
				kases << kase
				op_with_errors << [op, kase['frontend_url']]
			end
		end
	end

	ap op_with_errors.map{|op| op[0].css('author').map{|au| au.text} << op[1] }
	ap op_with_errors.count
	str = {"errors" => kases.map {|kase| kase['id']}}
	fJson = File.open("errors.json","w")
	fJson.write(str.to_json)
	fJson.close
end

def build_sample_db(sample_coll_name, start_year)
	big_coll = DB[:ALL_SCOTUS]
	sample_coll = DB[sample_coll_name]

	limted_set = big_coll.find({'decision_date'=> {'$gt'=> "#{start_year.to_s}-01-01"}})
	sample_coll.insert_many limted_set
end

def add_frontendurl(coll)
	coll = DB[coll]
	coll.find.each do |kase|
		coll.update_one({"id"=> kase['id']}, {"$set" => {frontend_url: "https://api.case.law/v1/cases/#{kase['id']}/?full_case=true&format=html"}})
	end
end

def group_by_author
	col = DB[:ALL]
	opinions = col.find.map{|kase| kase['casebody']['data']['opinions']}
	gr = opinions.flatten.group_by{|op| op["author"]}
	binding.pry
end

# METHOD LIST: build_rows(collection_name), build_case_database
# TOOLS LIST: export_to_csv, export_hash_to_csv, pp_xml

def convert_strings_to_int(coll_name)
	coll = DB[coll_name]
	coll.find.each do |kase|
		coll.update_one({_id: kase['_id']},{"$set" => {
			"first_page" => kase['first_page'].to_i,
			"last_page" => kase['last_page'].to_i,
			"volume.volume_number" => kase['volume']['volume_number'].to_i
		}})
	end
end

def build_case_database(query_hash = {
	full_case: "true",
	body_format: "xml",
	reporter: "983", 
	decision_date_min: "2000-01-01",
	decision_date_max: "2000-01-15"
})

  results = []

	uri = Addressable::URI.new
	uri.query_values = query_hash

	url = "https://api.case.law/v1/cases?" + uri.query
	collection_name= "#{Time.now.to_i}"
  collection = DB[collection_name]

  response = HTTParty.get(
    url,
    HEADERS
  )

  results.concat response['results'] 
  next_page = response['next']

  page_counter = 0
  until next_page.nil?
    page_counter += 1

    puts "(#{page_counter}) searching next page: #{next_page}"

    response = HTTParty.get(
      next_page,
      HEADERS
    )
    next_page = response['next']
    results.concat response['results'] 
  end

  results.each do |result|
    unless collection.find({id: result["id"]}).count > 0 #check to see if already saved...
			collection.insert_one(result)
      puts "\nsaved: #{result['id']}\n"
    else
      puts "\nalready saved: #{result['id']}\n"
    end
  end
end

def export_to_csv(collection_name)
	rows = DB[collection_name].find

	fn = "/Users/coopermayne/Code/UCLA_Re/#{collection_name}.csv"

	rowid = 0
	CSV.open(fn, 'w') do |csv|
		rows.each do |hsh|
			rowid += 1
			if rowid == 1
				csv << hsh.keys
			else
				puts hsh.values
				csv << hsh.values
			end
		end
	end
end

def export_hash_to_csv(hash, fn)

	fn = "/Users/coopermayne/Code/UCLA/#{fn}.csv"

	CSV.open(fn, 'w') do |csv|
		hash.each_pair do |k,v|
			puts [k,v]
			csv << [k,v]
		end
	end
end

#TOOLS
def pp_xml(xml='')
  doc = Nokogiri.XML(xml) do |config|
    config.default_xml.noblanks
  end
  puts doc.to_xml(:indent => 2)
  xml
end

def save_to_file(fn=Time.now.to_i.to_s, string)
  File.open(fn, 'w'){|file| file.write(string)}
end

def link(kase)
  puts kase['frontend_url']
end

def link_xml(kase)
	"https://api.case.law/v1/cases/#{kase['id']}/?full_case=true&format=xml"
end

def add_pagination_to_opinions
	# note: there are only 51 cases failing the error test in ALL_SCOTUS
	# fix by looking at full xml from API -- look at <structMap TYPE="physical"> to get perfect translation to physical page numbers
	col    = DB[COL]

  res_time = 0
	kases  = []
	errors = []

	col.find({'updated' => {'$exists' => false}}).each do |kase|

		next if kase['updated']==true

	  for_update = {'updated' => true}

		opinions = kase['casebody']['data']['opinions']
		xml = kase['casebody_xml']['data']
		p_xml = Nokogiri::XML(xml)
    ops = p_xml.css('opinion')

    if opinions.empty?
      res_time = "xxxxxxxxxxxxxxxxxxxx"
      for_update['updated'] = false
    elsif opinions.count == 1
      res_time = "xxxxxxxxxxxxxxxxxxxx"
      for_update["casebody.data.opinions.0.first_page"] = kase['first_page']
      for_update["casebody.data.opinions.0.last_page"] = kase['last_page']
    else
      st = Time.now
      res = HTTParty.get(link_xml(kase), HEADERS)
      res_time = Time.now - st

      full_xml = Nokogiri::XML(res.to_s)
      struct_map = {}
      full_xml.css('structMap[TYPE=physical] div[TYPE=page]').each{|div|  struct_map[ div.attr('ORDER').to_i ] = div.attr('ORDERLABEL').to_i  }

      ops.each_with_index do |op, i|
        pages = op.css('p').map{|p| p.attr('pgmap').to_i}.sort

        first_page_op = struct_map[pages.first]
        last_page_op = struct_map[pages.last]

        for_update["casebody.data.opinions.#{i}.first_page"] = first_page_op
        for_update["casebody.data.opinions.#{i}.last_page"] = last_page_op

      end #ops.each
    end #if options.count...

		col.update_one({'id'=> kase['id']},{'$set' => for_update })

    ap "-"*80
    ap "res time: #{res_time}"
    ap "opinions.count: #{opinions.count.to_s}"
    ap "Request: #{kase['id']}: #{link_xml	kase}"
    ap for_update
    ap "-"*80


  end #col.find.each
end

def import_scdb_data
  files = [
  'Data/scdb.wustl/SCDB_2018_02_caseCentered_Citation(1946-2018).csv',
  'Data/scdb.wustl/SCDB_Legacy_04_caseCentered_Citation(1791-1945).csv'
  ]

	keys = {
		"decisionType" => {
			'1' => 	'opinion of the court (orally argued)',
			'2' => 	'per curiam (no oral argument)',
			'4' => 	'decrees',
			'5' => 	'equally divided vote',
			'6' => 	'per curiam (orally argued)',
			'7' => 	'judgment of the Court (orally argued)',
			'8' => 	'seriatim '
		},
		"caseDisposition" => {
			'1' => 	"stay, petition, or motion granted",
			'2' => 	"affirmed (includes modified)",
			'3' => 	"reversed",
			'4' => 	"reversed and remanded",
			'5' => 	"vacated and remanded",
			'6' => 	"affirmed and reversed (or vacated) in part",
			'7' => 	"affirmed and reversed (or vacated) in part and remanded",
			'8' => 	"vacated",
			'9' => 	"petition denied or appeal dismissed",
			'10' => 	"certification to or from a lower court",
			'11' => 	"no disposition "
		},
		"precedentAlteration" => {
			'0' => 	"no determinable alteration of precedent",
			'1' => 	"precedent altered ",
		},
		"issueArea" => {
			'1' => 	"Criminal Procedure",
			'2' => 	"Civil Rights",
			'3' => 	"First Amendment",
			'4' => 	"Due Process",
			'5' => 	"Privacy",
			'6' => 	"Attorneys",
			'7' => 	"Unions",
			'8' => 	"Economic Activity",
			'9' => 	"Judicial Power",
			'10' => 	"Federalism",
			'11' => 	"Interstate Relations",
			'12' => 	"Federal Taxation",
			'13' => 	"Miscellaneous",
			'14' => 	"Private Action ",
		},
		"authorityDecision1" => {
			'1' => 	"judicial review (national level)",
			'2' => 	"judicial review (state level)",
			'3' => 	"Supreme Court supervision of lower federal or state courts or original jurisdiction",
			'4' => 	"statutory construction",
			'5' => 	"interpretation of administrative regulation or rule, or executive order",
			'6' => 	"diversity jurisdiction",
			'7' => 	"federal common law",
		}
	}

  col = DB[:scdb]

  files.each do |fn|
    CSV.parse(File.read(fn).scrub,encoding: 'ISO-8859-1', headers: :first_row, quote_char: '"').map do |line|
      h_new = {}
      h = line.to_hash
      h.each_pair do |k,v|
        if v == v.to_i.to_s && keys.keys.include?(k)
					puts k
					h_new[k] = keys[k][v]
        else
          h_new[k] = v
        end
      end
			col.insert_one h_new
    end
  end
end

def give_ops_ids
  col = DB[COL]

  col.find.each do |kase|
    for_update = {}
    ops = kase['casebody']['data']['opinions']
    ops.each_with_index do |op, i|
      for_update["casebody.data.opinions.#{i}._id"] = BSON::ObjectId.new
    end

    ap for_update

    unless ops.empty?
      if ops.first['_id'].nil?
        col.update_one({'id'=> kase['id']},{'$set' => for_update })
      end
    end
    
  end
end

def fix_null_or_empty_author_fields
  pipeline = 
    [
      {
        '$unwind': {
          'path': '$casebody.data.opinions', 
          'includeArrayIndex': 'opIndex', 
          'preserveNullAndEmptyArrays': false
        }
      }, {
        '$match': {
          '$and': [
            {
              'casebody.data.opinions.author': {
                '$eq': nil
              }
            }
          ]
        }
      }
  ]
  DB[:ALL].aggregate(pipeline).each do |doc|
    DB[:ALL].update_one( {'_id': doc['_id']}, {'$set': {
      "casebody.data.opinions.#{doc['opIndex']}.author_formatted" => nil
    }})
  end
end

def replace_null_with_error_where_necessary
	pipeline = [
		{
			'$unwind': {
				'path': '$casebody.data.opinions', 
				'includeArrayIndex': 'arrIndex', 
				'preserveNullAndEmptyArrays': false
			}
		}, {
			'$match': {
				'$and': [
					{
						'casebody.data.opinions.author': {
							'$ne': nil
						}
					}, {
						'casebody.data.opinions.author_formatted': {
							'$eq': nil
						}
					}
				]
			}
		} 
	]

  DB[:ALL].aggregate(pipeline).each do |doc|
    DB[:ALL].update_one( {'_id': doc['_id']}, {'$set': {
      "casebody.data.opinions.#{doc['arrIndex']}.author_formatted" => "ERROR"
    }})
  end
end


def fix_authors
	pipeline = [
		{
			'$unwind': {
				'path': '$casebody.data.opinions',
        'includeArrayIndex': 'opIndex'
			}
		}, {
			'$group': {
				'_id': '$casebody.data.opinions.author', 
				'ops': {
					'$push': {'_id': '$casebody.data.opinions._id', 'kase_id': '$id', 'opIndex': '$opIndex'}
				}
			}
    }, {
      '$project': {
        '_id': {'$ifNull': ['$_id', "xxx"]},
        'ops': 1,
        'count': {
          '$size': '$ops'
        }
      }
    }, {
      '$sort': {
        'count': -1
      }
    },{
      '$match': {
        '_id': { 
          '$ne': 'xxx'
        }
      }
    }
  ]

	col = DB[:ALL].aggregate(pipeline)

  #notes: van ==> van Devanter
  
  op_grouped_by_judge = []
  col.each do |group|
    op_grouped_by_judge << [get_justice_name(group), group['ops']]
    #judges << get_justice_name(group)
  end

  op_grouped_by_judge.each do |group|
    group[1].each do |op|
      puts op['kase_id']
      puts op['_id']
      puts "casebody.data.opinions.#{op['opIndex']}.author_formatted": group[0]
      #DB[:ALL].update_one( {'id': op['kase_id']}, {'$set': {
        #"casebody.data.opinions.#{op['opIndex']}.author_formatted" => group[0]
      #}})
    end
  end
end

def get_justice_name(group)
  return nil if group['_id'].nil?

  txt = group['_id'].gsub(/[^a-zA-Z\s]/,'').strip.downcase

  match = txt.match /(justice|jjtstice)\s?(?<judge>\w*)/

  if match.nil? 
    match = txt.match /per\s\w*/
    judge = "per curium" if match
  elsif match['judge']==""
    judge = "chief justice" if txt.match /chief\sjustice$/
  else
    judge = match['judge']
  end

  return judge
end
