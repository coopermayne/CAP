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

def export_arr_of_hashes_to_csv(arr, fn)
	fn = "/Users/coopermayne/Code/UCLA_Re/#{fn}.csv"

	rowid = 0
	CSV.open(fn, 'w') do |csv|
		arr.each do |hsh|
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

	fn = "/Users/coopermayne/Code/UCLA_Re/#{fn}.csv"

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
    '../UCLA/Data/scdb.wustl/SCDB_2018_02_caseCentered_Citation(1946-2018).csv',
    '../UCLA/Data/scdb.wustl/SCDB_Legacy_04_caseCentered_Citation(1791-1945).csv'
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

def match_data_to_db(date='1975')
	#NOTE: there are a couple anomalies -- xml has a few more cits than text fore some reason...

	patterns = [
    #/(?<vol>\d\d\d)\sU\.?\s?S\.\s(\d*)\,\s?(?<page>\d*)/,
    #/(?<vol>\d\d\d)\sU.{0,2}S.{0,3}at.{0,4}(?<page>\d*)/,
    #/(?<vol>\d\d\d)\sU.{0,2}S.{0,3}at\s?(?<page>\d*).{0,20}([C,c]\.\s)?[J,j]\.,\sdissenting/,
    /(?<vol>\d\d\d)\sU.{0,2}S.{0,3}at\s?(?<page>\d*).{0,20}([C,c]\.\s)?[J,j]\.,\sdissenting/,
    /([C,c].{1,3})?[J,j].{1,3}dissenting/,
		#/Id\.,\sat\s(\d*)?.{70}/,
    #/(?<vol>\d\d\d).{0,2}U.{0,2}S.{0,2}(?<first_page>\d*)(.{0,20})(\w*)?,?\s?([C,c]\.\s)?[J,j]\.,\sdissenting/,
    #/(?<vol>\d\d\d).{0,2}U.{0,2}S.{0,2}(?<first_page>\d*)(.{0,20})(\w*)?,?\s?([C,c]\.\s)?[J,j]\.,\sdissenting/,
    #/[I,i]d\.,\sat\s(\d*)?(.{0,30})(\w*)?,?\s?([C,c]\.\s)?[J,j]\.,\sdissenting/,
		#/.{190}(\w*)?,?\s?([C,c]\.\s)?[J,j]\.,\sdissenting/,
		#/.{100}dissent.{100}/
	]

  pipeline = [
    {
      '$match': {
        'decision_date': {
          '$gte': date
        }
      }
    },
    {
      '$unwind': {
        'path': '$casebody.data.opinions', 
        'includeArrayIndex': 'opIndex', 
        'preserveNullAndEmptyArrays': false
      }
    }
  ]

  #main pattern pick up all lines with dissent in parens
  pattern = /.{0,100}?\([^()]{0,100}dissent[^()]{0,100}\)/

  #filter the bad
  ignore_patterns = [
    /\(hereinafter.{0,20}dissent.*\)/,
    /F\.\s\dd.{3,25}\(/,
    /F\.\s[sS]upp\..{3,25}/,
    /[NS]..?[EW]..?\dd.{3,25}/,
  ]

  #main pattern for grabbing information
  good_patterns = [
    #good patterns
    #/\(dissenting\sopinion\)/,
    #/\([^()]{0,100}dissenting\sin\spart[^()]{0,100}\)/,
    /\([^()]{0,100}?(?<judge>[\w’']*).{1,5}[A-Z][\.\,][^()]{0,100}?\)/,

    /(?<vol>\d{1,3})\sU.{1,2}S.{1,2}(?<p>\d{1,4}).{1,2}(?<p2>\d{1,4})?-?(?<p3>\d{1,4})?.{3,25}\(/,

    /(?<vol>\d{1,3})\sU.{1,2}S.{1,3}at.{1,2}(?<p2>\d{1,4})-?(\d{1,4})?.{0,15}\(/,

    /\([^()]{0,100}?(?<vol>\d{1,3})\sU.{1,2}S.{1,2}(\d{1,4})[^()]{0,100}?\)/,

    /\([^()]{0,100}?(?<vol>\d{1,3})\sU.{1,2}S.{1,3}at.{1,2}(\d{1,4})[^()]{0,100}?\)/,
  ]

  #id patterns TODO get these linked up to case info -- maybe manually..
  id_patterns = [
    /[iI]d\..{1,2}at.{1,2}\d{1,4}.{0,20}\(/,
    /[iI]d\..{1,2}.{1,2}\d{1,4}.{0,5}\(/,
    /[sS]upra.{1,2}at.{1,2}\d{1,4}.{0,20}\(/,
    /[sS]upra.{1,2}\(/,
    /[pP]ost.{1,20}\(/,
    /[aA]nt[ie].{1,20}\(/,
  ]

  leftover_matches = []
  rejected_matches = []
  good_matches = []
  id_matches = []

  DB[:ALL].aggregate(pipeline).each do |opinion|

    kase_id = opinion['_id']
    op_index = opinion['opIndex']
    op_text = opinion['casebody']['data']['opinions']['text']

    #matches = op_text.to_enum(:scan, pattern).map { Regexp.last_match }
    matches = op_text.to_enum(:scan, pattern).map do |item| 

      regexp_match_text = Regexp.last_match[0]
      regexp_match_index =  Regexp.last_match.begin(0)

      judges = item.to_enum(:scan, /\([^()]{0,100}?(?<judge>[\w’']*).{1,5}[A-Z][\.\,][^()]{0,100}?\)/).map {Regexp.last_match}
      judge = judges.empty? ? nil : judges.last['judge'].downcase
      {
        date: opinion['decision_date'],
        regexp_match_text: regexp_match_text,
        regexp_match_index: regexp_match_index,
        kase_id: kase_id,
        op_index: op_index,
        category: nil,
        judge: judge,
        op_text: op_text,
      }
    end

    next if matches.empty?

    #ignore patterns
    rejected_matches.concat matches.extract{ |match_data|
      r = false
      ignore_patterns.each{|pattern| r = true if match_data[:regexp_match_text].match pattern}
      r
    }

    id_matches.concat matches.extract { |match_data|
      r = false
      id_patterns.each{|pattern| r = true if match_data[:regexp_match_text].match pattern }
      r
    }

    good_matches.concat matches.extract{ |match_data|
      r = false
      good_patterns.each{|pattern| r = true if match_data[:regexp_match_text].match pattern }
      r
    }

    leftover_matches.concat matches
    ap matches.map{|item|item[:regexp_match_text]} unless matches.empty?

  end
	
  ap "REJECT MATCHES: #{rejected_matches.count}"
  ap "ID MATCHES: #{id_matches.count}" 
  ap "GOOD MATCHES: #{good_matches.count}"
  ap "THE REST: #{leftover_matches.count}"

  rejected_matches.map{|item| item[:category]='rejected'; item}
  id_matches.map{|item| item[:category]='id'; item}
  good_matches.map{|item| item[:category]='good'; item}
  leftover_matches.map{|item| item[:category]='leftover'; item}

  DB[:matches].delete_many {}
  DB[:matches].insert_many rejected_matches + id_matches + good_matches + leftover_matches
end

def export_matches_to_csv
  rows = DB[:matches].find.map do |match|
    #case citing
    kase = DB[:ALL].find({'_id' => match['kase_id']}).first
    op = kase['casebody']['data']['opinions'][match['op_index']]
   
    #case cited to
    cit_kase = DB[:ALL].find({'_id' => match['cit_kase_id']}).first
    cit_op = cit_kase['casebody']['data']['opinions'][match['cit_op_index']]

    {
      :decision_date => kase['decision_date'],
      :case_name => kase['name_abbreviation'],
      :docket_number => kase['docket_number'],
      :case_citation => kase['citations'].first['cite'],
      :judge => op['author_formatted'],
      :part_of_opinion => op['type'],
      :dissent_citation => 'xxx',
      :judge_cited => match['xxx'],
      :blurb => match['blurb'],
      :full_opinion => kase['full_opinion'],
      :dissent_citation_raw => match['dissent_citation_raw'],
      :judge_cited_raw => match['judge_cited_raw'],
    }
  end

  fn = "/Users/coopermayne/Code/UCLA_Re/#{Time.now.to_i.to_s}_matches.csv"

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

def fix_judge_guess
  c = 0
  DB[:matches].aggregate([]).each do |match|

    full_text = match['op_text']
    txt = match['regexp_match_text']
    txt_i = match['regexp_match_index']

    paren_index = txt_i + txt.last_match(/\(/).begin(0)
    n = paren_index > 55 ? 55 : paren_index
    before_paren = full_text[paren_index-n, n]
    full_text_before_paren = full_text[0, paren_index]
    inside_paren = txt[txt.last_match(/\(/).begin(0), txt.length - txt.last_match(/\(/).begin(0)]

    ii = 1000
    first_match = nil

    JUDGES.map{|ji| ji[:last_name]}.each do |ln| 
      ip_clean = I18n.transliterate(inside_paren.downcase).gsub(/[^a-z\s]/,'')
      idx = ip_clean.index ln.downcase.gsub(/[^a-z\s]/,'')

      next if idx.nil?
      if idx < ii
        first_match = ln
        ii = idx
      end
    end

    judge = first_match

    ap DB[:matches].update_one({_id: match['_id']}, {'$set' => {judge: judge}})
  end
end

def find_misspelled_dissent
  #NOTE this didn't pick up many misspellings... 
  
  #Djissent, .86
  #Dissenting, .81 (NOTE fix all regex to ignore caps!)
  
  pipeline = [
    {
      '$unwind': {
        'path': '$casebody.data.opinions', 
        'includeArrayIndex': 'opIndex', 
        'preserveNullAndEmptyArrays': false
      }
    }
  ]

	#search all parens
  pattern = /\((?<text>.*?)\)/
  matches = []
  DB[:ALL].aggregate(pipeline).each do |opinion|
    op_text = opinion['casebody']['data']['opinions']['text']
    all_paren_matches = op_text.to_enum(:scan, pattern).map{Regexp.last_match}
    all_paren_matches.each do |paren_match|
      j = paren_match['text'].gsub(/\W/, ' ').split(' ').map{|ii| [ii, JaroWinkler.jaro_distance("dissent", ii) ]}
      r =  j.select{|i| !i[0].match(/dissent/) && i[1] > 0.80}
      ap r unless r.empty?
    end
  end
  #use fuzzy search to look for matches on "dissenting"
  #take out any that are actually 100% matches
  #the rest should be what we are looking for...
end

