require_relative 'keys.rb' #hide this file from git!
require_relative 'custom_methods.rb' #hide this file from git!

require 'jaro_winkler'
require 'gruff'
require 'nokogiri'
require 'csv'
require 'addressable/uri'
require 'json'
require 'jsonl'
require 'mongo'
require 'firebase'
require 'pry'
require 'httparty'
require 'awesome_print'
require 'ruby-graphviz'

Mongo::Logger.logger.level = Logger::FATAL

HEADERS = {headers: { "Authorization" => "Token #{CAP_API_KEY}" }} #set in keys.rb
DB = Mongo::Client.new(['127.0.0.1:27017'], :database => 'cases') #set in keys.rb
COL = :ALL #default collection

require_relative 'maint.rb' #just has some code req to get database.. can be thrown away...

#COUNT ALL US CITATIONS
def collect_citations
	id_count = 0
	collection = DB[COL]
	collection.find.each do |kase|
		op_text = kase['casebody']['data']
		id_matches = kase['casebody']['data'].to_enum(:scan, /Id\./).map { Regexp.last_match }
		id_count += id_matches.count
		
		id_matches.each do |m|
			puts
			rewind = m.begin(0)-200
			paragraph = op_text[rewind..m.begin(0)+20]
			puts paragraph
		end
	end
end

#BUILD MODEL SPREADSHEET
def build_rows(collection_name)
  collection = DB[:ALL]
  row_collection = DB[collection_name]
  count = 0

  collection.find.each {|kase|
    kase['casebody']['data']['opinions'].each do |op|
      op_text = op['text']

      diss_matches = op_text.to_enum(:scan, /dissent/).map { Regexp.last_match }

      unless diss_matches.empty?

        diss_matches.each do |diss_match|
          paragraph = op_text[0..diss_match.begin(0)-1].split("\n").last + diss_match[0] + op_text[diss_match.end(0)..-1].split("\n").first
          
					#find judge
          rgx =  /(\w*),\s([C,c]\.\s)?[J,j]\.,\sdissenting/
					js_cited = paragraph.to_enum(:scan, rgx).map { Regexp.last_match }
					judge_cited = nil
					judge_cited = js_cited.last[1].upcase unless js_cited.empty?

					#find citation
					
					start_of_paragraph = op_text[0..diss_match.begin(0)-1].split("\n").last 
					cs_cited = start_of_paragraph.to_enum(:scan, /(\d\d\d)\sU.\s?S.\s(\d*)/).map { Regexp.last_match }
					dissent_citation = nil
					dissent_citation = cs_cited.last[0].sub("U. S.", "U.S.") unless cs_cited.empty?
					author = op['author'].nil? ? nil : op['author'].gsub(/(chief|Cheif|justice|Justice|\,)/,"").strip.upcase

          doc = {
						kase:              kase['id'],
						diss_matches:      diss_matches.count,
						diss_match:        diss_match.begin(0),
            case_name:         kase['name_abbreviation'],
            docket_number:     kase['docket_number'],
            decision_date:     kase['decision_date'],
            case_citation:     kase['citations'].select{|ii| ii['type']=='official'}.first['cite'],
            dissent_citation:  dissent_citation,
            part_of_opinion:   op['type'],
            judge:             author,
            judge_cited:       judge_cited,
            blurb:             paragraph,
            full_opinion:      kase['frontend_url']
          }
						
					 #if judge_cited
						#puts doc[:judge_cited]; count = count+1; puts count
					#end

					 #if doc[:dissent_citation]
						#puts dissent_citation; count = count+1; puts count
					#end

					if judge_cited && dissent_citation
						#ap doc; count = count+1; puts count
      			row_collection.insert_one(doc)
					end
        end
      end
    end
	}
end

def explore
  col = DB[:ALL].find({
    'decision_date': {
      '$gte': '2000-01-01'
    }
  })

  binding.pry
end

def get_kases(date='2005', volume, page)
	#returns an array of cases that match that page number

	col = DB[COL]
	ms = col.find({
		'decision_date'=> {'$gte'=> date},
		'volume.volume_number'=> {'$eq'=> volume},
		'first_page'=> {'$lte'=> page},
		'last_page'=> {'$gte'=> page},
	})

	ms.map{|kase| kase}
end


def find_op_by_cit(vol, page)
  vol = vol.to_i
  page = page.to_i

	col = DB[COL].find({'$and'=> [
		{'volume.volume_number': { '$eq': vol } },
		{'first_page': { '$lte': page } },
		{'last_page': { '$gte': page } }
	]})

  return if col.count == 0 #no results! TODO deal with this

  op_matches = []

  type = nil

  col.each do |kase|
    opinions = kase['casebody']['data']['opinions']
    matching_ops = opinions.select do |op|
      op['first_page'] <= page && op['last_page'] >= page
    end
    op_matches.concat matching_ops
  end

  if op_matches.count<1
    au = col.first['casebody']['data']['opinions'].first['author']
  elsif op_matches.count>1
    au =  "-"*40+"MULTIPLE MATCHES" 
  else
    au = op_matches.first['author']
    type = op_matches.first['type']
  end

  names = col.map{|kase| kase['name_abbreviation']}
  cites = col.map{|kase| kase['citations'].first['cite'] + ", #{page}"}

  ap '-'*80
  ap names
  ap cites
  ap au
  ap type
  ap '-'*80
end

def better_find_op(vol, page)
  #return the opinion
  
  vol = vol.to_i
  page = page.to_i

  #TODO add conditionals to makes this all one pipeline?
  #numberOfColors: { $cond: { if: { $isArray: "$colors" }, then: { $size: "$colors" }, else: "NA"} }

	kase_pipline = [
		{
			'$match': {
				'$and': [
					{ 'volume.volume_number': vol }, 
          { 'first_page': { '$lte': page } }, 
          { 'last_page': { '$gte': page } }
				]
			}
		},
    {
      '$project': {
        'id': 1, 
        'name_abbreviation': 1, 
        'volume': 1, 
        'author': { '$arrayElemAt': ["$casebody.data.opinions.author", 0] }, 
        'type': { '$arrayElemAt': ["$casebody.data.opinions.type", 0] }, 
        "citations": 1
      },
    }
	]

  op_pipeline = [
    {
      '$match': {
        '$and': [
          {
            'volume.volume_number': vol
          }, {
            'first_page': {
              '$lte': page
            }
          }, {
            'last_page': {
              '$gte': page
            }
          }
        ]
      }
    },
    {
      '$unwind': {
        'path': '$casebody.data.opinions', 
        'includeArrayIndex': 'opIndex'
      }
    }, {
      '$match': {
        '$and': [
          {
            'casebody.data.opinions.first_page': {
              '$lte': page
            }
          }, {
            'casebody.data.opinions.last_page': {
              '$gte': page
            }
          }
        ]
      }
    }, {
      '$project': {
        'id': 1, 
        'name_abbreviation': 1, 
        'volume': '$volume.volume_number', 
        'author': '$casebody.data.opinions.author', 
        'type': '$casebody.data.opinions.type',
        "citations": 1
      }
    }
  ]

	m_kases = DB[COL].aggregate(kase_pipline)
  m_ops = DB[COL].aggregate(op_pipeline)

	if m_ops.count == 0
    res = m_kases
    kase_or_op = "kase"
  else
    res = m_ops
    kase_or_op = "op"
	end

  return_value = 0
  if res.count == 0
    return false
  end
  return_value = 2 if res.count == 1
  return_value = 3 if res.first['type']
  return_value = 4 if res.first['type'] && res.count == 1

  return res.map do |item|
    {
      "_id"=>item['_id'],
      "id"=>item['id'],
      "name_abbreviation"=>item['name_abbreviation'],
      "citation"=>item['citations'].select{|cit| cit['type']=='official'}.first['cite'],
      "author"=>item['author'],
      "type"=>item['type']
    }
  end
end

def join
	pipeline = [
		{
			'$lookup': {
				'from': 'scdb', 
				'localField': 'cite', 
				'foreignField': 'usCite', 
				'as': 'test'
			}
		}, {
			'$match': {
				'test': {
					'$ne': []
				}
			}
		}, {
			'$project': {
				'id': 1, 
				'test': 1
			}
		}
	]

	col = DB[:ALL].aggregate(pipeline)
end

def match_data_to_db(date='2005')
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
    /[pP]ost.{1,20}\(/,
    /[aA]nt[ie].{1,20}\(/,
    /F\.\s\dd.{3,25}\(/,
    /F\.\s[sS]upp\..{3,25}/,
    /[NS]..?[EW]..?\dd.{3,25}/,
  ]

  #main pattern for grabbing information
  good_patterns = [
    #good patterns
    #/\(dissenting\sopinion\)/,
    #/\([^()]{0,100}dissenting\sin\spart[^()]{0,100}\)/,
    /\([^()]{0,100}?(?<judge>[\w’']*).{1,3}[JX][\.\,][^()]{0,100}?\)/,

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
    matches = op_text.to_enum(:scan, pattern).map { |item| {
      regexp_match_text: Regexp.last_match[0],
      regexp_match_index: Regexp.last_match.begin(0),
      kase_id: kase_id,
      op_index: op_index,
      op_text: op_text
    }}

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

def find_misspelled_dissent
	#search all parens
  #use fuzzy search to look for matches on "dissenting"
  #take out any that are actually 100% matches
  #the rest should be what we are looking for...
end

col = DB[:matches] #for playing with good matches
count = 0

pipeline = [
  {'$match': {'category': 'good'}},
  {'$sample': {'size': 100}}
]
col.aggregate(pipeline).each do |match|
  txt = match['regexp_match_text']

  #24% not matching here... the rest are id matches
  #6% have more than 1 match
  matches = txt.to_enum(:scan, /U[\.\,]\s?S/).map{Regexp.last_match} 

  #(ap match['regexp_match_text']) if matches.empty? #24% here, the rest are fine (these are mostly "id" and "supra" matches... )
  next if matches.empty? 

  #cut off the bad match and the early part of string before U.S.
  cut_off_point = matches.last.begin(0)<4 ? 0 : matches.last.begin(0)-4
  txt = txt[cut_off_point..-1]
  
  #now search for the numbers
  #remove years and not numbers
  numbers = txt.gsub(/\(\d{4}\)/,'').gsub(/n\.\s\d+/, '').to_enum(:scan, /\d+/).map{Regexp.last_match}
  vol = numbers.shift
  page = numbers.pop

  next unless vol && page
  res = better_find_op(vol[0],page[0])
  next unless res.first
  scdb_res = DB[:scdb].find({usCite: res.first['citation']}).find
  count +=1 if res.first && scdb_res.first
  puts count if count%10==0
end

ap count.to_f/100
