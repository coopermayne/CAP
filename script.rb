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
  #return an array of matching opinions
  
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

def cull_good_matches
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
    count +=1 if vol && page
    res = better_find_op(vol[0],page[0])
    next unless res
    scdb_res = DB[:scdb].find({usCite: res.first['citation']}).find
    #count +=1 if res.first && scdb_res.first
    puts count if count%10==0
  end

  ap count.to_f/100
end

def cull_ids
  col = DB[:matches] #for playing with good matches
  count = 0

  pipeline = [
    {'$match': {'category': 'good'}}, 
    {'$sample': {'size': 100}}
  ]

  col.aggregate(pipeline)
end

match_data_to_db '1900'
