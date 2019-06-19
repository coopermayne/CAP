require_relative 'maint.rb' #just has some code req to get database.. can be thrown away...
require_relative 'keys.rb' #hide this file from git!

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

HEADERS = {headers: { "Authorization" => "Token #{CAP_API_KEY}" }} #set in keys.rb
DB = Mongo::Client.new(['127.0.0.1:27017'], :database => 'cases') #set in keys.rb
COL = :ALL #default collection

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
  row_collection = DB[collection_name]
  count = 0

	pipeline = [
		{
			'$match': {
				'decision_date': {
					'$gte': '2003'
				}
			}
		}, {
			'$unwind': {
				'path': '$casebody.data.opinions', 
				'includeArrayIndex': 'opinionsArrayIndex', 
				'preserveNullAndEmptyArrays': false
			}
		}
	] 

  DB[:ALL].aggregate(pipeline).each do |opinion|
		kase = opinion
    op = opinion['casebody']['data']['opinions']
    op_text = op['text']

    diss_matches = op_text.to_enum(:scan, /dissent/).map { Regexp.last_match }

    next if diss_matches.empty?

    diss_matches.each do |diss_match|
      paragraph = op_text[0..diss_match.begin(0)-1].split("\n").last + diss_match[0] + op_text[diss_match.end(0)..-1].split("\n").first

      #find judge
      rgx =  /(\w*),\s([C,c]\.\s)?[J,j]\.,\sdissenting/
      js_cited = paragraph.to_enum(:scan, rgx).map { Regexp.last_match }
      judge_cited = nil
      judge_cited = js_cited.last[1].upcase unless js_cited.empty?

      start_of_paragraph = op_text[0..diss_match.begin(0)-1].split("\n").last 
      cs_cited = start_of_paragraph.to_enum(:scan, /(\d\d\d)\sU.\s?S.\s(\d*)/).map { Regexp.last_match }
      dissent_citation = nil
      dissent_citation = cs_cited.last[0].sub("U. S.", "U.S.") unless cs_cited.empty?
      author = op['author_formatted'].nil? ? nil : op['author_formatted'].upcase

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
        #add some scdb info
				more_info = DB[:scdb].find({'usCite': doc[:case_citation]}).first
        unless more_info.nil?
          doc[:case_disposition] = more_info["caseDisposition"]
          doc[:precedent_alteration] = more_info["precedentAlteration"]
          doc[:issue_area] = more_info["issueArea"]
          doc[:maj_votes] = more_info["majVotes"]
          doc[:min_votes] = more_info["minVotes"]
          row_collection.insert_one(doc)
        else
          doc[:case_disposition] = nil
          doc[:precedent_alteration] = nil
          doc[:issue_area] = nil
          doc[:maj_votes] = nil
          doc[:min_votes] = nil
        end
       
        #ap doc
        #row_collection.insert_one(doc)
      end #if
    end #each diss
  end #each kase
end #def

def explore
  col = DB[:ALL].find({
    'decision_date': {
      '$gte': '2000-01-01'
    }
  })

  binding.pry
end

def explore_matching(date='2005')
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
					'$gte': year
				}
			}
		}, {
			'$unwind': {
				'path': '$casebody.data.opinions', 
				'includeArrayIndex': 'opinionsArrayIndex', 
				'preserveNullAndEmptyArrays': false
			}
		}
	] 

  DB[:ALL].aggregate(pipeline).each do |opinion|
    op_text = opinion['casebody']['data']['opinions']['text']
    res = []
    patterns.each_with_index do |pattern, i|
      res[i] = op_text.to_enum(:scan, pattern).map { Regexp.last_match }
    end #patterns.each
  end #col.each
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
  end

  names = col.map{|kase| kase['name_abbreviation']}
  cites = col.map{|kase| kase['citations'].first['cite'] + ", #{page}"}

  ap '-'*80
  ap names
  ap cites
  ap au
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
    return 1
  end
  return_value = 2 if res.count == 1
  return_value = 3 if res.first['type']
  return_value = 4 if res.first['type'] && res.count == 1

  return res.first["_id"]
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


explore_matching
