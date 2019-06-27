require_relative 'global_variables.rb'
require_relative 'keys.rb' #hide this file from git!
require_relative 'custom_methods.rb' #hide this file from git!

require 'i18n'
I18n.available_locales = [:en]

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

def better_find_op(vol, page, judge)
  #return an array of matching opinions
  
  vol = vol.to_i
  page = page.to_i

  op_pipeline = [
    {
      '$match': {
        'volume.volume_number': vol
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
    },
  ]

  results = DB[COL].aggregate(op_pipeline)

  if results.count == 0
    return nil
  elsif results.count == 1
    #TODO mark ones that seem wrong (like wrong judge etc)
    return results.first
  elsif results.count > 1
    ap '-'*80
    ap "JUDGE: #{judge}"
    ap results.map{|r| [r['casebody']['data']['opinions']['author_formatted'], r['casebody']['data']['opinions']['type']]}

    m_auth = results.select{|r|  !r['casebody']['data']['opinions']['author_formatted'].nil? && !judge.nil? && r['casebody']['data']['opinions']['author_formatted'] == judge.downcase.gsub(/[^a-z]/,'') }
    m_diss = results.select{|r|  !r['casebody']['data']['opinions']['type'].nil? && r['casebody']['data']['opinions']['type'].match(/dissent/) }

    if m_auth.count==1
      return m_auth.first
    end

    if judge.nil? && m_diss.count==1
      return m_diss.first
    end
    
    return results
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
  c = 0

  pipeline = [
    #{'$match': {'category': 'good'}}, 
    {'$sample': {'size': 100}}
  ]
  col.aggregate(pipeline).each do |match|
    txt = match['regexp_match_text']
    judge_guess = match['judge']

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

    if res.count == 0
      next
    elsif res.count>1
      #handle multiple matches by filtering for type dissent
      res_dissents = res.select{|item| item['casebody']['data']['opinions']['type'].match /dissent/}

      #handle multiple matches by filtering for judge name
      res_match_justice_name = res.select do |item|
        o = item['casebody']['data']['opinions']
        unless o['author_formatted'].nil? || judge_guess.nil?
          judge_guess.gsub(/[^a-z]/, '') == o['author_formatted'].gsub(/[^a-z]/, '')
        else
          false
        end
      end

      if res_match_justice_name.count == 1
        res = res_match_justice_name.first
      elsif res_dissents.count == 1
        res = res_dissents.first
      else
        #if we can't narrow it using name or category
        next
      end
    elsif res.count==1
      res = res.first
    end
    c+=1 if res && res['casebody']['data']['opinions']['author_formatted']==match['judge']
    ap c if c%10==0
  end
  ap c
end

def cull_id_matches
  #500 id
  #250 supra
  #250 post
  #150 ante

  c = 0
  pipeline = [
    {
      '$match': {
        'category': 'id',
        'done': {'$ne': 1}
      },
    }, 
    { 
      '$sample': {'size': 100}
    }
  ]

  DB[:matches].aggregate(pipeline).each do |match|
    txt = match['regexp_match_text']
    txt_i = match['regexp_match_index']
    next unless txt.match /[iI]d\./

    n = 200
    longer_match = match['op_text'][txt_i-n, txt.length+n]

    matches = longer_match.to_enum(:scan, /U[\.\,]\s?S/).map{Regexp.last_match} 

    next if matches.empty? 

    #cut off the bad match and the early part of string before U.S.
    cut_off_point = matches.last.begin(0)<4 ? 0 : matches.last.begin(0)-4
    longer_match = longer_match[cut_off_point..-1]

    #now search for the numbers
    #remove years and not numbers
    numbers = longer_match.gsub(/\(\d{4}\)/,'').gsub(/n\.\s\d+/, '').to_enum(:scan, /\d+/).map{Regexp.last_match}
    vol = numbers.shift
    page = numbers.pop
    next unless vol&&page
    res = better_find_op(vol[0],page[0])
    c+=1 if res.count>1
  end
  ap c
end

def new_culling_method
  c = 0
  c2 = 0

  pipeline = [
    {
      '$match': {
        'category': {'$in': ['good', 'id', 'leftover']},
        'judge': {'$ne': nil}
      }
    },
    {
      '$sample': {
        size: 1000
      }
    }
  ]
  
  DB[:matches].aggregate(pipeline).each do |match|
    txt = match['regexp_match_text']
    txt_i = match['regexp_match_index']

    n = 200

    start_i = txt_i-n < 0 ? 0 : txt_i-n

    longer_match = match['op_text'][start_i, txt.length+n]

    matches = longer_match.to_enum(:scan, /U[\.\,]\s?S/).map{Regexp.last_match} 

    next if matches.empty?

    #cut off the bad match and the early part of string before U.S.
    cut_off_point = matches.last.begin(0)<4 ? 0 : matches.last.begin(0)-4
    longer_match = longer_match[cut_off_point..-1]

    #now search for the numbers
    #remove years and not numbers
    numbers = longer_match.gsub(/\(\d{4}\)/,'').gsub(/n\.\s\d+/, '').to_enum(:scan, /\d+/).map{Regexp.last_match}
    vol = numbers.shift
    page = numbers.pop
    (c2+=1; next) unless vol&&page
    res = better_find_op(vol[0],page[0])


  end

  ap c
  ap c2
end

def new_new
  c1 = 0
  c2 = 0
  pipeline = [
    #{
      #'$match': {
        ##'category': {'$in': ['good', 'id', 'leftover']},
        ##'judge': {'$eq': nil}
      #}
    #},
    #{
      #'$sample': {
      #}
    #}
  ]
  #find closest keyword [anti/e, post, id, US, supra etc ]

  patterns = [
    {title: 'us', rgx: /\d{2,3}.{1,2}U[\.\,\s]\s?S[\.\,\s]/, count: 0},
    {title: 'us2', rgx: /How\./, count: 0},
    {title: 'us3', rgx: /Dall\./, count: 0},
    {title: 'us4', rgx: /Wall\./, count: 0},
    {title: 'id', rgx: /\W[iI]d/, count: 0},
    {title: 'ibid', rgx: /\W[iI]bid/, count: 0},
    {title: 'supra', rgx: /[sS][uw]pra/, count: 0},
    {title: 'supra2', rgx: /stipra/, count: 0},
    {title: 'post', rgx: /[pP]ost/, count: 0},
    {title: 'ante', rgx: /[aA]nt[ie]/, count: 0},
    {title: 'fed', rgx: /F[\.\,\s]\s?\dd/, count: 0},
    {title: 'fed supp', rgx: /[fF][\.\,\s]\s?[Ss]upp/, count: 0},
    {title: 'so', rgx: /So[\.\,\s]\s?\dd/, count: 0},
    {title: 'tc', rgx: /T[\.\,\s]\s?C[\.\,\s]/, count: 0},
    {title: 'nw', rgx: /[NS][\.\,\s]\s?[WE][\.\,\s]\s?\dd/, count: 0},
    {title: 'p', rgx: /P[\.\,\s]\s?\dd/, count: 0},
    {title: 'fed reg', rgx: /Fed\.\sReg\./, count: 0},
    {title: 'a', rgx: /A[\.\,\s]\s?\dd/, count: 0},
    {title: 'ca', rgx: /Cal\. \dth/, count: 0},
    {title: 'mj', rgx: /M[\.\,\s]\s?J[\.\,\s]/, count: 0},
    {title: 'car', rgx: /Cal\.\sRptr\./, count: 0},
    {title: 'fedappx', rgx: /Fed\.\sAppx\./, count: 0},

    #TODO pattern for citation separated by parentetical of info
    #TODO pattern for citation separated by quotation

    {title: 'paren', rgx: /[^\(]{10,}\)/, count: 0},
  ]

  capture_in_paren = [
    /[Ll]ord/
  ]
  DB[:matches].aggregate(pipeline).each do |match|
    full_text = match['op_text']
    txt = match['regexp_match_text']
    txt_i = match['regexp_match_index']

    paren_index = txt_i + txt.last_match(/\(/).begin(0)
    n = paren_index > 55 ? 55 : paren_index
    before_paren = full_text[paren_index-n, n]
    full_text_before_paren = full_text[0, paren_index]

    inside_paren = txt[txt.last_match(/\(/).begin(0), txt.length - txt.last_match(/\(/).begin(0)]

    reject_in_paren = [
      /Rep\./,
      /[pP]ost/,
      /[aA]nt[ie]/,
      /hereinafter/,
      /view/,
      /according\sto/,
      /as\sthe\sd/,
    ]

    reject_in_paren.map! { |rgx| inside_paren.match rgx }

    #skip if we matched a bad pattern inside paren
    ( next) unless reject_in_paren.compact.empty?

    #now find the first citation type
    patterns_sorted = patterns.map do |pattern|
      pattern['last_match'] = before_paren.last_match(pattern[:rgx])
      pattern
    end

    patterns_sorted.reject!{|p| p['last_match'].nil?}.sort_by{|pattern| pattern['last_match'].begin(0)}.reverse

    #skip if no citation is found (TODO maybe go back further?)
    ( next ) if patterns_sorted.empty?

    #we can get or closest citation now as first in the sorted list
    if patterns_sorted.first[:title]=='us'

      cit_str = before_paren[patterns_sorted.first['last_match'].begin(0)..-1].gsub(/\(.{4,5}\)/, '').gsub(/\Wn\. \d+/, '').gsub(/\Wnn.*/, '')

      vol = cit_str.match(/(?<vol>\d{2,3}).{1,2}U[\.\,\s]\s?S[\.\,\s]/)['vol']
      page = cit_str.last_match(/(?<page>\d+)/)['page']

      res = better_find_op(vol, page, match['judge'])

      #if res.count == 0
        #res = better_find_op(vol, page.to_i-1)
      #end

      #matches = txt.to_enum(:scan, /U[\.\,\s]\s?S/).map{Regexp.last_match} 

    elsif patterns_sorted.first[:title]=='id'
    elsif patterns_sorted.first[:title]=='ibid'
    elsif patterns_sorted.first[:title]=='supra'
    elsif patterns_sorted.first[:title]=='paren'
    end

  end

  #ap patterns

  ap c1
  ap c2
end

new_new
