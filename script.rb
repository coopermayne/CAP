COUNTER = Hash.new(0)
require_relative 'global_variables.rb'
require_relative 'keys.rb' #hide this file from git!
require_relative 'custom_methods.rb' #hide this file from git!
require_relative 'generate_matches.rb' 


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

def new_new
  count_array = [0,0,0,0,0,0,0]

  pipeline = [
    {
      '$match': {
        'cit_to': {'$exists': 0}
      }
    },
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

    count_array[6]+=1
    ap count_array

    set_values =  {
      cit_to: {
        kase_id: nil,
        op_index: nil,
        last_cit_type: nil,
        no_match: nil,
        multiple_matches: nil,
        vol: nil,
        page: nil,
        next: nil,
      }
    } #this will eventually be filled up and sent to db with info from this function

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
    (count_array[0]+=1; next) unless reject_in_paren.compact.empty?

    #now find the first citation type
    patterns_sorted = []
    patterns.each do |pattern|
      h = {
        title: pattern[:title],
        last_match: before_paren.last_match(pattern[:rgx])
      }
      patterns_sorted << h
    end

    patterns_sorted = patterns_sorted.reject{|p| p[:last_match].nil?}.sort_by{|pattern| pattern[:last_match].begin(0)}.reverse
      

    #skip if no citation is found (TODO maybe go back further?)
    (count_array[1]+=1; next ) if patterns_sorted.empty?

    set_values[:cit_to][:last_cit_type] = patterns_sorted.first[:title]

    count_array[2]+=1

    #we can get or closest citation now as first in the sorted list
    if patterns_sorted.first[:title]=='us'
      #52%
      cit_str = before_paren[patterns_sorted.first[:last_match].begin(0)..-1].gsub(/\(.{4,5}\)/, '').gsub(/\Wn\. \d+/, '').gsub(/\Wnn.*/, '')

      vol = cit_str.match(/(?<vol>\d{2,3}).{1,2}U[\.\,\s]\s?S[\.\,\s]/)['vol']
      page = cit_str.last_match(/(?<page>\d+)/)['page']

      res = better_find_op(vol, page, match['judge'])
      
      #add info to set_values hash
      if res.class == BSON::Document
        set_values[:cit_to][:kase_id] = res[:_id]
        set_values[:cit_to][:op_index] = res[:opIndex]
			elsif res.class == Mongo::Collection::View::Aggregation
        set_values[:cit_to][:multiple_matches] = res.map{|op| op[:frontend_url]}.uniq
      elsif res.nil?
        set_values[:cit_to][:no_match] = true
      end

    elsif patterns_sorted.first[:title]=='id'
      #16% -- just do same as for US and then go through by hand checking these carefully

      page_match = before_paren[patterns_sorted.first[:last_match].begin(0)..-1].gsub(/\(.{4,5}\)/, '').gsub(/\Wn\. \d+/, '').gsub(/\Wnn.*/, '').last_match(/(?<page>\d+)/)
      page = page_match.nil? ? nil : page_match['page']

      #find last non-id citation
      id_patterns_sorted = []
      patterns.reject{|pattern| ['id'].include? pattern[:title] }.each do |pattern|
        id_patterns_sorted << {
          title: pattern[:title],
          last_match: full_text_before_paren.last_match(pattern[:rgx])
        }
      end

      id_patterns_sorted = id_patterns_sorted.reject{|p| p[:last_match].nil?}.sort_by{|pattern| pattern[:last_match].begin(0)}.reverse

      #if last is us then grab the volume and repsonse from better_find_op
      if id_patterns_sorted.first[:title].match /^us/

        vol_match = full_text_before_paren.last_match(/(?<vol>\d{2,3}).{1,2}U[\.\,\s]\s?S[\.\,\s]/)
        vol = vol_match.nil? ? nil : full_text_before_paren.last_match(/(?<vol>\d{2,3}).{1,2}U[\.\,\s]\s?S[\.\,\s]/)['vol']

        res = better_find_op(vol, page, match['judge'])

        #add info to set_values hash
        if res.class == BSON::Document
          set_values[:cit_to][:kase_id] = res[:_id]
          set_values[:cit_to][:op_index] = res[:opIndex]
        elsif res.class == Mongo::Collection::View::Aggregation
          set_values[:cit_to][:multiple_matches] = res.map{|op| op[:frontend_url]}.uniq
        elsif res.nil?
          set_values[:cit_to][:no_match] = true
        else
          set_values[:cit_to][:no_match] = true
        end
      else
        set_values[:cit_to][:next] = id_patterns_sorted.first[:title]
      end

    elsif patterns_sorted.first[:title]=='ibid'
    elsif patterns_sorted.first[:title]=='supra'
      #8%
    elsif patterns_sorted.first[:title]=='paren'
    else
      #22%
    end


    #get paragraph text
    mcounter=0
    paragraph = full_text.split("\n").select do |paragraph|
      mcounter_old = mcounter
      mcounter += paragraph.length
      txt_i > mcounter_old && txt_i < mcounter
    end.first

    set_values[:cit_to][:paragraph] = paragraph

    ap DB[:matches].update_one({'_id' => match['_id']}, {'$set' => set_values})

    #ap count_array.map{|item| (item.to_f/count_array.inject{|x, sum| sum+x}.to_f).round(2)*100}
  end

end

def save_matches_to_csv
  pipeline = [{'$match': {'cit_to': {'$exists': 1}}}]
  rows = DB[:matches].aggregate(pipeline).map do |match|
      
    #case citing
    kase = DB[:ALL].find({'_id' => match['kase_id']}).first
    op = kase['casebody']['data']['opinions'][match['op_index']]
   
    #case cited to
    cit_kase = DB[:ALL].find({'_id' => match['cit_to']['kase_id']}).first
    cit_kase = {} if cit_kase.nil?
    begin
      cit_op = cit_kase['casebody']['data']['opinions'][match['cit_to']['op_index']]
    rescue
      cit_op = {} if cit_op.nil?
    end

    {
      #for potential updates
      :_id => match['_id'],
      #kase info
      :decision_date => kase['decision_date'],
      :case_name => kase['name_abbreviation'],
      :docket_number => kase['docket_number'],
      :case_citation => kase['citations'].first['cite'],
      :author => op['author'],
      :author_formatted => op['author_formatted'],
      :part_of_opinion => op['type'],

      #cit case info
      :d_judge_from_scrape => match['judge'],
      :d_judge_from_match => cit_op['author_formatted'],
      :d_citation => cit_kase['cite'],
      :d_case_name => cit_kase['name_abbreviation'],

      #blurb
      :d_blurb => match['cit_to']['paragraph'],
      :d_matching_txt => match['regexp_match_text'],
      :d_last_cit_type => match['cit_to']['last_cit_type'],
      :d_mult_matches => match['cit_to']['multiple_matches'],
      :d_no_match => match['cit_to']['no_match'],
      :d_next => match['cit_to']['next'],
      :d_category => match['category'],

      :full_opinion => kase['frontend_url'],

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

def import_data_from_sheet
	csv_text = File.read('new_data.csv')
	csv = CSV.parse(csv_text, :headers => true)
	csv.each do |row|
		binding.pry
    {
      #for potential updates
      :_id => match['_id'],

      #cit case info
      :d_judge_from_match => cit_op['author_formatted'],
      :d_citation => cit_kase['cite'],
      :d_case_name => cit_kase['name_abbreviation'],

    }

    #DB[:matches].update_one({'_id' => match['_id']}, {'$set' => set_values})
	end
end

def migration_change_brandis_to_brandeis
  pipeline = [
    {
      '$unwind': {
        'path': '$casebody.data.opinions', 
        'includeArrayIndex': 'opIndex'
      }
    }
  ]

  #DB[:ALL].aggregate(pipeline).each do |op|
  #end
end

def better_find_op(vol, page, judge, extra={})
  #returns best match op
  #        nil if no match
  #        array of res if multiples can't be narrowed
  
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
  #elsif results.count == 1
    #COUNTER[:one] +=1
    #COUNTER[:one_auth_match] +=1
    ##TODO mark ones that seem wrong (like wrong judge etc)
    #return results.first
  else
  #elsif results.count > 1

    #judge = "brandis" if judge=="brandeis"
    #judge = "brandis" if judge=="marshall"
    m_auth = results.select{|r|  !r['casebody']['data']['opinions']['author_formatted'].nil? && !judge.nil? && JaroWinkler.distance( r['casebody']['data']['opinions']['author_formatted'], judge.downcase.gsub(/[^a-z]/,'') ) > 0.9 }

#JaroWinkler.jaro_distance("dissent", ii)

    type_rgx = extra[:lm][:dissent] ? /dissent/ : /concur/
    m_type = results.select{|r|  !r['casebody']['data']['opinions']['type'].nil? && r['casebody']['data']['opinions']['type'].match(type_rgx) }

    if m_auth.count==1
      COUNTER[:auth] +=1
      return m_auth.first
    end

    if judge.nil? && m_type.count==1
      COUNTER[:type] +=1
      return m_type.first
    end

    return results
  end
end

def better_find_kase(vol, page, judge, extra={})
end

get_citations_before_matches
