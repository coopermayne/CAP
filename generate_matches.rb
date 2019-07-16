def generate_dissent_and_concurrences_matches

  DB[:all_matches].delete_many {}

  pipeline = [
    {
      '$match': {
        'decision_date': {
          '$gte': '1900'
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

  #main pattern pick up all lines with dissent or concur in parens
  pattern = /\([^\(\)]*(dissent|concur)[^\(\)]*\)/

  DB[:ALL].aggregate(pipeline).each do |opinion|

    kase_id = opinion['_id']
    op_text = opinion['casebody']['data']['opinions']['text']
    op_index = opinion['opIndex']

    matches = op_text.to_enum(:scan, pattern).map do |item| 
      regexp_match_text = Regexp.last_match[0]
      regexp_match_index =  Regexp.last_match.begin(0)

			{
				kaseId: kase_id,
				opIndex: op_index,
				matchText: regexp_match_text,
				matchIndex: regexp_match_index,
        concur: regexp_match_text.match(/concur/) ? true : false ,
        dissent: regexp_match_text.match(/dissent/) ? true : false
			}
    end
    DB[:all_matches].insert_many(matches)
  end
end

def clean_judge_name(dirty_name)
  I18n.transliterate(dirty_name.downcase).gsub(/[^a-z\s]/,'')
end

def guess_scotus_judge
  #this just pick the first scotus judge mentioned
  #
  DB[:all_matches].find.each do |match|

    judges = JUDGES.map{|j|j[:last_name]}.uniq.map do |judge_name|
      judge_name = clean_judge_name(judge_name)
      match_text = clean_judge_name(match[:matchText])
      i = match_text.index(judge_name)
      { i: i, last_name: judge_name }
    end

    judges = judges.reject{|j| j[:i].nil?}.sort_by{|j| j[:i]}.map{|j| j[:last_name]}
    set_values = {judgeGuessFromMatchText: judges.first}

    DB[:all_matches].update_one({_id: match[:_id]},{'$set': set_values})
  end
end

def reject_some
  #TODO can we reject more? this is only getting 300!
  pipeline = [
    {
      '$match': {
        'reject': false
      }
    },
    {
      '$lookup': {
        'from': 'ALL', 
        'localField': 'kaseId', 
        'foreignField': '_id', 
        'as': 'kase'
      }
    }
  ]

  reject_pattern_in_paren = [
    /\WRep\./,
    /\W[pP]ost/,
    /\W[aA]nt[ie]/,
    /\Whereinafter/,
    /\Wview/,
    /\Waccording\sto/,
    /\Was\sthe\sd/,
    /\(with/,
    /\Wconcurrent/,
    /\(one\sjudge/,
    /\WHouse/,
  ]

  DB[:all_matches].aggregate(pipeline).each do |match|
    reject = false
    reject_pattern_in_paren.each do |rgx|
      reject = true if match[:matchText].match rgx
    end

    set_values = {reject: reject}

    #DB[:all_matches].update_one({'_id': match['_id']}, {'$set': set_values})
  end

end

def recursive_citation_search(op_text, index, extra_info={})
  #take a string as input
  #returns [match(match data), id_match(boolean)]

  patterns = [
    {title: 'us', rgx: /(?<vol>\d{2,3}).{1,2}U.{0,2}S[\.\,\s]/, count: 0},
    {title: 'us', rgx: /(?<vol>\d+)\sHow\./, count: 0},
    {title: 'us', rgx: /(?<vol>\d+)\sDall\./, count: 0},
    {title: 'us', rgx: /(?<vol>\d+)\sWall\./, count: 0},
    {title: 'id', rgx: /\W[iI]d/, count: 0},
    {title: 'id', rgx: /\W[iI]bid/, count: 0},
    {title: 'supra', rgx: /[sS][uw]pra/, count: 0},
    {title: 'supra', rgx: /stipra/, count: 0},
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
    {title: 'fpc', rgx: /\d+\sF.{1,2}P.{1,2}C/, count: 0},

    #{title: 'paren', rgx: /[^\(]{10,}\)/, count: 0},
  ]

  last_match = nil
  keep_searching = true
  id = false
  loop_index = index

  while keep_searching
    op_text_cut = op_text[0..loop_index]

    patterns_sorted = []
    patterns.each do |pattern|
      h = {
        title: pattern[:title],
        last_match: op_text_cut.last_match(pattern[:rgx])
      }
      patterns_sorted << h
    end

    patterns_sorted = patterns_sorted.reject{|p| p[:last_match].nil?}.sort_by{|pattern| pattern[:last_match].begin(0)}.reverse

    last_match = patterns_sorted.first
    break if last_match.nil? #end if no matches at all
    id=true if last_match[:title]=='id'
    keep_searching = false unless last_match[:title]=='id'

    loop_index = last_match[:last_match].begin(0)
  end

  #ap op_text[index-500..index+extra_info[:match][:matchText].length]

  {
    last_match: last_match.nil? ? nil : last_match,
    id_match: id
  }

  #look for last citation
  #if id or supra look for referenced citation
  
end

def get_first_citation(input_string)
  #return first citation from string
  patterns = [
    {title: 'us', rgx: /(?<vol>\d{2,3}).{1,2}U.{0,2}S[\.\,\s]\s?(?<page>\d+)/, count: 0},
    {title: 'us', rgx: /(?<vol>\d+)\sHow\./, count: 0},
    {title: 'us', rgx: /(?<vol>\d+)\sDall\./, count: 0},
    {title: 'us', rgx: /(?<vol>\d+)\sWall\./, count: 0},
    {title: 'id', rgx: /\W[iI]d/, count: 0},
    {title: 'id', rgx: /\W[iI]bid/, count: 0},
    {title: 'supra', rgx: /[sS][uw]pra/, count: 0},
    {title: 'supra', rgx: /stipra/, count: 0},
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
    {title: 'fpc', rgx: /\d+\sF.{1,2}P.{1,2}C/, count: 0},

    #{title: 'paren', rgx: /[^\(]{10,}\)/, count: 0},
  ]

  sorted_patterns = []
  patterns.each do |pattern|
    next if input_string.match(pattern[:rgx]).nil?

    h = {
      title: pattern[:title],
      rgx: pattern[:rgx],
      match: input_string.match(pattern[:rgx]),
      idx: input_string.match(pattern[:rgx]).begin(0)
    }
    sorted_patterns << h
  end
  return sorted_patterns.reject{|h| h[:idx].nil?}.sort_by{|h| h[:idx]}.first
end

def get_citations_before_matches

  cs = Hash.new(0)

	pipeline = [
    #for testing only
		{
			'$lookup': {
				'from': 'ALL', 
				'localField': 'kaseId', 
				'foreignField': '_id', 
				'as': 'kase'
			}
		}
	]

  results = Hash.new(0)

  total_count = DB[:all_matches].aggregate(pipeline).count
  ap total_count
  count = 0
  DB[:all_matches].aggregate(pipeline).each do |match|
    ap count
    count += 1

    set_values = {cit_to: {}} #for updating each match in db

    op_text = match[:kase].first[:casebody][:data][:opinions][match[:opIndex]][:text]
    index = match[:matchIndex]
    res = recursive_citation_search(op_text, index , {match: match})

    set_values[:cit_to][:citation_type] = res[:last_match] && res[:last_match][:title]
    set_values[:id_match] = res[:id_match]
    set_values[:no_match_from_recursive_citation_search] = false

    if res[:last_match].nil?
      set_values[:no_match_from_recursive_citation_search] = true
    elsif res[:last_match][:title] == 'us'
      #get volume and page number and run search for kase info
      #52%
      cit_str = op_text[res[:last_match][:last_match].begin(0)..index-1].gsub(/\(.{4,5}\)/, '').gsub(/\Wn\. \d+/, '').gsub(/\Wnn.*/, '')

      vol = res[:last_match][:last_match]['vol']
      page = cit_str.last_match(/(?<page>\d+)/)['page']

      set_values[:cit_to][:vol] = vol
      set_values[:cit_to][:page] = page

      res = better_find_op(vol, page, match['judgeGuessFromMatchText'], {lm: match})

    elsif res[:last_match][:title] == 'supra'
      res = find_supra_match res[:last_match][:last_match], op_text, index, {match: match}
    end

    #add info to set_values hash
    if res.class == BSON::Document
      set_values[:find_CAP_data_response] = 'single'
      set_values[:cit_to][:kase_id] = res[:_id]
      set_values[:cit_to][:op_index] = res[:opIndex]
    elsif res.class == Mongo::Collection::View::Aggregation
      set_values[:find_CAP_data_response] = 'multiple'

      if res.count==1
        set_values[:cit_to][:kase_id] = res.first[:_id]
        set_values[:cit_to][:op_index] = res.first[:opIndex]
      else
        res_map = res.map{|op| op[:frontend_url]}.uniq
        set_values[:cit_to][:multiple_matches] = res_map
      end

    elsif res.nil?
      set_values[:find_CAP_data_response] = 'no match'
    end

    ap set_values
    DB[:all_matches].update_one({_id: match[:_id]},{'$set': set_values})
    ap "done: #{((count.to_f/total_count.to_f)*100).round(1)}%"
  end
end

def find_supra_match (last_match, op_text, index, extra_info)
  #get case name ref
  text = op_text[last_match.begin(0)-50...last_match.begin(0)]+'supra'
	#case_name = text.match /(?<name>(([A-Z][a-z\.\-]+\s?)|v\.?\s?|of\s){1,}).?.?supra/
	case_name = text.match /(?<name>(([A-Z][a-z\.\-]+\s?)|of\s){1,}).?.?supra/
  case_name = case_name['name'].gsub(/See/,'').strip unless case_name.nil?

  # search for first mention
  idx = nil
  if case_name.nil?
    return nil
  else
    rgx_before = /#{case_name}[^;]{1,30}v\./
    before_v = op_text.to_enum(:scan, rgx_before).map {Regexp.last_match.begin 0}
    rgx_after = /v\.[^;]{1,30}#{case_name}/
    after_v = op_text.to_enum(:scan, rgx_after).map {Regexp.last_match.begin 0}

    idx = after_v.first if after_v.count>0 && before_v.count==0
    idx = before_v.first if after_v.count==0 && before_v.count>0
  end

  #get first citation
  if !idx.nil?
    res = get_first_citation(op_text[idx, 100])

    page = op_text[last_match.begin(0)..index].last_match(/\d+/)
    page = page[0] if page
    if res && res[:title]=='us' && page
      return better_find_op(res[:match][:vol], page, extra_info[:match][:judgeGuessFromMatchText], {lm: extra_info[:match]})
    else
      return nil
    end
  else
    return nil
  end
end

def find_supra_match2 (last_match, op_text, index, extra_info)
  #get case name ref
  text = op_text[last_match.begin(0)-50...last_match.begin(0)]+'supra'
	#case_name = text.match /(?<name>(([A-Z][a-z\.\-]+\s?)|v\.?\s?|of\s){1,}).?.?supra/
	case_name = text.match /(?<name>(([A-Z][a-z\.\-]+\s?)|of\s){1,}).?.?supra/
  case_name = case_name['name'].gsub(/See/,'').strip unless case_name.nil?

  # search for first mention
  idx = nil
  if case_name.nil?
    return nil
  else
    rgx_before = /#{case_name}[^;]{1,30}v\./
    before_v = op_text.to_enum(:scan, rgx_before).map {Regexp.last_match.begin 0}
    rgx_after = /v\.[^;]{1,30}#{case_name}/
    after_v = op_text.to_enum(:scan, rgx_after).map {Regexp.last_match.begin 0}

    idx = after_v.first if after_v.count>0 && before_v.count==0
    idx = before_v.first if after_v.count==0 && before_v.count>0
  end

  #get first citation
  if !idx.nil?
    res = get_first_citation(op_text[idx, 100])

    page = op_text[last_match.begin(0), 50].last_match(/\d+/)
    page = page[0] if page
    if res && res[:title]=='us' && page
      return better_find_op(res[:match][:vol], page, extra_info[:match][:judgeGuessFromMatchText], {lm: extra_info[:match]})
    else
      return nil
    end
  else
    return nil
  end
end

def scdb_get_kase_from_citation(vol, page)
	pipeline = [
		{
			'$match': {
				'citeDetails.vol': vol, 
				'citeDetails.page': {
					'$lte': page
				}
			}
		}, {
			'$sort': {
				'citeDetails.page': -1
			}
		}
	]

  DB[:scdb].aggregate(pipeline).first
end

def add_citation_fields_to_scdb
  #check for kase name and cite from pin cite using scdb

  DB[:scdb].find.each do |kase|
    
    rgx = /(?<vol>\d+)\sU.{0,2}S\.?\s(?<page>\d+)/

    next if kase['usCite'].nil?
    matches = kase['usCite'].match rgx
    next if matches.nil?

    set_values = {
      citeDetails: {
        vol: matches[:vol].to_i,
        page: matches[:page].to_i
      }
    }

    ap DB[:scdb].update_one({_id: kase['_id']}, {'$set': set_values})
  end
end

def add_scdb_data
  pipeline = [{
    '$match': {
      '$and': [
        {
          'cit_to.vol': {
            '$exists': 1
          }
        }, {
          'cit_to.page': {
            '$exists': 1
          }
        }
      ]
    }
  }]

  DB[:all_matches].aggregate(pipeline).each do |match|
    vol = match[:cit_to][:vol].to_i
    page = match[:cit_to][:page].to_i

    ap vol

    res = scdb_get_kase_from_citation(vol, page)
    next if res.nil?
    ap res[:caseName]
    set_values = {
      'cit_to.scdb_id': res[:_id]
    }
    DB[:all_matches].update_one({_id: match['_id']}, {"$set": set_values})
  end
end

def save_all_matches_to_spreadsheet
  #pipeline = [{'$match': {'cit_to': {'$exists': 1}}}]
  pipeline = []
  rows = DB[:all_matches].aggregate(pipeline).map do |match|

    kase = DB[:ALL].find({_id: match[:kaseId]}).first
    op = kase[:casebody][:data][:opinions][match[:opIndex]]
    #scdb_id = match[:cit_to] && match[:cit_to][:scdb_id]
    #scdb_kase = DB[:scdb].find({_id: scdb_id}).first
    
    begin
      _kase = DB[:ALL].find({_id: match[:cit_to][:kase_id]}).first
      _op = _kase[:casebody][:data][:opinions][match[:cit_to][:op_index]]
    rescue
      _kase = {}
      _op = {}
    end

    _scdb_id = match[:cit_to] && match[:cit_to][:scdb_id]
    _scdb_kase = DB[:scdb].find({_id: _scdb_id}).first

    blurb = op['text'][match['matchIndex']-100..match['matchIndex']-1]+match['matchText']

    ii = match['matchIndex']-500
    ii = ii<0 ? 0 : ii
    long_blurb = op['text'][ii..match['matchIndex']-1]+match['matchText']
    
    secs = op['text'].split('\n')
    paragraph = op_text[0..diss_match.begin(0)-1].split("\n").last + diss_match[0] + op_text[diss_match.end(0)..-1].split("\n").first

      #:_scdb_kase_name1 => _scdb_kase_name1,
      #:_scdb_kase_cit1 => _scdb_kase_cit1,
      #:_scdb_kase_name2 => _scdb_kase && _scdb_kase['caseName'],
      #:_scdb_kase_cit2 => _scdb_kase && _scdb_kase['usCite'],

    if _scdb_kase
      binding.pry
    else
      _scdb_kase_cit1 = nil
      _scdb_kase_name1 = nil
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

      #type
      :citation_type => match['cit_to']['citation_type'],
      :id_match => match['id_match'],
      :_diss => match['dissent'],
      :_concur => match['concur'],

      #cit case info
      :_judge_from_scrape => match['judgeGuessFromMatchText'],
      :_judge_from_match => _op && _op['author_formatted'],
      :_vol => match['cit_to']['vol'],
      :_page => match['cit_to']['page'],
      :_citation => _kase && _kase['cite'],
      :_case_name => _kase && _kase['name_abbreviation'],
      :_scdb_kase_name1 => _scdb_kase_name1,
      :_scdb_kase_cit1 => _scdb_kase_cit1,
      :_scdb_kase_name2 => _scdb_kase && _scdb_kase['caseName'],
      :_scdb_kase_cit2 => _scdb_kase && _scdb_kase['usCite'],

      #blurb
      :blurb => blurb,
      :long_blurb => long_blurb,
      :paragraph => paragraph,
      :matching_txt => match['matchText'],

      :reject => match[:reject],
      :multiple_matches => match[:cit_to][:multiple_matches],
      :distance => match[:cit_to][:distance],

      :full_opinion => kase['frontend_url'],
    }
  end

  fn = "/Users/coopermayne/Code/UCLA_Re/export/#{Time.now.to_i.to_s}_all_matches.csv"

	rowid = 0
	CSV.open(fn, 'w') do |csv|
		rows.each do |hsh|
			rowid += 1
			if rowid == 1
				csv << hsh.keys
			else
				ap hsh.values
				csv << hsh.values
			end
		end
	end
end

def redo_supras
  #messed up the subefore_matches

	pipeline = [
    {
      '$match': {
        'cit_to.citation_type': 'supra'
      }
    },
		{
			'$lookup': {
				'from': 'ALL', 
				'localField': 'kaseId', 
				'foreignField': '_id', 
				'as': 'kase'
			}
		}
	]

  DB[:all_matches].aggregate(pipeline).each do |match|
    set_values = {cit_to: {}} #for updating each match in db

    op_text = match[:kase].first[:casebody][:data][:opinions][match[:opIndex]][:text]
    index = match[:matchIndex]
    res = recursive_citation_search(op_text, index , {match: match})

    set_values[:cit_to][:citation_type] = res[:last_match] && res[:last_match][:title]
    set_values[:id_match] = res[:id_match]
    set_values[:no_match_from_recursive_citation_search] = false

    res2 = find_supra_match2(res[:last_match][:last_match], op_text, index, {match: match})

    distance = index - res[:last_match][:last_match].begin(0)
    set_values[:cit_to][:distance] = distance

    res = find_supra_match2(res[:last_match][:last_match], op_text, index, {match: match})

    #add info to set_values hash
    if res.class == BSON::Document
      set_values[:find_CAP_data_response] = 'single'
      set_values[:cit_to][:kase_id] = res[:_id]
      set_values[:cit_to][:op_index] = res[:opIndex]

    elsif res.class == Mongo::Collection::View::Aggregation
      set_values[:find_CAP_data_response] = 'multiple'

      if res.count==1
        set_values[:cit_to][:kase_id] = res.first[:_id]
        set_values[:cit_to][:op_index] = res.first[:opIndex]
      else
        res_map = res.map{|op| op[:frontend_url]}.uniq
        set_values[:cit_to][:multiple_matches] = res_map
      end

    elsif res.nil?
      set_values[:find_CAP_data_response] = 'no match'
    end

    ap DB[:all_matches].update_one({_id: match[:_id]},{'$set': set_values})
  end
end

def add_distance_to_cit_to

	pipeline = [
    #for testing only
    {
      '$match': {
        'cit_to.citation_type': 'us'
      }
    },
		{
			'$lookup': {
				'from': 'ALL', 
				'localField': 'kaseId', 
				'foreignField': '_id', 
				'as': 'kase'
			}
		}
	]

  DB[:all_matches].aggregate(pipeline).each do |match|

    op_text = match[:kase].first[:casebody][:data][:opinions][match[:opIndex]][:text]
    index = match[:matchIndex]
    res = recursive_citation_search(op_text, index , {match: match})

    distance = index - res[:last_match][:last_match].begin(0)
    #add info to set_values hash

    set_values = {'cit_to.distance': distance}
    ap DB[:all_matches].update_one({_id: match[:_id]},{'$set': set_values})
  end
end

def get_answers
  pipeline = [
    {
      '$match': {
        'reject': false, 
        'dissent': true, 
        'cit_to.citation_type': {
          '$in': [
            'us', 'supra'
          ]
        }
      }
    },
    #{
      #'$sample': { 'size': 1000}
    #}
  ]

  g = Hash.new(0)
  g_wsc = Hash.new(0)

  DB[:all_matches].aggregate(pipeline).each do |match|
    kase = DB[:ALL].find({_id: match[:kaseId]}).first
    op = kase[:casebody][:data][:opinions][match[:opIndex]]

    begin
      _kase = DB[:ALL].find({_id: match[:cit_to][:kase_id]}).first
      _op = _kase[:casebody][:data][:opinions][match[:cit_to][:op_index]]
    rescue
      next
    end

    judge = match['judgeGuessFromMatchText'] || _op['author_formatted']

    g[judge] += 1

    g_wsc[judge]+=1 unless judge == op['author_formatted']

  end

  g = g.sort_by{|k,v| v}.reverse.map{|item| item[2]=g_wsc[item[0]]; item}

  fn = "/Users/coopermayne/Code/UCLA_Re/export/#{Time.now.to_i.to_s}_dissenters_with_and_without_self_cites.csv"

  CSV.open(fn, 'w') do |csv|
    g.each do |row|
      csv << row
    end
  end
end

def count_term_occurance(term)
  pipeline = [
    #{'$sample': {'size': 1000}},
    {
      '$unwind': {
        'path': '$casebody.data.opinions', 
        'includeArrayIndex': 'opIndex', 
        'preserveNullAndEmptyArrays': false
      }
    }
  ]
  count = 0
  DB[:ALL].aggregate(pipeline).each do |op|
    op_text = op['casebody']['data']['opinions']['text']
    matches = op_text.to_enum(:scan, term){Regexp.last_match}
    count += matches.count
    ap count
  end

end


#TODO 
#(Mr. Justice Holmes, dissenting, in Southern Pacific Co. v. Jensen, 244 U. S. 205, 222)
    #get citaitons in parens
# Olmstead v. United States, (dissent)
    # maybe do a regex with the 'v'

#TODO 
#write function to guess at supra citaitons
#

