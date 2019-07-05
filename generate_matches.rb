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

def recursive_citation_search(op_text, index, extra_info={})
  #take a string as input

  patterns = [
    {title: 'us', rgx: /\d{2,3}.{1,2}U.{0,2}S[\.\,\s]/, count: 0},
    {title: 'us', rgx: /How\./, count: 0},
    {title: 'us', rgx: /Dall\./, count: 0},
    {title: 'us', rgx: /Wall\./, count: 0},
    {title: 'id', rgx: /\W[iI]d/, count: 0},
    {title: 'ibid', rgx: /\W[iI]bid/, count: 0},
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
    ap [loop_index, last_match[:title]]
  end

  ap op_text[index-500..index+extra_info[:match][:matchText].length]
  ap last_match
  ap extra_info[:match][:judgeGuessFromMatchText]

  last_match.nil? ? nil : last_match[:title]

  #look for last citation
  #if id or supra look for referenced citation
  
end

def get_citations_before_matches
	pipeline = [
    #for testing only
    {
      '$match': {
        'rejected': {'$ne': true}
      }
    },
    {
      '$sample': {
        'size': 1000
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

  results = Hash.new(0)

  DB[:all_matches].aggregate(pipeline).each do |match|
    op_text = match[:kase].first[:casebody][:data][:opinions][match[:opIndex]][:text]
    res = recursive_citation_search(op_text, match[:matchIndex], {match: match})
    results[res] += 1 
  end

  ap results
end

def reject_some
  pipeline = [
    {
      '$match': {
        'rejected': {'$ne': true}
      }
    },
    #{
      #'$sample': {
        #'size': 500
      #}
    #},
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

    DB[:all_matches].update_one({'_id': match['_id']}, {'$set': set_values})
  end

end

#TODO 
#(Mr. Justice Holmes, dissenting, in Southern Pacific Co. v. Jensen, 244 U. S. 205, 222)
    #get citaitons in parens
# Olmstead v. United States, (dissent)
    # maybe do a regex with the 'v'

#TODO 
#write function to guess at supra citaitons
