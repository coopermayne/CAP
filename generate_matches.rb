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

#def recursive_citation_search(string)
  #take a string as input

  #look for last citation
  #if id or supra look for referenced citation
  
#end
