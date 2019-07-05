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
				regexpMatchText: regexp_match_text,
				regexpMatchIndex: regexp_match_index,
			}
    end
    DB[:all_matches].insert_many(matches)
  end
end

def add_dissent_concurrence_boolean
  DB[:all_matches].find.each do |match|
    set_values = {
      concur: match['regexpMatchText'].match(/concur/) ? true : false ,
      dissent: match['regexpMatchText'].match(/dissent/) ? true : false
    }

    DB[:all_matches].update_one({_id: match['_id']}, {'$set': set_values})
  end
end
