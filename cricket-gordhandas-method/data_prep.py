import json
import pandas as pd
import os


# Revised function to parse the cricket data according to the actual structure of the JSON file
def parse_cricket_data(file_path):
    # Read JSON file
    with open(file_path) as f:
        data = json.load(f)
    
    if len(data['innings']) != 2:
        return None

    if data['info']['outcome'].get('method') == 'D/L':
        return None
    # Assuming the second entry in the 'innings' array corresponds to the second innings
    second_innings = data['innings'][1]['overs']
    target_score = data['innings'][1]['target']['runs']
    target_over = data['innings'][1]['target']['overs']

    match_id = file_path.split('/')[-1].split('.')[0]

    # Variables to keep track of cumulative score and wickets
    cumulative_score = 0
    cumulative_wickets = 0

    # List to store each row of data
    rows = []

    for over in second_innings:
        over_number = over['over']
        runs_in_over = 0
        wickets_in_over = 0

        for delivery in over['deliveries']:
            runs_in_over += delivery['runs']['total']
            if 'wickets' in delivery:
                wickets_in_over += len(delivery['wickets'])

        cumulative_score += runs_in_over
        cumulative_wickets += wickets_in_over

        rows.append({
            'match_id': match_id,
            'over_number': over_number,
            # 'cumulative_score': cumulative_score,
            'cumulative_average_score': cumulative_score / (over_number+1),
            'runs_scored_in_over': runs_in_over,
            'cumulative_wickets_lost': cumulative_wickets,
            'wickets_lost_in_over': wickets_in_over,
            'target_score': target_score,
            'target_over': target_over
        })

    return pd.DataFrame(rows)


def process_all_json_files(directory):
    all_files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.json')]
    all_dfs = []

    for file in all_files:
        try:
            df = parse_cricket_data(file)
            if df is not None:
                all_dfs.append(df)
            
        except Exception as e:
            print(f"Error processing file {file}: {e}")

    combined_df = pd.concat(all_dfs, ignore_index=True)
    return combined_df


if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_directory = os.path.join(script_dir, 'data')
    # # Test the function on a single file
    # test_file = os.path.join(data_directory, '1193505.json')
    # test_df = parse_cricket_data(test_file)
    # if (test_df is not None):
    #     print(test_df.head())
    combined_df = process_all_json_files(data_directory)
    combined_df.to_csv(os.path.join(script_dir, 'processed_data.csv'), index=False)
