import argparse
from datetime import datetime
import pandas as pd
from utils import get_beer_vector, get_beer_style_recommendations, splitDftoDict


def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--project',
                        dest='project',
                        default = 'scarlet-labs',
                        help='This is the GCP project you wish to send the data')
    parser.add_argument('--nchunks',
                        dest='nchunks',
                        default=200,
                        help='total chunks for streaming data to bq')

    args, _ = parser.parse_known_args(argv)

    beer_df = pd.read_gbq(project_id='scarlet-labs',
                      query="select * from `scarlet-labs.beer.beer_info_master_table` order by ratings desc",
                      dialect='standard')

    beer_style_dict = splitDftoDict(df=beer_df, split_col="beer_style_clean")
    recs_df = pd.DataFrame()
    for style in beer_df['beer_style_clean'].unique():
            print((style, datetime.now().strftime("%Y%m%d %H:%M:%S")))
            recs = get_beer_style_recommendations(beer_style_dict[style])
            try:
                recs_df = pd.concat([recs_df, recs], axis=0, ignore_index=True)
            except:
                next

    recs_df.to_csv("recommendations.csv", index=False)
    recs.to_gbq(project_id=args.project,
                destination_table="beer.recommendations",
                if_exists="replace",
                chunksize=args.nchunks,
                verbose=False)

if __name__ == '__main__':
    main()
