import argparse
import pandas as pd
from utils import get_beer_styles, get_beer_style_info, get_brewery_info


class BeerMasterTables:
    def get_master_table():
        beer_style_links, beer_style_names = get_beer_styles()
        beer_style_info_lists = [get_beer_style_info(n, l) for n,l in zip(beer_style_names, beer_style_links)]
        beer_df = pd.concat(beer_style_info_lists, axis=0)
        # beer_df.to_gbq(project_id='scarlet-labs',
        #                destination_table="beer.beer_info_master_table",
        #                if_exists="replace",
        #                verbose=False)
        return beer_df

    def get_brewery_table():
        beer_df = pd.read_gbq(project_id='scarlet-labs',
                          query="select * from `scarlet-labs.beer.beer_info_master_table` order by ratings desc",
                          dialect='standard',
                          verbose=False)
        brewery_df_list = [get_brewery_info(x) for x in beer_df["brewery_link"].unique()]
        brewery_df = pd.concat(brewery_df_list).join(beer_df[["brewery_link", "brewery"]].drop_duplicates().set_index("brewery_link")).reset_index().rename(columns={"index":"brewery_link"})
        # beer_df.to_gbq(project_id='scarlet-labs',
        #                destination_table="beer.brewery_info",
        #                if_exists="replace",
        #                verbose=False)
        return brewery_df



def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--table-type',
                        dest='table_type',
                        default = 'brewery',
                        help='Brewery or Beers')

    args, _ = parser.parse_known_args(argv)

    if args.table_type.lower() == "brewery":
        print(("Getting Brewery Info", datetime.now().strftime("%Y%m%d %H:%M:%S")))
        df = BeerMasterTables.get_brewery_table()
        print(("Writing Output", datetime.now().strftime("%Y%m%d %H:%M:%S")))
        df.to_csv("brewery_info.csv", index=False)
    else:
        print(("Getting Master Info", datetime.now().strftime("%Y%m%d %H:%M:%S")))
        df = BeerMasterTables.get_master_table()
        print(("Writing Output", datetime.now().strftime("%Y%m%d %H:%M:%S")))
        df.to_csv("beer_info_master_table.csv", index=False)

if __name__ == '__main__':
    main()
