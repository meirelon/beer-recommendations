import pandas as pd
from utils import get_beer_styles, get_beer_style_info

def main():
    beer_style_links, beer_style_names = get_beer_styles()
    beer_style_info_lists = [get_beer_style_info(n, l) for n,l in zip(beer_style_names, beer_style_links)]
    beer_df = pd.concat(beer_style_info_lists, axis=0)
    beer_df.to_csv("beer_info_master_table.csv", index=False)
    # beer_df.to_gbq(project_id='scarlet-labs',
    #                destination_table="beer.beer_info_master_table",
    #                if_exists="replace",
    #                verbose=False)


if __name__ == '__main__':
    main()
