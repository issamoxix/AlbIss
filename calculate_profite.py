import pandas as pd
import requests
import json
from datetime import datetime


def main(ag_bm=False):
    print("Calculating Profite Started  !")
    response = requests.get(
        "https://raw.githubusercontent.com/issamoxix/albion-auto-searh/master/items2.json"
    )
    items_index = response.json()

    df = pd.read_csv("data/P.csv", index_col=False, on_bad_lines="skip")
    cols = [
        "UnitPriceSilver",
        "Amount",
        "Tier",
        "QualityLevel",
        "EnchantmentLevel",
        "ItemTypeId",
        "BuyerName",
    ]

    df = df[cols]
    df["items_name"] = df["ItemTypeId"].map(lambda x: items_index.get(x, "None"))

    cols.append("items_name")

    # saperate the Black & Carlean Market Data
    if ag_bm:
        black_market_prices = pd.read_csv("data/b_m.csv")
    else:
        black_market_prices = df[df["BuyerName"] == "@BLACK_MARKET"]
    carleaon_market_prices = df[df["BuyerName"] != "@BLACK_MARKET"]

    # Remove duplicates and get the lowest price from the carleaon Market
    carleaon_market_prices = carleaon_market_prices.sort_values(
        by=["UnitPriceSilver"], ascending=True
    )
    carleaon_market_prices = carleaon_market_prices.drop_duplicates(
        subset=["Tier", "QualityLevel", "EnchantmentLevel", "ItemTypeId", "items_name"],
        keep="first",
    )

    # set the Silver owned as curr_silver
    # loop thru all the carleaon items and comapre them to the black market items
    results = []
    items = []

    x = 10000
    curr_silver = 1000000
    for index, carleaon_market_price in carleaon_market_prices.iterrows():
        c_price = carleaon_market_price.UnitPriceSilver / x
        c_tier = carleaon_market_price.Tier
        c_quality_level = carleaon_market_price.QualityLevel
        c_ench = carleaon_market_price.EnchantmentLevel
        c_item_name = carleaon_market_price.items_name
        c_amount = carleaon_market_price.Amount
        c_type = carleaon_market_price.ItemTypeId
        items.append(
            {
                "c_name": c_item_name,
                "c_price": c_price,
                "c_ench": c_ench,
                "c_quality": c_quality_level,
                "type": c_type,
                "time_stamp": datetime.now(),
            }
        )
        if c_price > curr_silver:
            continue
        for index2, black_market_price in black_market_prices.iterrows():
            b_price = black_market_price.UnitPriceSilver / x
            b_tier = black_market_price.Tier
            b_quality_level = black_market_price.QualityLevel
            b_ench = black_market_price.EnchantmentLevel
            b_item_name = black_market_price.items_name
            b_amount = black_market_price.Amount
            profite = b_price - c_price
            # print(c_item_name, c_price, "||", b_item_name, b_price, b_price > c_price)
            if profite < 1000:
                continue
            # if c_item_name == b_item_name:
            if b_quality_level > c_quality_level:
                continue
            if c_ench == b_ench and c_item_name == b_item_name:
                # print('X',c_item_name,c_price,'||',b_item_name,b_price,b_price>c_price)
                if b_price > c_price:
                    result = {
                        "name": c_item_name,
                        "price": c_price,
                        "ench": c_ench,
                        "quality": c_quality_level,
                        "profite": (b_price - c_price),
                        "Black_price": b_price,
                        "b_amount": b_amount,
                    }
                    results.append(result)
                    print(result)
                # print("name : ",c_item_name,'price : ',c_price,'ench : ',c_ench,"quality : ",c_quality_level)
    profite_history_path = "data/profite_result.csv"
    profite_history = pd.read_csv(profite_history_path, index_col=False)
    for result_item in results:
        result_item["time_stamp"] = datetime.now()
        profite_history = profite_history.append(
            result_item, ignore_index=True, verify_integrity=True
        )
    profite_history.to_csv(profite_history_path)

    items_path = "data/items_data.csv"
    items_data = pd.read_csv(items_path, index_col=False)
    for i in items:
        items_data = items_data.append(i, ignore_index=True, verify_integrity=True)

    items_data = items_data.sort_values(by=["c_price"], ascending=True)
    items_data = items_data.drop_duplicates(
        subset=["c_name", "c_price", "c_ench", "type"], keep="first"
    )
    items_data.to_csv(items_path)
    with open("profite.json", "w") as file:
        json.dump(results, file)
        # break


def run_against_bm():
    print("Running Against B_m !")
    items_index = requests.get(
        "https://raw.githubusercontent.com/issamoxix/albion-auto-searh/master/items2.json"
    ).json()
    items = requests.get(
        "https://raw.githubusercontent.com/issamoxix/Albion-online-items-Id/master/items.json"
    ).json()

    path = "data/black_market.csv"
    black_market_items = pd.read_csv(path, index_col=False, on_bad_lines="skip")
    cols = [
        "UnitPriceSilver",
        "Amount",
        "Tier",
        "QualityLevel",
        "EnchantmentLevel",
        "ItemTypeId",
        "BuyerName",
    ]
    black_market_items = black_market_items[cols]
    black_market_items["items_name"] = black_market_items["ItemTypeId"].map(
        lambda x: items_index.get(x, "None")
    )

    cols.append("items_name")

    black_market_items.to_csv("data/b_m.csv")
    main(True)


if __name__ == "__main__":
    main()
