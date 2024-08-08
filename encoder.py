# create a sample dataframe
import string
import random

def generate_random_data(num_rows=6, num_cols=10):
    data = {
        f'feature{k}': [
            random.choice(string.ascii_lowercase[:10]) for _ in range(num_rows)]
        for k in range(num_cols)
    }
    return data

def value_count_dict(df):
    # Creates a Dataframe
    feature_item_count = []
    for col in df.columns:
        for x in df[col].value_counts().items():
            feature_item_count.append((col, *x))
    df = pd.DataFrame(feature_item_count, columns=["feature", "value", "count"])
    return df

def get_top_k(df, top_k):
    sorted_df = df.sort_values("sum(count)", ascending=False)
    return sorted_df[:top_k]

def calculate_counts(ds, top_k=2, min_count=100):
    counts_ds = ds.map_batches(
        value_count_dict, batch_format="pandas", batch_size=None, zero_copy_batch=True)
    summed = counts_ds.groupby(["feature", "value"]).sum()
    summed = summed.filter(lambda x: x["sum(count)"] >= min_count)
    sorted_df = summed.groupby("feature").map_groups(get_top_k, fn_args=[top_k])
    return sorted_df.to_pandas()


if __name__ == "__main__":
    ray.init()

    data = generate_random_data(num_rows=1000, num_cols=10)
    df = pd.DataFrame(data)
    print(df)
    ds = ray.data.from_pandas(df)

    top_k_features = calculate_counts(ds, top_k=5, min_count=100)
    print(top_k_features)

