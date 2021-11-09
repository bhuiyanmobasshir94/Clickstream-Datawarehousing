import pandas as pd


def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    # df_with_null = df[df.isnull().any(axis=1)]
    df_without_null = df.dropna()
    df_with_null = df.drop(df_without_null.index)
    df_with_null["referrer"].fillna("no referrer", inplace=True)
    df_with_null["resource"].fillna("no resource", inplace=True)
    df_with_null["type"].fillna("no type", inplace=True)
    df_with_null["number_of_occurrences"].fillna(0, inplace=True)
    df = pd.concat([df_without_null, df_with_null], ignore_index=True)
    df = df.astype(
        {
            "referrer": str,
            "resource": str,
            "type": str,
            "number_of_occurrences": int,
        }
    )
    return df


# def write_or_append(line, file_path, mode):
#     if mode == "w":
#         with open(file_path, mode) as f:
#             f.write(line)
#     elif mode == "a":
#         with open(file_path, mode) as f:
#             f.write(line)
#             f.write("\n")
#     else:
#         print("Invalid mode")
