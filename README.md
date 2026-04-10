def build_combined_shipper():
    shipper_frames = []
    for y in [2018, 2019, 2020]:
        df = pd.read_csv(f'silver//shipper_{y}_silver.csv')
        df['year'] = y
        shipper_frames.append(df)
    combined = pd.concat(shipper_frames, ignore_index=True)
    combined['identifier'] = combined['identifier'].astype(str).str.strip()
    print(f"Combined shipper rows: {combined.shape[0]}")
    return combined

shipper_combined = build_combined_shipper()

def build_combined_tariff():
    tariff_frames = []
    for y in [2018, 2019, 2020]:
        df = pd.read_csv(f'silver//tariff_{y}_silver.csv')
        df['year'] = y
        tariff_frames.append(df)
        print(f"tariff_{y} rows: {df.shape[0]}")
    
    combined = pd.concat(tariff_frames, ignore_index=True)
    combined['identifier'] = combined['identifier'].astype(str).str.strip()
    
    print(f"\nCombined tariff rows: {combined.shape[0]}")
    print(f"Years present: {combined['year'].unique()}")
    print(f"Null harmonized weights: {combined['harmonized_weight_kg'].isnull().sum()}")

    # Aggregate by identifier AND year
    tariff_agg = combined.groupby(['identifier', 'year']).agg(
        total_harmonized_weight=('harmonized_weight_kg', 'sum'),
        harmonized_weight_unit=('harmonized_weight_combined', 'first'),
        total_tariff_lines=('identifier', 'count')
    ).reset_index()

    print(f"\nAggregated tariff rows: {tariff_agg.shape[0]}")
    return combined, tariff_agg

tariff_combined, tariff_agg = build_combined_tariff()

def build_gold_summary(tariff_agg, shipper_combined):
    # Join shipper names onto aggregated tariff
    tariff_with_names = tariff_agg.merge(
        shipper_combined[['identifier', 'shipper_party_name', 'country_name']],
        on='identifier',
        how='left'
    )

    print(f"After shipper join: {tariff_with_names.shape[0]} rows")
    print(f"Null shipper names: {tariff_with_names['shipper_party_name'].isnull().sum()}")

    # Final gold summary
    gold_summary = tariff_with_names.groupby(
        ['shipper_party_name', 'country_name', 'year']
    ).agg(
        total_harmonized_weight=('total_harmonized_weight', 'sum'),
        total_shipments=('identifier', 'count')
    ).reset_index()

    # Sort by weight descending
    gold_summary = gold_summary.sort_values(
        'total_harmonized_weight', ascending=False
    ).reset_index(drop=True)

    # Add audit columns
    gold_summary['load_date'] = pd.Timestamp.today().date()
    gold_summary['source_system'] = 'tariff_2018_2020'

    print(f"\nGold summary rows: {gold_summary.shape[0]}")
    print(gold_summary.head(10))
    return gold_summary

gold_summary = build_gold_summary(tariff_agg, shipper_combined)

import os
os.makedirs('gold', exist_ok=True)
gold_summary.to_parquet('gold/tariff_gold.parquet', index=False)
print(f"Saved tariff_gold.parquet with {gold_summary.shape[0]} rows")
